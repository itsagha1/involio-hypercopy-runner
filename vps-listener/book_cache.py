#!/usr/bin/env python3
"""Shared source-book cache with priority tiers (owner 2026-11-12).

One process logs into Involio once per cycle and pulls each source portfolio
once; every engine reads the result on 127.0.0.1:8010 instead of fetching its
own copy.

Tiers: a profile we actually hold a position in is HOT (refreshed about every
25s); a profile with no open mirror is COLD (every 4 minutes). This keeps the
books that decide something fresh without exceeding Involio's request budget,
which is what produced the 429 storms when everything refreshed at one rate.

Read-only: it never places, changes or closes anything.
"""
import json
import os
import re
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["BOOK_CACHE_DISABLE"] = "1"   # never read our own cache

import source_api  # noqa: E402

PORT = int(os.environ.get("BOOK_CACHE_PORT", "8010"))
MAX_AGE = float(os.environ.get("BOOK_CACHE_MAX_AGE", "400"))
HOT_INTERVAL = float(os.environ.get("BOOK_CACHE_HOT", "25"))
COLD_INTERVAL = float(os.environ.get("BOOK_CACHE_COLD", "240"))
PACE = float(os.environ.get("BOOK_CACHE_PACING", "1.6"))
COOLDOWN = float(os.environ.get("BOOK_CACHE_COOLDOWN", "300"))
UNITS = "/etc/systemd/system"
ENGINE_PORTS = [8000, 8001, 8002, 8003, 8004, 8005, 8006]

CACHE = {}
DUE = {}
COOLDOWN_UNTIL = {}
HOT = set()
LOCK = threading.Lock()
STATS = {"cycles": 0, "fetches": 0, "errors": 0, "last_cycle": 0, "hot": 0, "cold": 0, "pace": PACE}


def groups():
    """profile/portfolio list per env file, straight from the engine units."""
    out, seen = [], set()
    try:
        for name in sorted(os.listdir(UNITS)):
            if not (name.startswith("copy-") and name.endswith(".service")):
                continue
            text = open(os.path.join(UNITS, name)).read()
            prof = re.search(r"COPY_PROFILE=(\S+)", text)
            pids = re.search(r"COPY_PORTFOLIO_ID=(\S+)", text)
            envf = re.search(r"EnvironmentFile=(\S+)", text)
            if not (prof and pids and envf):
                continue
            pairs = list(zip([x.strip() for x in prof.group(1).split(",") if x.strip()],
                             [x.strip() for x in pids.group(1).split(",") if x.strip()]))
            key = tuple(pairs)
            if pairs and key not in seen:
                seen.add(key)
                out.append((envf.group(1), pairs))
        main_env = "/home/ubuntu/involio-hypercopy-runner/vps-listener/.env"
        main_pairs = [(p, pid) for p, pid in getattr(source_api, "PROFILES", {}).items()]
        key = tuple(main_pairs)
        if main_pairs and key not in seen:
            out.append((main_env, main_pairs))
    except Exception as e:
        print("group discovery failed:", e, flush=True)
    return out


def token_from(path):
    try:
        for line in open(path):
            if line.startswith("INVOLIO_REFRESH_TOKEN="):
                return line.split("=", 1)[1].strip()
    except OSError:
        pass
    return ""


def refresh_hot_set():
    """Profiles we hold a position in are the ones worth refreshing often."""
    hot = set()
    for port in ENGINE_PORTS:
        try:
            with urllib.request.urlopen("http://127.0.0.1:%d/status" % port, timeout=5) as r:
                d = json.load(r)
        except Exception:
            continue
        for key in (d.get("mirrored") or {}):
            prof = str(key).split("|")[0]
            if prof:
                hot.add(prof)
        for pos in (d.get("bybit") or {}).get("positions") or []:
            pass
    with LOCK:
        changed = hot != HOT
        HOT.clear()
        HOT.update(hot)
        STATS["hot"] = len(HOT)
    if changed:
        print("hot set (%d): %s" % (len(hot), ",".join(sorted(hot))[:160]), flush=True)


def fetch_one(profile, pid, env_path, tok):
    global PACE
    try:
        if COOLDOWN_UNTIL.get(profile, 0) > time.time():
            return False
        os.environ["INVOLIO_REFRESH_TOKEN"] = tok
        time.sleep(PACE)
        book = source_api.fetch_source_book(profile=profile, portfolio_id=pid)
        with LOCK:
            CACHE[profile] = {"book": book, "ts": time.time(), "error": None}
        STATS["fetches"] += 1
        return True
    except Exception as e:
        msg = str(e)[:140]
        with LOCK:
            prev = CACHE.get(profile) or {}
            CACHE[profile] = {"book": prev.get("book"), "ts": prev.get("ts"),
                              "error": msg, "err_ts": time.time()}
        STATS["errors"] += 1
        if "429" in msg:
            COOLDOWN_UNTIL[profile] = time.time() + COOLDOWN
            PACE = min(PACE * 1.3, 5.0)
            STATS["pace"] = round(PACE, 2)
        return False


def scheduler():
    """Fetch whatever is due, hot first. Never bursts: one call at a time."""
    global PACE
    now = time.time()
    with LOCK:
        due = [(prof, pid, env, tok) for (prof, pid, env, tok) in JOBS
               if now >= DUE.get(prof, 0)]
    hot_due = [j for j in due if j[0] in HOT]
    cold_due = [j for j in due if j[0] not in HOT]
    ordered = hot_due + cold_due
    for prof, pid, env, tok in ordered:
        ok = fetch_one(prof, pid, env, tok)
        interval = HOT_INTERVAL if prof in HOT else COLD_INTERVAL
        with LOCK:
            DUE[prof] = time.time() + (interval if ok else 30)
    # relax pacing back toward the floor on a clean pass
    if not any(CACHE.get(p, {}).get("error") for p in CACHE):
        PACE = max(float(os.environ.get("BOOK_CACHE_PACING", "1.6")), PACE * 0.9)
        STATS["pace"] = round(PACE, 2)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if u.path == "/book":
            profile = (q.get("profile") or [""])[0]
            with LOCK:
                rec = CACHE.get(profile) or {}
                book = rec.get("book")
                age = (time.time() - rec["ts"]) if rec.get("ts") else None
                err = rec.get("error")
            if book is None:
                return self._send(404, {"ok": False, "error": err or "not cached yet"})
            if age is not None and age > MAX_AGE:
                return self._send(409, {"ok": False, "error": "stale", "age": round(age, 1)})
            return self._send(200, {"ok": True, "age": round(age or 0, 1), "book": book})
        if u.path == "/health":
            now = time.time()
            with LOCK:
                table = {p: {"age": round(now - (v.get("ts") or now), 1),
                             "error": v.get("error"), "tier": "hot" if p in HOT else "cold"}
                         for p, v in CACHE.items()}
            return self._send(200, {"ok": True, "max_age": MAX_AGE,
                                    "hot_interval": HOT_INTERVAL, "cold_interval": COLD_INTERVAL,
                                    "stats": STATS, "profiles": table})
        return self._send(404, {"ok": False, "error": "unknown path"})


GROUPS = groups()
for _env, _pairs in GROUPS:
    for _prof, _pid in _pairs:
        source_api.PROFILES.setdefault(_prof, _pid)
JOBS = [(prof, pid, env, token_from(env)) for env, pairs in GROUPS for prof, pid in pairs]


def loop():
    while True:
        try:
            refresh_hot_set()
            scheduler()
            STATS["cycles"] += 1
            STATS["last_cycle"] = time.time()
        except Exception as e:
            print("scheduler error:", str(e)[:160], flush=True)
        time.sleep(2)


if __name__ == "__main__":
    hot_hint = sum(1 for j in JOBS if True)
    print("book-cache tiers on 127.0.0.1:%d | %d profiles | hot %.0fs cold %.0fs"
          % (PORT, len(JOBS), HOT_INTERVAL, COLD_INTERVAL), flush=True)
    threading.Thread(target=loop, daemon=True).start()
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
