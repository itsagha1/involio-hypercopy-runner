#!/usr/bin/env python3
"""Shared source-book cache (owner 2026-11-11).

Problem it solves: six copy engines each logged into Involio and pulled every
source book independently. Doubling the poll rate from 120s to 30s pushed
Involio into HTTP 429 and stalled every engine.

This daemon logs in once per cycle, pulls each portfolio once, and serves the
result on 127.0.0.1:8010 so all engines read the same book instead of fetching
their own copies. Involio load drops by roughly the number of engines.

Read-only: it never places, changes or closes anything.
"""
import json
import os
import re
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["BOOK_CACHE_DISABLE"] = "1"   # never read our own cache

import source_api  # noqa: E402

PORT = int(os.environ.get("BOOK_CACHE_PORT", "8010"))
CYCLE = float(os.environ.get("BOOK_CACHE_CYCLE", "60"))
MAX_AGE = float(os.environ.get("BOOK_CACHE_MAX_AGE", "90"))
UNITS = "/etc/systemd/system"

CACHE = {}          # profile -> {"book":..., "ts":..., "error":...}
LOCK = threading.Lock()
PACING = float(os.environ.get("BOOK_CACHE_PACING", "2.0"))
PACE_BASE = PACING
COOLDOWN = float(os.environ.get("BOOK_CACHE_COOLDOWN", "300"))
STATS = {"cycles": 0, "fetches": 0, "errors": 0, "last_cycle": 0, "backoff": 0}
COOLDOWN_UNTIL = {}


def groups():
    """profile/portfolio list per env file, straight from the engine units."""
    out = []
    try:
        seen = set()
        for path in sorted(os.listdir(UNITS)):
            if not (path.startswith("copy-") and path.endswith(".service")):
                continue
            text = open(os.path.join(UNITS, path)).read()
            prof = re.search(r"COPY_PROFILE=(\S+)", text)
            pids = re.search(r"COPY_PORTFOLIO_ID=(\S+)", text)
            envf = re.search(r"EnvironmentFile=(\S+)", text)
            if not (prof and pids and envf):
                continue
            names = [x.strip() for x in prof.group(1).split(",") if x.strip()]
            ids = [x.strip() for x in pids.group(1).split(",") if x.strip()]
            pairs = list(zip(names, ids))
            key = tuple(pairs)
            if not pairs or key in seen:
                continue
            seen.add(key)
            out.append((envf.group(1), pairs))
        # the main listener follows its own profiles from .env
        main_env = "/home/ubuntu/involio-hypercopy-runner/vps-listener/.env"
        main_pairs = [(p, pid) for p, pid in getattr(source_api, "PROFILES", {}).items()]
        if main_pairs:
            key = tuple(main_pairs)
            if key not in seen:
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


def refresh_group(env_path, pairs):
    tok = token_from(env_path)
    if not tok:
        return
    os.environ["INVOLIO_REFRESH_TOKEN"] = tok
    for profile, pid in pairs:
        if COOLDOWN_UNTIL.get(profile, 0) > time.time():
            continue
        global PACING
        time.sleep(PACING)   # Involio throttles bursts; pace the portfolio calls
        try:
            book = source_api.fetch_source_book(profile=profile, portfolio_id=pid)
            with LOCK:
                CACHE[profile] = {"book": book, "ts": time.time(), "error": None}
            STATS["fetches"] += 1
        except Exception as e:
            msg = str(e)[:140]
            with LOCK:
                prev = CACHE.get(profile) or {}
                CACHE[profile] = {"book": prev.get("book"), "ts": prev.get("ts"),
                                  "error": msg, "err_ts": time.time()}
            STATS["errors"] += 1
            if "429" in msg:
                COOLDOWN_UNTIL[profile] = time.time() + COOLDOWN
                PACING = min(PACING * 1.5, 6.0)   # widen the gap, relax later
                STATS["backoff"] = 1


def worker():
    global PACING
    while True:
        started = time.time()
        for env_path, pairs in GROUPS:
            try:
                refresh_group(env_path, pairs)
            except Exception as e:
                print("cycle error:", str(e)[:120], flush=True)
        if not STATS["backoff"]:
            PACING = max(PACE_BASE, PACING * 0.85)
        STATS["cycles"] += 1
        STATS["last_cycle"] = time.time()
        waited = max(1.0, CYCLE - (time.time() - started))
        if STATS["backoff"]:
            waited += min(30.0, CYCLE / 2)
            STATS["backoff"] = 0
        time.sleep(waited)


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
                rec = CACHE.get(profile)
                age = (time.time() - rec["ts"]) if rec and rec.get("ts") else None
                book = rec.get("book") if rec else None
                err = rec.get("error") if rec else None
            if book is None:
                return self._send(404, {"ok": False, "error": err or "not cached yet"})
            if age is not None and age > MAX_AGE:
                return self._send(409, {"ok": False, "error": "stale", "age": round(age, 1)})
            return self._send(200, {"ok": True, "age": round(age or 0, 1), "book": book})
        if u.path == "/health":
            now = time.time()
            with LOCK:
                table = {p: {"age": round(now - (v.get("ts") or now), 1),
                             "error": v.get("error")} for p, v in CACHE.items()}
            return self._send(200, {"ok": True, "cycle_seconds": CYCLE, "max_age": MAX_AGE,
                                    "stats": STATS, "profiles": table})
        return self._send(404, {"ok": False, "error": "unknown path"})


GROUPS = groups()
# source_api only trusts profiles registered in its own table; the engines do the
# same at startup. Without this every copy source comes back "Unauthorized".
for _env, _pairs in GROUPS:
    for _prof, _pid in _pairs:
        source_api.PROFILES.setdefault(_prof, _pid)

if __name__ == "__main__":
    threading.Thread(target=worker, daemon=True).start()
    print("book-cache on 127.0.0.1:%d, cycle %.0fs, %d group(s)" % (PORT, CYCLE, len(GROUPS)), flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
