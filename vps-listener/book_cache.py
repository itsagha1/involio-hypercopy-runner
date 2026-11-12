#!/usr/bin/env python3
"""Shared source-book cache, two Involio accounts, priority tiers.

Owner 2026-11-12: supports a SECOND Involio account. Each token has its own
worker and its own pacing, so the two accounts fetch in parallel and the
profiles are split between them. A throttle on one token no longer stalls the
whole fleet, which is what produced the 429 storms with a single account.

Tiers: profiles we hold positions in are HOT (target ~25s); the rest are COLD
(every 4 minutes). Read-only: never places, changes or closes anything.
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
os.environ["BOOK_CACHE_DISABLE"] = "1"

import source_api  # noqa: E402

PORT = int(os.environ.get("BOOK_CACHE_PORT", "8010"))
MAX_AGE = float(os.environ.get("BOOK_CACHE_MAX_AGE", "400"))
HOT_INTERVAL = float(os.environ.get("BOOK_CACHE_HOT", "25"))
COLD_INTERVAL = float(os.environ.get("BOOK_CACHE_COLD", "240"))
PACE_FLOOR = float(os.environ.get("BOOK_CACHE_PACING", "1.2"))
COOLDOWN = float(os.environ.get("BOOK_CACHE_COOLDOWN", "180"))
# Owner 2026-11-12: a cold start used to fire every profile at once and burn the
# Involio rate limit (26 x 429 in one cycle). Spread the first pass instead, and
# back off harder each time one account is throttled.
STAGGER = float(os.environ.get("BOOK_CACHE_STAGGER", "2.0"))
HARD_PAUSE = {"A": 0.0, "B": 0.0}
STRIKES = {"A": 0, "B": 0}
UNITS = "/etc/systemd/system"
TOKEN2_FILE = "/etc/involio-token2.env"
ENGINE_PORTS = [8000, 8001, 8002, 8003, 8004, 8005, 8006]

CACHE, DUE, COOLDOWN_UNTIL, HOT = {}, {}, {}, set()
LOCK = threading.Lock()
STATS = {"fetches": 0, "errors": 0, "hot": 0, "tokens": 0}


def groups():
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
        main_pairs = [(p, pid) for p, pid in getattr(source_api, "PROFILES", {}).items()]
        key = tuple(main_pairs)
        if main_pairs and key not in seen:
            out.append(("/home/ubuntu/involio-hypercopy-runner/vps-listener/.env", main_pairs))
    except Exception as e:
        print("group discovery failed:", e, flush=True)
    return out


def token_from(path, name="INVOLIO_REFRESH_TOKEN"):
    try:
        for line in open(path):
            if line.startswith(name + "="):
                return line.split("=", 1)[1].strip()
    except OSError:
        pass
    return ""


def refresh_hot_set():
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
    with LOCK:
        HOT.clear()
        HOT.update(hot)
        STATS["hot"] = len(HOT)


def worker(token, jobs, label):
    """One Involio account: own pacing, own throttle state, own profile slice."""
    pace = PACE_FLOOR
    with LOCK:
        for _i, (_prof, _pid) in enumerate(jobs):
            _ck = "%s|%s" % (_prof, _pid)
            if not DUE.get(_ck):
                DUE[_ck] = time.time() + _i * STAGGER
    while True:
        did = False
        for profile, pid in jobs:
            ck = "%s|%s" % (profile, pid)
            if COOLDOWN_UNTIL.get(ck, 0) > time.time():
                continue
            if HARD_PAUSE.get(label, 0) > time.time():
                continue
            with LOCK:
                if time.time() < DUE.get(ck, 0):
                    continue
            did = True
            time.sleep(pace)
            ck = "%s|%s" % (profile, pid)   # composite key: one entry per portfolio
            try:
                book = source_api.fetch_source_book(profile=profile, portfolio_id=pid,
                                                    refresh_token=token)
                with LOCK:
                    CACHE[ck] = {"book": book, "ts": time.time(), "error": None, "token": label,
                                 "profile": profile, "pid": pid}
                STATS["fetches"] += 1
                STRIKES[label] = 0
                interval = HOT_INTERVAL if profile in HOT else COLD_INTERVAL
                with LOCK:
                    DUE[ck] = time.time() + interval
            except Exception as e:
                msg = str(e)[:140]
                with LOCK:
                    prev = CACHE.get(ck) or {}
                    CACHE[ck] = {"book": prev.get("book"), "ts": prev.get("ts"),
                                 "error": msg, "err_ts": time.time(), "token": label,
                                 "profile": profile, "pid": pid}
                    DUE[ck] = time.time() + 20
                STATS["errors"] += 1
                if "429" in msg:
                    STRIKES[label] = STRIKES.get(label, 0) + 1
                    backoff = min(COOLDOWN * (2 ** (STRIKES[label] - 1)), 1800)
                    COOLDOWN_UNTIL[ck] = time.time() + backoff
                    pace = min(pace * 1.3, 5.0)
                    if STRIKES[label] >= 3:
                        HARD_PAUSE[label] = time.time() + 120
                        STRIKES[label] = 0
        if not did:
            time.sleep(1.5)
        else:
            pace = max(PACE_FLOOR, pace * 0.95)


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
            want_pid = (q.get("portfolio_id") or [None])[0]
            with LOCK:
                keys = [k for k, v in CACHE.items() if v.get("profile") == profile]
                if want_pid:
                    keys = [k for k in keys if k.endswith("|" + str(want_pid))]
                rec = CACHE.get(keys[0]) if len(keys) == 1 else {}
                if len(keys) > 1:
                    for k in keys:
                        v = CACHE[k]
                        if v.get("error"):
                            rec = {"error": v["error"], "ts": v.get("ts")}
                            break
                    else:
                        merged = {"positions": [], "equity": 0.0}
                        oldest = None
                        for k in keys:
                            bk = CACHE[k].get("book") or {}
                            merged["positions"].extend(bk.get("positions") or [])
                            try:
                                merged["equity"] += float(bk.get("equity") or 0)
                            except Exception:
                                pass
                            ts = CACHE[k].get("ts") or 0
                            oldest = ts if oldest is None else min(oldest, ts)
                        seen, uniq = set(), []
                        for p in merged["positions"]:
                            ident = p.get("source_id") or (p.get("ticker"), p.get("side"))
                            if ident in seen:
                                continue
                            seen.add(ident); uniq.append(p)
                        merged["positions"] = uniq
                        rec = {"book": merged, "ts": oldest, "error": None}
                book, age, err = rec.get("book"), None, rec.get("error")
                if rec.get("ts"):
                    age = time.time() - rec["ts"]
            if book is None:
                return self._send(404, {"ok": False, "error": err or "not cached yet"})
            if age is not None and age > MAX_AGE:
                return self._send(409, {"ok": False, "error": "stale", "age": round(age, 1)})
            return self._send(200, {"ok": True, "age": round(age or 0, 1), "book": book})
        if u.path == "/health":
            now = time.time()
            with LOCK:
                table = {p: {"age": round(now - (v.get("ts") or now), 1), "error": v.get("error"),
                             "tier": "hot" if p in HOT else "cold", "token": v.get("token")}
                         for p, v in CACHE.items()}
            return self._send(200, {"ok": True, "max_age": MAX_AGE, "hot_interval": HOT_INTERVAL,
                                    "cold_interval": COLD_INTERVAL, "stats": STATS, "profiles": table})
        return self._send(404, {"ok": False, "error": "unknown path"})


GROUPS = groups()
for _env, _pairs in GROUPS:
    for _prof, _pid in _pairs:
        source_api.PROFILES.setdefault(_prof, _pid)

MAIN_TOKEN = token_from("/home/ubuntu/involio-hypercopy-runner/vps-listener/.env")
SECOND_TOKEN = token_from(TOKEN2_FILE)

ALL_JOBS = [(prof, pid) for _env, pairs in GROUPS for prof, pid in pairs]
if SECOND_TOKEN:
    # split the profiles between the two accounts so they fetch in parallel
    jobs1 = [j for i, j in enumerate(ALL_JOBS) if i % 2 == 0]
    jobs2 = [j for i, j in enumerate(ALL_JOBS) if i % 2 == 1]
else:
    jobs1, jobs2 = ALL_JOBS, []

STATS["tokens"] = 2 if SECOND_TOKEN else 1

if __name__ == "__main__":
    print("book-cache: %d profiles | account A: %d, account B: %d | hot %.0fs cold %.0fs"
          % (len(ALL_JOBS), len(jobs1), len(jobs2), HOT_INTERVAL, COLD_INTERVAL), flush=True)
    threading.Thread(target=worker, args=(MAIN_TOKEN, jobs1, "A"), daemon=True).start()
    if jobs2:
        threading.Thread(target=worker, args=(SECOND_TOKEN, jobs2, "B"), daemon=True).start()
    threading.Thread(target=lambda: [(refresh_hot_set(), time.sleep(20)) for _ in iter(int, 1)],
                     daemon=True).start()
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
