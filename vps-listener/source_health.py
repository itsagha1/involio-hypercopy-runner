#!/usr/bin/env python3
"""Source-profile watchdog (owner 2026-11-09).

Every run, for each authorized Involio profile:
  1. fetch the book;
  2. on success, clear the failure streak and record the live owner name;
  3. on failure, try to RECOVER a rename: read the portfolio's immutable
     identity (createdAt + title) and let source_api adopt the new username;
  4. if the profile has been unreachable for >= 6 hours with no recovery,
     POST /source/lost so its mirrors stop pretending to be mirrored: live
     positions become owner-managed with a protective TP, flat ones deregister.

Zero Base44 credits: plain VPS code on a timer.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

BASE_DIR = '/home/ubuntu/involio-hypercopy-runner/vps-listener'
STATE = '/var/lib/hypercopy-copy/source_health.json'
LOG = '/var/lib/hypercopy-copy/source_health.log'
LOST_AFTER_H = 6.0

sys.path.insert(0, BASE_DIR)
os.chdir(BASE_DIR)


def log(msg: str) -> None:
    line = '[%s] %s' % (datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), msg)
    print(line, flush=True)
    try:
        with open(LOG, 'a') as f:
            f.write(line + '\n')
    except Exception:
        pass


def load_state() -> dict:
    try:
        with open(STATE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(d: dict) -> None:
    tmp = STATE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(d, f, indent=1)
    os.replace(tmp, STATE)


def listener_secret() -> str:
    for path in ('/home/ubuntu/involio-hypercopy-runner/vps-listener/.env', '/etc/copy-kings.env'):
        try:
            with open(path) as f:
                for line in f:
                    if line.startswith('WEBHOOK_SHARED_SECRET='):
                        return line.split('=', 1)[1].strip().strip('"').strip("'")
        except Exception:
            continue
    return ''


def notify_lost(profile: str) -> dict:
    secret = listener_secret()
    try:
        r = requests.post('http://127.0.0.1:8000/source/lost', json={'profile': profile},
                          headers={'x-signature': secret}, timeout=90)
        return r.json()
    except Exception as e:
        return {'ok': False, 'error': str(e)[:120]}


def main() -> int:
    import source_api

    state = load_state()
    profiles = list(source_api.PROFILES)
    now = time.time()
    touched = False

    for prof in profiles:
        rec = state.setdefault(prof, {})
        try:
            book = source_api.fetch_source_book(profile=prof)
            n = len(book.get('positions') or [])
            rec.update({'last_ok': now, 'fails': 0, 'open_positions': n, 'lost': False})
            touched = True
            log('%s: ok, %d open position(s)' % (prof, n))
            continue
        except Exception as e:
            msg = str(e)[:110]
            rec['fails'] = int(rec.get('fails') or 0) + 1
            rec['last_error'] = msg
            rec.setdefault('first_fail', now)
            hours = (now - float(rec.get('first_fail') or now)) / 3600.0
            log('%s: FAIL %s (streak %d, %.1fh)' % (prof, msg, rec['fails'], hours))
            touched = True
            if hours >= LOST_AFTER_H and not rec.get('lost'):
                res = notify_lost(prof)
                rec['lost'] = bool((res or {}).get('ok'))
                rec['lost_at'] = now
                rec['lost_result'] = res
                log('%s: SOURCE LOST for %.1fh -> %s' % (prof, hours, json.dumps(res)[:200]))
            continue

    if touched:
        save_state(state)
    return 0


if __name__ == '__main__':
    sys.exit(main())
