#!/usr/bin/env python3
"""Spillover analyser - SHADOW MODE (owner 2026-11-11).

Idea under test: when a source trade cannot open in its own subaccount (below
the 5 USDT exchange minimum, margin hold, 20% cap, symbol conflict), could a
sibling subaccount take it instead?

This process NEVER trades. It reads the engines' own skip lines and their live
account state, and logs what a spillover *would* do. Run it for a day, compare
against reality, then decide whether to enable placement.

Excluded on purpose: the AI Main account and hypercopy.
"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone

LOG = '/var/lib/hypercopy-copy/spillover_shadow.log'
STATE = '/var/lib/hypercopy-copy/spillover_shadow.json'

# account key -> (port, label). main (8000) and hypercopy (8006) excluded.
ACCOUNTS = {
    'mainport': (8001, 'Sub account 01 (MAINPORT)'),
    'bakery': (8002, 'Sub account 02 (BAKERY)'),
    'scalping': (8003, 'Sub account 03 (SCALPING)'),
    'kings': (8004, 'Sub account 04 (KINGS)'),
    'bluespire': (8005, 'Sub account 05 (BLUESPIIRE)'),
}

MIN_NOTIONAL = 5.0
ENTRY_CAP = 0.20
MARGIN_HOLD = 0.70      # no new entries at or above this
MARGIN_RELEASE = 0.40   # only resume under this


def fetch(port):
    try:
        with urllib.request.urlopen('http://127.0.0.1:%d/status' % port, timeout=15) as r:
            return json.load(r)
    except Exception:
        return None


def wallet_of(d):
    if not isinstance(d, dict):
        return None
    for path in (('wallet_balance',), ('wallet',), ('bybit', 'balance'), ('equity',)):
        cur = d
        for p in path:
            cur = cur.get(p) if isinstance(cur, dict) else None
        if isinstance(cur, (int, float)) and cur:
            return float(cur)
    return None


def margin_pct(d):
    v = d.get('margin_pct') if isinstance(d, dict) else None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        used = float((d.get('bybit') or {}).get('used_margin') or 0)
        w = wallet_of(d) or 0
        return (used / w * 100.0) if w else None
    except Exception:
        return None


def open_symbols(d):
    out = set()
    try:
        for p in (d.get('bybit') or {}).get('positions') or []:
            if float(p.get('size') or 0) > 0:
                out.add((str(p.get('symbol') or '').upper(), 'long' if p.get('side') == 'Buy' else 'short'))
    except Exception:
        pass
    for k, v in (d.get('mirrored') or {}).items():
        try:
            out.add((str(v.get('symbol') or '').upper(), v.get('side')))
        except Exception:
            pass
    return out


SKIP_RE = re.compile(r'SKIP\s+(\S+\|[^|]+\|[0-9a-f-]+)\s+(new_entry|size_add):\s+(.*)')
NOTIONAL_RE = re.compile(r'notional\s+([\d.]+)\s+below minimum', re.I)
CAP_RE = re.compile(r'desired margin\s+([\d.]+)\s+exceeds 20% of wallet', re.I)
CONFLICT_RE = re.compile(r'live position conflict', re.I)
MANUAL_RE = re.compile(r'manual protect', re.I)


def log_line(text):
    line = '[%s] %s' % (datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), text)
    try:
        with open(LOG, 'a') as f:
            f.write(line + '\n')
    except Exception:
        pass


def main():
    status = {k: fetch(p) for k, (p, _l) in ACCOUNTS.items()}
    summary = {}
    seen = {}

    # Owner 2026-11-11: a symbol already open anywhere in the fleet blocks a
    # spillover. Without this, four sources shorting BTC looked like four
    # independent trades when they are one bet at four times the size.
    fleet_symbols = {}
    for _a in ACCOUNTS:
        for _pair in open_symbols(status.get(_a)):
            fleet_symbols[_pair] = fleet_symbols.get(_pair, 0) + 1

    for acct, (port, label) in ACCOUNTS.items():
        path = '/var/lib/hypercopy-copy/%s_actions.log' % acct
        try:
            with open(path, 'rb') as f:
                f.seek(max(0, os.path.getsize(path) - 200000))
                tail = f.read().decode('utf-8', 'replace').splitlines()[-400:]
        except Exception:
            continue
        own = status.get(acct)
        own_wallet = wallet_of(own) or 0.0
        for line in tail:
            m = SKIP_RE.search(line)
            if not m:
                continue
            key, kind, reason = m.group(1), m.group(2), m.group(3)
            coin_side = key.split('|')[1]
            coin, side = (coin_side.split('/') + ['long'])[:2]
            symbol = coin if coin.endswith('USDT') else coin + 'USDT'
            side = side.lower()

            why = None
            notional = None
            mn = NOTIONAL_RE.search(reason)
            if mn:
                notional = float(mn.group(1))
                why = 'below exchange minimum (%.2f USDT)' % notional
            elif CAP_RE.search(reason):
                why = 'exceeds the 20% entry cap'
            elif CONFLICT_RE.search(reason):
                why = 'live position conflict in its own account'
            elif MANUAL_RE.search(reason):
                why = 'owner-managed protection on that symbol'
            if not why:
                continue
            if fleet_symbols.get((symbol, side), 0) > 0:
                # already held somewhere in the fleet: spillover would stack one bet
                continue

            targets = []
            for sib, (sport, slabel) in ACCOUNTS.items():
                if sib == acct:
                    continue
                sd = status.get(sib)
                if not sd:
                    continue
                sw = wallet_of(sd) or 0.0
                mp = margin_pct(sd)
                if not sw or (mp is not None and mp >= MARGIN_HOLD * 100):
                    continue
                if (symbol, side) in open_symbols(sd):
                    continue
                if (symbol, side) in fleet_symbols:
                    continue
                est = notional * (sw / own_wallet) if (notional and own_wallet) else None
                if est is not None and est < MIN_NOTIONAL:
                    continue
                if est is not None and est > sw * ENTRY_CAP * 10:
                    continue
                targets.append({'account': sib, 'label': slabel, 'estimate_notional': round(est, 2) if est else None,
                                'wallet': round(sw, 2), 'margin_pct': round(mp, 1) if mp is not None else None})
            if targets:
                entry = {'key': key, 'home': acct, 'reason': why, 'targets': targets, 'ts': time.time()}
                log_line('SPILLOVER-CANDIDATE %s (%s) from %s -> %s' % (
                    key, why, acct, ', '.join(t['account'] + (' @%.2f' % t['estimate_notional'] if t.get('estimate_notional') else '') for t in targets)))
                summary[key] = entry
                seen[acct] = seen.get(acct, 0) + 1

    try:
        with open(STATE, 'w') as f:
            json.dump({'updated': time.time(), 'candidates': summary, 'per_home': seen}, f, indent=1)
    except Exception:
        pass
    log_line('SUMMARY homes=%s total_candidates=%d' % (seen or '{}', len(summary)))


if __name__ == '__main__':
    main()
