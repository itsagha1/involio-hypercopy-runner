#!/usr/bin/env python3
"""Build the exact per-account invested figure and publish it for the dashboard.

Owner rule (2026-11-07): investment = money moved from the master funding account
into a subaccount. Every transfer in ADDS to it, every transfer out SUBTRACTS, and
the figure always reflects the current position. No master balance is shown.

Sources, in order of authority:
  1. /var/lib/hypercopy-copy/ledger_import.json  - parsed from the owner's Bybit
     asset-change exports (UTA TRANSFER_IN / TRANSFER_OUT rows, with internal
     FUND<->UNIFIED shuffles excluded), plus one derived row for AI Main, which
     Bybit excludes from the master export.
  2. /var/lib/hypercopy-copy/transfer_ledger.jsonl - every live transfer the bot
     makes, appended automatically by move_funds.record_ledger.

Writes /var/lib/hypercopy-copy/invest_exact.json -> {account_key: {invested,in,out,rows}}
"""
import datetime, json, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import move_funds as mf  # noqa: F401  (kept for the UID map / future validation)

IMPORT = '/var/lib/hypercopy-copy/ledger_import.json'
LIVE = '/var/lib/hypercopy-copy/transfer_ledger.jsonl'
OUT = '/var/lib/hypercopy-copy/invest_exact.json'
ACCOUNTS = ('main', 'mainport', 'bakery', 'scalping', 'kings', 'bluespire', 'hypercopy')


def load_rows():
    rows = []
    try:
        d = json.load(open(IMPORT))
        rows += d.get('imported') or []
        rows += d.get('derived') or []
    except Exception:
        pass
    try:
        with open(LIVE) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
    except Exception:
        pass
    return rows


def main():
    rows = load_rows()
    seen, deduped = set(), []
    for r in rows:
        key = r.get('transfer_id') or (r.get('ts'), r.get('from'), r.get('to'), r.get('amount'), r.get('kind'))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    agg = {a: {'invested': 0.0, 'in': 0.0, 'out': 0.0, 'rows': 0} for a in ACCOUNTS}
    for r in deduped:
        src, dst = r.get('from'), r.get('to')
        amt = float(r.get('amount') or 0)
        if amt <= 0:
            continue
        if dst in agg:
            agg[dst]['invested'] += amt
            agg[dst]['in'] += amt
            agg[dst]['rows'] += 1
        if src in agg:
            agg[src]['invested'] -= amt
            agg[src]['out'] += amt
            agg[src]['rows'] += 1

    for a in agg.values():
        for k in ('invested', 'in', 'out'):
            a[k] = round(a[k], 2)
    out = {
        'generated_at': datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds'),
        'accounts': agg,
        'total': round(sum(v['invested'] for v in agg.values()), 2),
    }
    tmp = OUT + '.tmp'
    json.dump(out, open(tmp, 'w'), indent=1)
    os.replace(tmp, OUT)
    os.chmod(OUT, 0o644)
    for a in ACCOUNTS:
        print('%-10s invested %9.2f  (in %.2f / out %.2f, %d rows)' % (
            a, agg[a]['invested'], agg[a]['in'], agg[a]['out'], agg[a]['rows']))
    print('TOTAL invested: %.2f' % out['total'])


if __name__ == '__main__':
    main()
