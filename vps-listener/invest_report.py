#!/usr/bin/env python3
"""Exact investment report from the transfer ledger (+ optional Bybit CSV export).

Investment = money transferred from the master funding account INTO a subaccount.
Returns (sub -> master) are tracked separately so net and gross are both visible.

Usage:
  invest_report.py                     # ledger only
  invest_report.py --csv <file.csv>    # merge a Bybit transfer-history export
"""
import argparse, csv, json, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from margin_relief import LABELS
import move_funds as mf

LEDGER = '/var/lib/hypercopy-copy/transfer_ledger.jsonl'
MASTER = 'Master funding'
UID_TO_LABEL = {v: LABELS.get(k, k) for k, v in mf.UIDS.items()}
UID_TO_LABEL[mf.UIDS['master']] = MASTER


def norm(who):
    if who is None:
        return '?'
    who = str(who).strip()
    if who in LABELS:
        return LABELS[who]
    if who in UID_TO_LABEL:
        return UID_TO_LABEL[who]
    if who == 'master':
        return MASTER
    for label in list(LABELS.values()) + [MASTER]:
        if who.lower() == label.lower():
            return label
    return who


def load_ledger(path=LEDGER):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def pick(row, *needles):
    for k, v in row.items():
        kl = (k or '').lower()
        if all(n in kl for n in needles):
            return v
    return ''


def parse_csv(path):
    out = []
    with open(path, newline='', encoding='utf-8-sig') as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=',;\t')
        except Exception:
            dialect = csv.excel
        for row in csv.DictReader(f, dialect=dialect):
            raw = pick(row, 'amount') or pick(row, 'quantity')
            try:
                amount = abs(float(str(raw).replace(',', '').replace('$', '').strip()))
            except Exception:
                continue
            status = (str(pick(row, 'status') or 'SUCCESS')).strip().upper()
            if status and not any(s in status for s in ('SUCCESS', 'COMPLETE', 'DONE')):
                continue
            out.append({'ts': pick(row, 'time') or pick(row, 'date'),
                        'from': pick(row, 'from'), 'to': pick(row, 'to'),
                        'amount': amount, 'kind': 'imported', 'status': status})
    return out


def report(rows):
    agg = {}
    for r in rows:
        src, dst = norm(r.get('from')), norm(r.get('to'))
        amt = float(r.get('amount') or 0)
        if amt <= 0:
            continue
        for a in (src, dst):
            agg.setdefault(a, {'in': 0.0, 'out': 0.0, 'from_master': 0.0, 'to_master': 0.0})
        agg[src]['out'] += amt
        agg[dst]['in'] += amt
        if src == MASTER:
            agg[dst]['from_master'] += amt
        if dst == MASTER:
            agg[src]['to_master'] += amt
    return agg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--csv', action='append', default=[])
    a = ap.parse_args()
    rows = load_ledger()
    print('ledger rows: %d' % len(rows))
    for p in a.csv:
        extra = parse_csv(p)
        print('csv %s: %d usable rows' % (os.path.basename(p), len(extra)))
        rows += extra
    agg = report(rows)
    print()
    print('%-32s %12s %12s %12s' % ('account', 'from master', 'to master', 'net'))
    tot = 0.0
    for name in sorted(agg):
        v = agg[name]
        net = v['from_master'] - v['to_master']
        if name != MASTER:
            tot += net
        print('%-32s %12.2f %12.2f %12.2f' % (name, v['from_master'], v['to_master'], net))
    print()
    print('total net invested across subaccounts: %.2f' % tot)


if __name__ == '__main__':
    main()
