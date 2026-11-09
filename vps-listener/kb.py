"""Permanent issue & error knowledge base for the Hyper app.

Every problem we have ever found and fixed lives here. Scans read it, fixes
update it, and nothing is ever deleted - only superseded.

Storage: /var/lib/hypercopy-copy/issues.jsonl (append-only JSON lines, one
issue per line). Seeded on first run from knowledge_seed.json, which is
committed to the repo so a rebuilt VM starts with the full history.

Entry shape:
{
  "id": "KB-001",
  "title": "Invested amount drifted with trade PnL",
  "area": "accounting",
  "severity": "critical",
  "symptoms": ["invested changes without a transfer", "..."],
  "root_cause": "invested_usd was computed as wallet - cumulative PnL",
  "fix": "read the transfer ledger invest_exact.json",
  "files": ["vps-listener/listener.py", "vps-listener/copy_listener.py"],
  "verify": "compared invested across two samples 100s apart with live trades",
  "detect": {"message_pattern": "invested", "log_pattern": null},
  "safety": "safe|needs_approval",
  "auto_fixable": false,
  "status": "fixed|open|regressed",
  "first_seen": "2026-11-08",
  "fixed_on": "2026-11-08",
  "occurrences": 1
}
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone

STORE = '/var/lib/hypercopy-copy/issues.jsonl'
SEED = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'knowledge_seed.json')


def _now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def load() -> list:
    entries = {}
    order = []
    if os.path.exists(STORE):
        try:
            with open(STORE) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        e = json.loads(line)
                    except Exception:
                        continue
                    eid = e.get('id')
                    if not eid:
                        continue
                    if eid not in entries:
                        order.append(eid)
                    entries[eid] = e
        except Exception:
            pass
    if not entries and os.path.exists(SEED):
        # first boot on a fresh VM: seed from the committed history
        try:
            with open(SEED) as f:
                for e in json.load(f):
                    entries[e['id']] = e
                    order.append(e['id'])
        except Exception:
            pass
        save([entries[i] for i in order])
    return [entries[i] for i in order]


def save(entries: list) -> None:
    tmp = STORE + '.tmp'
    os.makedirs(os.path.dirname(STORE), exist_ok=True)
    with open(tmp, 'w') as f:
        for e in entries:
            f.write(json.dumps(e, sort_keys=False) + '\n')
    os.replace(tmp, STORE)


def next_id(entries: list) -> str:
    nums = [int(e['id'].split('-')[1]) for e in entries if str(e.get('id', '')).startswith('KB-')]
    return 'KB-%03d' % ((max(nums) + 1) if nums else 1)


def _matches(entry: dict, items: list, log_text: str) -> list:
    """Return the findings that this known issue explains."""
    det = entry.get('detect') or {}
    out = []
    pat = det.get('message_pattern')
    area = det.get('area')
    if pat:
        try:
            rx = re.compile(pat, re.I)
        except re.error:
            rx = None
        if rx:
            for it in items:
                if area and it.get('area') and area != it.get('area'):
                    continue
                if rx.search(it.get('message') or ''):
                    out.append(it)
    lpat = det.get('log_pattern')
    if lpat and log_text:
        try:
            if re.search(lpat, log_text, re.I):
                out.append({'area': entry.get('area') or 'log',
                            'message': 'log pattern matched: ' + lpat,
                            'severity': entry.get('severity') or 'warning'})
        except re.error:
            pass
    return out


def analyse(items: list, log_text: str = '') -> dict:
    """Compare current findings against the whole history.

    Returns {"annotations": {message: kb}, "regressions": [...], "known": [...]}
    """
    entries = load()
    annotations = {}
    regressions = []
    known = []
    for e in entries:
        hits = _matches(e, items, log_text)
        if not hits:
            continue
        was_fixed = (e.get('status') == 'fixed')
        info = {
            'id': e['id'],
            'title': e.get('title'),
            'status': e.get('status'),
            'fixed_on': e.get('fixed_on'),
            'safety': e.get('safety', 'needs_approval'),
            'auto_fixable': bool(e.get('auto_fixable')),
            'regression': was_fixed,
            'occurrences': e.get('occurrences', 0),
            'previous_fix': e.get('fix'),
        }
        known.append({'id': e['id'], 'title': e.get('title'),
                      'regression': was_fixed, 'hits': len(hits)})
        if was_fixed:
            regressions.append({'id': e['id'], 'title': e.get('title'),
                                'fixed_on': e.get('fixed_on')})
        for it in hits:
            msg = it.get('message') or ''
            annotations[msg] = info
    return {'annotations': annotations, 'regressions': regressions, 'known': known,
            'total_entries': len(entries)}


def record(entry_ids: list, outcome: str, detail: str = '', fixed: bool = False) -> None:
    """Update occurrences and status for the entries involved in a scan/fix."""
    if not entry_ids:
        return
    entries = load()
    by_id = {e['id']: e for e in entries}
    changed = False
    for eid in entry_ids:
        e = by_id.get(eid)
        if not e:
            continue
        e['occurrences'] = int(e.get('occurrences') or 0) + 1
        e['last_seen'] = _now()
        e['last_outcome'] = outcome
        if detail:
            e['last_detail'] = detail[:400]
        if fixed:
            e['status'] = 'fixed'
            e['fixed_on'] = _now()
        elif e.get('status') == 'regressed':
            pass
        changed = True
    if changed:
        save(entries)


def flag_regressions(entry_ids: list) -> None:
    entries = load()
    changed = False
    for e in entries:
        if e['id'] in entry_ids and e.get('status') == 'fixed':
            e['status'] = 'regressed'
            e['regressed_on'] = _now()
            changed = True
    if changed:
        save(entries)


def add(title: str, symptoms: list, root_cause: str, fix: str, files: list,
        verify: str, area: str = 'general', severity: str = 'warning',
        message_pattern: str = None, log_pattern: str = None,
        safety: str = 'needs_approval', auto_fixable: bool = False,
        status: str = 'open') -> dict:
    """Append a brand new issue discovered now (used by scans and by me)."""
    entries = load()
    eid = next_id(entries)
    entry = {
        'id': eid, 'title': title, 'area': area, 'severity': severity,
        'symptoms': symptoms, 'root_cause': root_cause, 'fix': fix, 'files': files,
        'verify': verify,
        'detect': {'message_pattern': message_pattern, 'log_pattern': log_pattern,
                   'area': area},
        'safety': safety, 'auto_fixable': auto_fixable, 'status': status,
        'first_seen': _now(), 'fixed_on': _now() if status == 'fixed' else None,
        'occurrences': 1,
    }
    entries.append(entry)
    save(entries)
    return entry
