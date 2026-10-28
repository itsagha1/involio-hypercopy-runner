#!/usr/bin/env python3
"""Copy-fleet self-audit (owner 2026-10-28).

Owner directive: "I don't want trades to be missed or wrongly labelled or
skipped due to a bug. In the daily health check do all scanning to find bugs
and fix them."

This script scans EVERY engine (main + 5 copy subaccounts) for the failure
classes that actually bite this fleet:

  A. label integrity      - a live mirror stamped into the manual (owner-managed)
                            list, stale manual entries, duplicates
  B. mirror vs exchange   - mirror registered but position flat (and the reverse:
                            live position no mirror owns -> unowned/mislabelled)
  C. missed mirrors       - source holds N open positions, we only account for M,
                            and the log tail does not explain the gap with a
                            legitimate skip (2% ROI watch, sub-minimum, hold...)
  D. skip bug signatures  - the exact bug shapes we have hit before (cross-source
                            "live position conflict" against our own mirror)
  E. engine health        - service inactive, stale poll, error counter
  F. funds-at-risk        - retained losing position with no exchange-side
                            protection (no TP trigger, no stop)
  G. margin-hold sanity   - hold engaged although margin is well below threshold

Conservative auto-fixes are applied through the engine's POST /audit/fix
endpoint (the engine owns its in-memory state, so the runtime applies the edit
under its own lock; editing the state file from here would be overwritten):

  * manual entry that is really a live mirror      -> manual_remove
  * manual entry not held on the exchange at all   -> manual_remove
  * duplicate manual entries                       -> manual_remove
  * live position no mirror/baseline/manual owns   -> manual_add (visible, never
                                                      auto-managed)
  * mirror flat on exchange 2 audits in a row      -> deregister
                                                      (source still open would
                                                      re-open it next poll)

Everything is written to /var/lib/hypercopy-copy/audit_report.json (read by the
daily digest) and /var/lib/hypercopy-copy/audit.log. Runs hourly from
hypercopy-audit.timer. Zero Base44 credits.
"""
import hmac
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

LISTENER_ENV = '/home/ubuntu/involio-hypercopy-runner/vps-listener/.env'
STATE_DIR = '/var/lib/hypercopy-copy'
REPORT_FILE = os.path.join(STATE_DIR, 'audit_report.json')
AUDIT_LOG = os.path.join(STATE_DIR, 'audit.log')

MAIN_PORT = 8000
COPY_ACCOUNTS = [
    (8001, 'copy-mainport'),
    (8002, 'copy-bakery'),
    (8003, 'copy-scalping'),
    (8004, 'copy-kings'),
    (8005, 'copy-bluespire'),
]

ACCOUNT_SUMMARY = {}
COPY_STALE_SEC = 8 * 60        # copy engines poll every 120s; 8min = clearly stale
MAIN_STALE_SEC = 4 * 60        # main self-polls ~80s
MARGIN_RELEASE_PCT = 40.0      # owner 2026-10-28 hysteresis: release at <= 40%


# ------------------------------------------------------------------ plumbing

def now():
    return datetime.now(timezone.utc)


def load_secret():
    try:
        for line in open(LISTENER_ENV):
            if line.startswith('WEBHOOK_SHARED_SECRET='):
                return line.split('=', 1)[1].strip()
    except OSError:
        pass
    return ''


SECRET = load_secret()


def http_json(path, payload=None, headers=None, timeout=25):
    url = path if path.startswith('http') else 'http://127.0.0.1' + path
    data = None
    hdrs = {'Accept': 'application/json'}
    if headers:
        hdrs.update(headers)
    if payload is not None:
        data = json.dumps(payload).encode()
        hdrs['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, data=data, headers=hdrs,
                                 method='POST' if data is not None else 'GET')
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode() or '{}')


def get_status(port, is_main=False, attempts=3):
    """Fetch /status; engines can be briefly busy mid-poll, so retry."""
    last = None
    for i in range(attempts):
        try:
            hdrs = {'X-Signature': SECRET}
            if not is_main:
                hdrs['X-Internal-Status'] = '1'
            return http_json('http://127.0.0.1:%d/status' % port,
                             headers=hdrs, timeout=20)
        except Exception as e:  # noqa: BLE001 - any transport/shape error retries
            last = e
            time.sleep(2 + 2 * i)
    raise last


def post_fix(port, payload):
    """Apply state repairs through the engine's own lock."""
    try:
        return http_json('http://127.0.0.1:%d/audit/fix' % port, payload,
                         headers={'X-Signature': SECRET}, timeout=25)
    except Exception as e:  # noqa: BLE001
        return {'ok': False, 'error': '%s: %s' % (type(e).__name__, e)}


def service_active(name):
    try:
        out = subprocess.run(['systemctl', 'is-active', name],
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip() == 'active', out.stdout.strip()
    except Exception as e:  # noqa: BLE001
        return False, 'unknown (%s)' % e


def load_state(name):
    p = os.path.join(STATE_DIR, '%s_state.json' % name)
    try:
        return json.load(open(p))
    except Exception:  # noqa: BLE001
        return {}


def parse_ts(value):
    if not value:
        return None
    try:
        txt = str(value).replace('Z', '+00:00')
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:  # noqa: BLE001
        return None


def age_seconds(value):
    dt = parse_ts(value)
    if not dt:
        return None
    return (now() - dt).total_seconds()


def sym_of(coin):
    coin = str(coin or '').strip()
    return coin if coin.endswith('USDT') else coin + 'USDT'


def coin_of(symbol):
    return str(symbol or '').replace('USDT', '')


def side_norm(val):
    v = str(val or '').strip().lower()
    if v in ('buy', 'long'):
        return 'long'
    if v in ('sell', 'short'):
        return 'short'
    return v


def fmt_age(sec):
    if sec is None:
        return 'unknown'
    if sec < 120:
        return '%ds' % sec
    if sec < 7200:
        return '%dm' % (sec // 60)
    return '%.1fh' % (sec / 3600.0)


# ------------------------------------------------------------------ findings

class Findings:
    def __init__(self):
        self.items = []

    def add(self, account, code, severity, detail, fixed=None, subject=''):
        self.items.append({
            'account': account,
            'code': code,
            'severity': severity,
            'detail': detail,
            'auto_fixed': bool(fixed),
            'fix': fixed or '',
            'subject': subject,
        })

    def by_severity(self, sev):
        return [f for f in self.items if f['severity'] == sev]

    def count(self, code=None):
        if code is None:
            return len(self.items)
        return len([f for f in self.items if f['code'] == code])


# ------------------------------------------------------------------ checks

def check_healthy(account, port, status, is_main, findings):
    """E. engine health: service, freshness, error counter."""
    svc = 'hypercopy-listener' if is_main else account
    active, raw = service_active(svc)
    if not active:
        findings.add(account, 'service_inactive', 'critical',
                     'systemd unit %s reports %s' % (svc, raw))
        return False

    ts_key = 'last_processed' if is_main else 'last_poll'
    age = age_seconds(status.get(ts_key))
    limit = MAIN_STALE_SEC if is_main else COPY_STALE_SEC
    if age is None:
        findings.add(account, 'poll_time_unknown', 'high',
                     '%s missing from /status' % ts_key)
    elif age > limit:
        findings.add(account, 'stale_poll', 'critical',
                     '%s is %s old (limit %s)' % (ts_key, fmt_age(age), fmt_age(limit)))

    errs = status.get('errors_last_run') or 0
    if errs:
        findings.add(account, 'poll_errors', 'high',
                     'errors_last_run=%s on %s' % (errs, status.get('code_version')))
    return True


def live_positions(status):
    """{(SYMBOL, side): size} from the engine's cached Bybit snapshot."""
    pos = ((status.get('bybit') or {}).get('positions') or [])
    out = {}
    for p in pos:
        try:
            size = float(p.get('size') or 0)
        except (TypeError, ValueError):
            size = 0.0
        if abs(size) > 0:
            out[(str(p.get('symbol')), side_norm(p.get('side')))] = size
    return out


def mirrors_of(status):
    out = {}
    for key, m in (status.get('mirrored') or {}).items():
        if isinstance(m, dict):
            out[key] = {
                'symbol': m.get('symbol'),
                'side': side_norm(m.get('side')),
                'qty': m.get('qty'),
                'profile': m.get('profile') or (key.split('|')[0] if '|' in key else ''),
            }
    return out


def check_labels(account, port, status, findings, prev_codes, apply_fixes=True):
    """A. label integrity + B. unowned positions."""
    manual = list(status.get('manual') or [])
    mirrors = mirrors_of(status)
    live = live_positions(status)
    baseline = set()
    bp = status.get('baseline_positions')
    if isinstance(bp, (list, tuple)):
        baseline = {str(b) for b in bp}
    if not baseline:
        st = load_state(account)
        bl = st.get('baseline')
        if isinstance(bl, (list, tuple)):
            baseline = {str(b) for b in bl}

    mirror_cs = {(m['symbol'], m['side']): key for key, m in mirrors.items()}

    remove, add = [], []
    seen = set()
    for bk in manual:
        bkl = str(bk)
        if bkl in seen:
            remove.append(bkl)
            findings.add(account, 'manual_duplicate', 'info',
                         'duplicate manual entry %s' % bkl,
                         fixed='manual_remove' if apply_fixes else None)
            continue
        seen.add(bkl)
        coin, _, sd = bkl.partition('/')
        sym, sd = sym_of(coin), side_norm(sd)
        if (sym, sd) in mirror_cs:
            remove.append(bkl)
            findings.add(account, 'manual_overlap_mirror', 'high',
                         '%s is in the manual (owner-managed) list but is a LIVE '
                         'MIRROR of %s - dashboard mislabels it and future entries '
                         'for that coin/side are blocked'
                         % (bkl, mirror_cs[(sym, sd)]),
                         fixed='manual_remove' if apply_fixes else None)
        elif (sym, sd) not in live:
            remove.append(bkl)
            findings.add(account, 'manual_stale', 'info',
                         'manual entry %s is not held on the exchange' % bkl,
                         fixed='manual_remove' if apply_fixes else None)

    owned = set(mirror_cs.keys())
    # retained (source-vanished) positions are still ENGINE-managed: never manual
    for rk, rv in (status.get('retained_trailing') or status.get('retained') or {}).items():
        if isinstance(rv, dict) and rv.get('symbol'):
            owned.add((rv.get('symbol'), side_norm(rv.get('side'))))
    for bk in seen:
        coin, _, sd = str(bk).partition('/')
        owned.add((sym_of(coin), side_norm(sd)))
    for b in baseline:
        if '/' in b:
            coin, _, sd = b.partition('/')
            owned.add((sym_of(coin), side_norm(sd)))
    for (sym, sd), size in live.items():
        if (sym, sd) in owned:
            continue
        bk = '%s/%s' % (coin_of(sym), sd)
        if bk not in seen:
            add.append(bk)
            findings.add(account, 'position_unowned', 'high',
                         'live position %s/%s (%s) is not owned by any mirror, '
                         'baseline or manual entry - it would be invisible/'
                         'misclassified on the dashboard' % (sym, sd, size),
                         fixed='manual_add' if apply_fixes else None)

    fixes = {}
    if remove:
        fixes['manual_remove'] = remove
    if add:
        fixes['manual_add'] = add
    if fixes and apply_fixes:
        result = post_fix(port, fixes)
        applied = result.get('applied') or []
        if not result.get('ok'):
            findings.add(account, 'fix_failed', 'high',
                         'POST /audit/fix failed: %s' % result.get('error'))
    return fixes


def check_mirror_divergence(account, port, status, findings, prev_keys, apply_fixes=True):
    """B. mirror registered but exchange flat (2 consecutive audits -> deregister)."""
    live = live_positions(status)
    flat = []
    for key, m in mirrors_of(status).items():
        if (m['symbol'], m['side']) not in live:
            tag = '%s|flat' % key
            if tag in prev_keys:
                flat.append(key)
                findings.add(account, 'mirror_flat_on_exchange', 'high',
                             'mirror %s (%s/%s) has no live exchange position in '
                             'two consecutive audits - deregistering so the next '
                             'poll re-opens it cleanly if the source still holds it'
                             % (key, m['symbol'], m['side']),
                             fixed='deregister' if apply_fixes else None, subject=key)
            else:
                findings.add(account, 'mirror_flat_first_seen', 'info',
                             'mirror %s (%s/%s) shows no live exchange position '
                             '(first audit; watching)'
                             % (key, m['symbol'], m['side']), subject=key)
    if flat and apply_fixes:
        result = post_fix(port, {'deregister': flat})
        if not result.get('ok'):
            findings.add(account, 'fix_failed', 'high',
                         'POST /audit/fix deregister failed: %s' % result.get('error'))


def baseline_count(account, status):
    """Number of baselined (pre-existing, never-mirrored) positions."""
    bp = status.get('baseline_count')
    if isinstance(bp, int):
        return bp
    st = load_state(account)
    bl = st.get('baseline')
    return len(bl) if isinstance(bl, (list, tuple)) else 0


def read_log(account, max_lines=1500):
    """Tail of the account's action log (full file, not the 20-line /status tail)."""
    path = os.path.join(STATE_DIR, '%s_actions.log' % account)
    try:
        with open(path, 'rb') as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 500000))
            return f.read().decode('utf-8', 'replace').splitlines()[-max_lines:]
    except OSError:
        return []


def check_missed_mirrors(account, status, findings):
    """C. source holds positions we are neither mirroring nor legitimately holding
    back. Uses the engine's own source book (ground truth) and the full action log
    for evidence, so baselined positions and legitimate skips are not false alarms."""
    st = load_state(account)
    book = ((st.get('books') or {}).get('positions') or [])
    if not book:
        if status.get('source_stats'):
            findings.add(account, 'source_book_missing', 'info',
                         'no cached source book this poll; gap check skipped')
        return

    log = read_log(account)
    baseline_keys = set()
    for line in log:
        if 'baseline position' in line:
            m = re.search(r'\|([^|\s]+)/([a-z]+)\|[^:\s]*:\s*baseline position', line)
            if m:
                baseline_keys.add((m.group(1).upper(), side_norm(m.group(2))))

    covered = {}
    for grp in ('mirrored', 'skip_watch', 'retained', 'entry_blocks'):
        src = status.get(grp) or {}
        if grp == 'retained' and isinstance(status.get('retained_trailing'), dict):
            src = status.get('retained_trailing')
        if grp == 'entry_blocks':
            src = st.get('entry_blocks') or {}
        if isinstance(src, dict):
            for key in src:
                parts = str(key).split('|')
                if len(parts) >= 2:
                    coin, _, sd = parts[1].partition('/')
                    covered[(parts[0], coin.upper(), side_norm(sd))] = grp

    unexplained = []
    for pos in book:
        profile = pos.get('profile')
        coin = str(pos.get('ticker') or '').upper()
        sd = side_norm(pos.get('side'))
        if not profile or not coin:
            continue
        if (profile, coin, sd) in covered:
            continue
        if (coin, sd) in baseline_keys:
            continue
        # last log evidence for this exact trade family
        needle = '%s|%s/%s' % (profile, coin, sd)
        evidence = [ln for ln in log if needle in ln and
                    any(w in ln for w in ('SKIP', 'RE-ENTRY', 'ERROR', 'degraded', 'baseline'))]
        if evidence:
            findings.add(account, 'source_skip_explained', 'info',
                         '%s is not mirrored but the log explains it: %s'
                         % (needle, evidence[-1][-130:]), subject=needle)
        else:
            unexplained.append(needle)

    for needle in unexplained:
        findings.add(account, 'missed_mirror_suspect', 'high',
                     '%s is open at the source but has no mirror, no skip-watch, no '
                     'baseline and no skip reason in the log - possible missed trade'
                     % needle, subject=needle)


def check_skip_signatures(account, status, findings):
    """D. known bug shapes in the recent log tail (current process run only)."""
    mirrors = mirrors_of(status)
    mirror_cs = {(m['symbol'], m['side']) for m in mirrors.values()}
    started = process_start(account)
    for line in (status.get('log_tail') or []):
        line = str(line)
        ts = line_ts(line)
        if started and ts and ts < started:
            continue  # pre-restart history: already fixed
        if 'live position conflict on Bybit for' in line:
            try:
                sym = line.split('for ')[1].split('/')[0].strip()
                sd = side_norm(line.split('for ')[1].split('/')[1].split()[0])
            except IndexError:
                continue
            if (sym, sd) in mirror_cs:
                findings.add(account, 'conflict_vs_own_mirror', 'critical',
                             'log shows a conflict skip against our OWN mirror '
                             '(%s/%s) - this is the mislabel bug class: %s'
                             % (sym, sd, line[-140:]))
        if 'manual protect active' in line:
            findings.add(account, 'skip_manual_protect', 'info',
                         'entry skipped by a manual-protect block: %s' % line[-140:])


def check_funds_at_risk(account, status, findings):
    """F. retained losing position with no exchange-side protection."""
    live = live_positions(status)
    retained = status.get('retained_trailing')
    if not isinstance(retained, dict):
        retained = status.get('retained') or {}
    for key, r in retained.items():
        if not isinstance(r, dict):
            continue
        sym = r.get('symbol')
        sd = side_norm(r.get('side'))
        if (sym, sd) not in live:
            continue
        status_txt = str(r.get('status') or '')
        tp = r.get('tp_trigger')
        sl = r.get('current_sl')
        if status_txt in ('trailing_active', 'trailing') or tp or sl:
            continue
        findings.add(account, 'retained_unprotected', 'critical',
                     'retained %s (%s/%s) is live with no TP trigger and no stop '
                     '- unprotected exposure' % (key, sym, sd))


def check_main_reconciliation(status, findings):
    """Main listener tracks orphans/mismatches itself - surface them."""
    for name, label in (('orphans', 'exchange position with no mirror'),
                        ('mismatches', 'mirror/exchange size mismatch')):
        bad = status.get(name) or {}
        keys = [k for k, v in bad.items() if v] if isinstance(bad, dict) else list(bad)
        if keys:
            findings.add('main', name, 'high',
                         '%d %s: %s' % (len(keys), label, ', '.join(map(str, keys[:4]))))


def check_margin_hold(account, status, findings):
    """G. hold engaged while margin is already below the release threshold."""
    if not status.get('margin_hold'):
        return
    try:
        pct = float(status.get('margin_pct') or 0)
    except (TypeError, ValueError):
        pct = 0.0
    if pct and pct <= MARGIN_RELEASE_PCT:
        findings.add(account, 'margin_hold_stale', 'high',
                     'margin_hold still engaged at %.1f%% (release threshold %.0f%%)'
                     % (pct, MARGIN_RELEASE_PCT))
    else:
        findings.add(account, 'margin_hold_active', 'info',
                     'entry hold active at %.1f%% margin (no new entries until '
                     '<= %.0f%%) - existing positions unaffected' % (pct, MARGIN_RELEASE_PCT))


# ------------------------------------------------------------------ driver

def audit_account(name, port, is_main, findings, prev_keys, apply_fixes):
    try:
        status = get_status(port, is_main=is_main)
    except Exception as e:  # noqa: BLE001
        findings.add(name, 'status_unreachable', 'critical',
                     'could not read /status on port %d: %s: %s'
                     % (port, type(e).__name__, e))
        return
    if not check_healthy(name, port, status, is_main, findings):
        return
    if is_main:
        check_main_reconciliation(status, findings)
    check_labels(name, port, status, findings, prev_keys, apply_fixes=apply_fixes)
    check_mirror_divergence(name, port, status, findings, prev_keys, apply_fixes=apply_fixes)
    check_missed_mirrors(name, status, findings)
    check_skip_signatures(name, status, findings)
    check_funds_at_risk(name, status, findings)
    check_margin_hold(name, status, findings)
    ACCOUNT_SUMMARY[name] = {
        'mirrors': len(mirrors_of(status)),
        'manual': len(status.get('manual') or []),
        'retained': len(status.get('retained_trailing') or status.get('retained') or {}),
        'skip_watch': len(status.get('skip_watch') or {}),
        'margin_pct': status.get('margin_pct'),
        'margin_hold': bool(status.get('margin_hold')),
        'code_version': status.get('code_version'),
    }


def main(argv):
    apply_fixes = '--dry-run' not in argv
    try:
        prev = json.load(open(REPORT_FILE))
    except Exception:  # noqa: BLE001
        prev = {}
    # a mirror flagged flat in the PREVIOUS audit gets deregistered if still flat
    prev_keys = {'%s|flat' % f.get('subject') for f in (prev.get('findings') or [])
                 if f.get('code') in ('mirror_flat_first_seen', 'mirror_flat_on_exchange')
                 and f.get('subject')}

    findings = Findings()
    started = now()
    audit_account('main', MAIN_PORT, True, findings, prev_keys, apply_fixes)
    for port, name in COPY_ACCOUNTS:
        audit_account(name, port, False, findings, prev_keys, apply_fixes)

    summary = {
        'critical': len(findings.by_severity('critical')),
        'high': len(findings.by_severity('high')),
        'info': len(findings.by_severity('info')),
        'auto_fixes': len([f for f in findings.items if f['auto_fixed']]),
        'healthy': len(findings.by_severity('critical')) == 0 and len(findings.by_severity('high')) == 0,
    }
    report = {
        'ok': True,
        'accounts': ACCOUNT_SUMMARY,
        'generated_at': started.isoformat(timespec='seconds'),
        'took_ms': int((now() - started).total_seconds() * 1000),
        'fixes_applied': apply_fixes,
        'summary': summary,
        'findings': findings.items,
    }
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        tmp = REPORT_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(report, f, indent=1)
        os.replace(tmp, REPORT_FILE)
    except OSError as e:
        print('WARN cannot write report: %s' % e, file=sys.stderr)

    stamp = started.strftime('%Y-%m-%d %H:%M:%S')
    lines = []
    for f in findings.items:
        if f['severity'] == 'info':
            continue
        lines.append('[%s UTC] AUDIT %s %s %s: %s%s'
                     % (stamp, f['severity'].upper(), f['account'], f['code'],
                        f['detail'], (' [FIXED: %s]' % f['fix']) if f['auto_fixed'] else ''))
    if not lines:
        lines.append('[%s UTC] AUDIT OK: no critical/high findings across %d engines'
                     % (stamp, 1 + len(COPY_ACCOUNTS)))
    try:
        with open(AUDIT_LOG, 'a') as fh:
            fh.write('\n'.join(lines) + '\n')
    except OSError:
        pass
    print('\n'.join(lines))
    print('summary: %s' % json.dumps(summary))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
