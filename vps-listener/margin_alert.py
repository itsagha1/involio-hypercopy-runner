"""Immediate margin-breach alerts (owner standing rule 2026-10-21).

When the TOTAL initial margin of any account reaches 70% of its wallet
balance, post an alert record to Base44 (receiveMarginAlert) which triggers
an instant WhatsApp notification to the owner. Edge-triggered with
hysteresis: fires once on crossing 70%, re-arms only below 65% so an account
hovering at the threshold cannot spam. Failure to post re-arms so the next
check retries.
"""
import time
import requests

BASE44_ALERT_URL = 'https://superagent-85fc14c0.base44.app/functions/receiveMarginAlert'
FIRE_PCT = 70.0
REARM_PCT = 65.0
CHECK_COOLDOWN_S = 60.0

_state = {'ts': {}, 'fired': set()}


def margin_pct(wallet, positions):
    used = 0.0
    for p in (positions or []):
        try:
            if float(p.get('size') or 0) > 0:
                used += float(p.get('positionIM') or 0)
        except (TypeError, ValueError):
            continue
    if not wallet or float(wallet) <= 0:
        return 0.0, 0.0
    return 100.0 * used / float(wallet), used


def check(account, wallet, positions, secret, log=print):
    """Returns the used-pct when an alert fired this call, else None."""
    now = time.time()
    pct, used = margin_pct(wallet, positions)
    if pct >= FIRE_PCT:
        if account in _state['fired']:
            return None
        _state['ts'][account] = now
        _state['fired'].add(account)
        detail = '; '.join(sorted('%s %sx im=%s' % (p.get('symbol'), p.get('leverage'), p.get('positionIM'))
                                  for p in (positions or [])
                                  if _safe_size(p) > 0)[:12])
        try:
            r = requests.post(BASE44_ALERT_URL, json={
                'secret': secret or '', 'account': str(account), 'kind': 'margin_70',
                'used_pct': round(pct, 2), 'wallet': round(float(wallet or 0), 2),
                'margin_used': round(used, 2), 'detail': detail[:500]}, timeout=20)
            if getattr(r, 'status_code', 0) != 200:
                raise RuntimeError('HTTP %s: %s' % (r.status_code, r.text[:80]))
        except Exception as e:
            _state['fired'].discard(account)
            log('ERROR margin-alert post failed for %s: %s: %s' % (account, type(e).__name__, str(e)[:80]))
            return None
        log('ALERT MARGIN-70 %s: used %.1f%% of wallet (%s of %s) - owner notified' % (
            account, pct, round(used, 2), round(float(wallet or 0), 2)))
        return pct
    if account in _state['fired'] and pct < REARM_PCT:
        _state['fired'].discard(account)
        log('LOG margin monitor: %s margin back below %.0f%% (%.1f%%); alert re-armed' % (
            account, REARM_PCT, pct))
    return None


def _safe_size(p):
    try:
        return float(p.get('size') or 0)
    except (TypeError, ValueError):
        return 0.0
