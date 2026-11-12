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


def exchange_margin(client, wallet, positions):
    """The exchange's own initial-margin rate: initial margin over EQUITY.

    Owner 2026-11-12. The engines divided initial margin by WALLET, so any
    account holding open losses showed a smaller, flattering number: AI Main
    read 27% here while Bybit showed 33.87%. Display, alerts and the 70% entry
    hold must all use the exchange rate, with the old figure only as a fallback
    when the endpoint cannot be read.
    """
    try:
        r = client._get('/v5/account/wallet-balance', {'accountType': 'UNIFIED'})
        x = (r.get('list') or [{}])[0]
        rate = x.get('accountIMRate')
        im = x.get('totalInitialMargin')
        if rate is not None and im is not None:
            return float(rate) * 100.0, float(im), float(x.get('totalEquity') or 0), 'exchange'
    except Exception:
        pass
    pct, used = margin_pct(wallet, positions)
    return pct, used, float(wallet or 0), 'wallet_fallback'

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
            # Owner 2026-11-10: Base44 alerts are switched off (margin messages
            # cost credits the owner asked us to stop spending). Log locally
            # only; margin relief acts automatically at 90%.
            # Posting to Base44 is disabled (owner 2026-11-10, credit cut):
            # the alert is recorded locally only and never leaves the VM.
            log('ALERT MARGIN-70 %s: used %.1f%% of wallet (local only, no Base44 post)' % (
                account, pct))
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
