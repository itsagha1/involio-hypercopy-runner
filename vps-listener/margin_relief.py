#!/usr/bin/env python3
"""Margin relief between the master funding account and the subaccounts.

OWNER RULE (2026-11-07):
  * any unified subaccount that reaches 90% margin usage borrows 100-200 USDT
    from the master funding account, enough to bring margin back to <=60%
  * when that account falls back to <=30% margin, the same amount is returned
    to the master funding account
  * if it climbs to 90% again, it borrows again

Safety: per-account cooldown, hard cap on outstanding relief, never borrows more
than the master actually holds, dry-run by default (MARGIN_RELIEF_DRY_RUN=1).
"""
import json, os, sys, time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import move_funds as mf
from margin_alert import margin_pct

TRIGGER_PCT = 90.0
TARGET_PCT = 60.0
REPAY_PCT = 30.0
MIN_AMOUNT = 100.0
MAX_AMOUNT = 200.0
MAX_OUTSTANDING = 600.0
COOLDOWN_SECONDS = 600
STATE_FILE = '/var/lib/hypercopy-copy/margin_relief.json'
LOG_FILE = '/var/lib/hypercopy-copy/margin_relief.log'
DRY_RUN = os.environ.get('MARGIN_RELIEF_DRY_RUN', '1') != '0'

# Owner renamed the Bybit accounts 2026-11-07: never use the technical keys in
# anything user-facing. Keys stay for wiring (state, ports, env files) only.
LABELS = {
    'main': 'AI Main account (BYBIT_MAIN)',
    'mainport': 'Sub account 01 (MAINPORT)',
    'bakery': 'Sub account 02 (BAKERY)',
    'scalping': 'Sub account 03 (SCALPING)',
    'kings': 'Sub account 04 (KINGS)',
    'bluespire': 'Sub account 05 (BLUESPIIRE)',
    'hypercopy': 'hypercopy',
}

ACCOUNTS = ('main', 'mainport', 'bakery', 'scalping', 'kings', 'bluespire', 'hypercopy')


def log(msg):
    line = '[%s] %s%s' % (datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
                          '[DRY] ' if DRY_RUN else '', msg)
    print(line)
    try:
        with open(LOG_FILE, 'a') as f:
            f.write(line + '\n')
    except Exception:
        pass


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {'accounts': {}}


def save_state(state):
    try:
        tmp = STATE_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(state, f, indent=1)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log('ERROR saving state: %s' % e)


def master_balance():
    try:
        return mf.balance('master')
    except Exception as e:
        log('ERROR reading master balance: %s' % str(e)[:80])
        return 0.0


def main():
    state = load_state()
    now = time.time()
    mbal = master_balance()
    changed = False
    if abs(float(state.get('master_pool_usd') or 0) - mbal) > 0.005:
        state['master_pool_usd'] = round(mbal, 2)
        state['master_pool_ts'] = now
        changed = True
    for name in ACCOUNTS:
        key = name
        name = LABELS.get(key, key)
        try:
            c = mf.client_for(key)
            summ = c.get_account_summary()
            wallet = float(summ.get('wallet_balance') or 0)
            avail = float(summ.get('available_balance') or 0)
            pos = [p for p in c.get_positions() if float(p.get('size') or 0) > 0]
        except Exception as e:
            log('%s: read failed (%s)' % (name, str(e)[:70]))
            continue
        pct, used = margin_pct(wallet, pos)
        st = state['accounts'].setdefault(key, {'outstanding': 0.0, 'last_action_ts': 0})
        out = float(st.get('outstanding') or 0)
        if DRY_RUN or pct >= 70:
            log('%s: margin %.1f%% | wallet %.2f | outstanding %.2f' % (name, pct, wallet, out))
        since = now - float(st.get('last_action_ts') or 0)

        # --- repayment: recovered enough to release the borrowed funds ---
        if out > 0 and pct <= REPAY_PCT:
            amount = round(min(out, avail * 0.95), 2)
            if amount < 1:
                log('%s: margin %.1f%% <= %.0f%% but only %.2f USDT available to repay' % (name, pct, REPAY_PCT, avail))
                continue
            log('%s: margin %.1f%% <= %.0f%% -> returning %.2f USDT to master (outstanding %.2f)' % (name, pct, REPAY_PCT, amount, out))
            if not DRY_RUN:
                try:
                    res = mf.transfer(key, 'master', amount, execute=True)
                    if (res or {}).get('status') != 'SUCCESS':
                        log('%s: repayment not confirmed: %s' % (name, json.dumps(res)[:120]))
                        continue
                    st['outstanding'] = round(out - amount, 2)
                    st['last_action_ts'] = now
                    st['events'] = (st.get('events') or [])[-9:] + [{'t': now, 'kind': 'repay', 'amount': amount, 'pct': round(pct, 1)}]
                    changed = True
                    mbal = master_balance()
                except Exception as e:
                    log('%s: repayment FAILED %s' % (name, str(e)[:100]))
            continue

        # --- relief: margin has run up to the trigger ---
        if pct >= TRIGGER_PCT and out < MAX_OUTSTANDING and since >= COOLDOWN_SECONDS:
            needed = round(used / (TARGET_PCT / 100.0) - wallet, 2)
            amount = max(MIN_AMOUNT, min(MAX_AMOUNT, needed))
            amount = round(min(amount, MAX_OUTSTANDING - out), 2)
            if amount < 1:
                continue
            if mbal < amount:
                log('%s: margin %.1f%% >= %.0f%% but master holds only %.2f USDT (need %.2f) - cannot fund' % (name, pct, TRIGGER_PCT, mbal, amount))
                st['last_action_ts'] = now
                continue
            log('%s: margin %.1f%% >= %.0f%% -> borrowing %.2f USDT from master (wallet %.2f, needed %.2f for %.0f%%)' % (name, pct, TRIGGER_PCT, amount, wallet, needed, TARGET_PCT))
            if not DRY_RUN:
                try:
                    res = mf.transfer('master', name, amount, execute=True)
                    if (res or {}).get('status') != 'SUCCESS':
                        log('%s: relief not confirmed: %s' % (name, json.dumps(res)[:120]))
                        continue
                    st['outstanding'] = round(out + amount, 2)
                    st['last_action_ts'] = now
                    st['events'] = (st.get('events') or [])[-9:] + [{'t': now, 'kind': 'borrow', 'amount': amount, 'pct': round(pct, 1)}]
                    changed = True
                    mbal = master_balance()
                except Exception as e:
                    log('%s: relief FAILED %s' % (name, str(e)[:100]))
    if changed:
        save_state(state)


if __name__ == '__main__':
    main()
