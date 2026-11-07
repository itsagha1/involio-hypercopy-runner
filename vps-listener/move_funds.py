#!/usr/bin/env python3
"""Internal fund movement between the owner's own Bybit accounts (owner 2026-11-07).

Safety rules baked in:
  * universal-transfer endpoint ONLY; withdrawal endpoints are never referenced
  * destination must exist in UIDS (the owner's own subaccounts)
  * signed with the MASTER account key (sub keys cannot move funds between accounts)
  * dry-run unless --execute is passed
Usage: move_funds.py --from main --to kings --amount 5 [--execute]
"""
import argparse, json, os, sys, time, uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Confirmed from the Bybit subaccount page 2026-11-07
UIDS = {
    'main': '587820763',
    'mainport': '593122696',
    'bakery': '593122782',
    'scalping': '593122854',
    'kings': '593123036',
    'bluespire': '593304088',
    'hypercopy': '600742547',
    'master': '585090581',
}

ENVS = {
    'main': '/home/ubuntu/involio-hypercopy-runner/vps-listener/.env',
    'mainport': '/etc/copy-mainport.env',
    'bakery': '/etc/copy-bakery.env',
    'scalping': '/etc/copy-scalping.env',
    'kings': '/etc/copy-kings.env',
    'bluespire': '/etc/copy-bluespire.env',
    'hypercopy': '/etc/copy-hypercopy.env',
    'master': '/etc/bybit-master.env',
}

# Wallet type per account. The master funding account keeps its lending pool in
# the FUND (funding) wallet, not the unified trading wallet - verified live 2026-11-07
# (master UNIFIED = 0.00 while FUND = 942.05). Subaccounts trade in UNIFIED.
ACCOUNT_TYPES = {'master': 'FUND'}

ENV_KEYS = ('BYBIT_API_KEY', 'BYBIT_API_SECRET', 'BYBIT_RSA_PRIVATE_KEY_FILE', 'BYBIT_PROXY', 'BYBIT_PROXY_FALLBACKS')


def load_env(path):
    env = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()
    return env


def client_for(account):
    for key in ENV_KEYS:
        os.environ.pop(key, None)
    env = load_env(ENVS[account])
    for key in ENV_KEYS:
        if env.get(key):
            os.environ[key] = env[key]
    from bybit_api import BybitClient
    return BybitClient()


def balance(account):
    c = client_for(account)
    if account == 'master':
        d = c._get('/v5/asset/transfer/query-account-coins-balance',
                   {'accountType': ACCOUNT_TYPES['master'], 'coin': 'USDT'})
        r = d.get('result') or d
        for b in (r.get('balance') or []):
            if b.get('coin') == 'USDT':
                return float(b.get('walletBalance') or 0)
        return 0.0
    return float(c.get_account_summary().get('wallet_balance') or 0)


def transfer(src, dst, amount, execute=False):
    if src not in UIDS or dst not in UIDS:
        raise SystemExit('unknown account; known: ' + ','.join(sorted(UIDS)))
    if src == dst:
        raise SystemExit('source and destination are the same account')
    if not execute:
        print('[DRY-RUN] %.2f USDT  %s (%s) -> %s (%s)' % (amount, src, UIDS[src], dst, UIDS[dst]))
        return None
    c = client_for('master')
    body = {
        'transferId': str(uuid.uuid4()),
        'coin': 'USDT',
        'amount': str(amount),
        'fromMemberId': int(UIDS[src]),
        'toMemberId': int(UIDS[dst]),
        'fromAccountType': ACCOUNT_TYPES.get(src, 'UNIFIED'),
        'toAccountType': ACCOUNT_TYPES.get(dst, 'UNIFIED'),
    }
    return c._post('/v5/asset/transfer/universal-transfer', body)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--from', dest='src', required=True)
    ap.add_argument('--to', dest='dst', required=True)
    ap.add_argument('--amount', type=float, required=True)
    ap.add_argument('--execute', action='store_true')
    a = ap.parse_args()
    print('before: %s=%.2f  %s=%.2f' % (a.src, balance(a.src), a.dst, balance(a.dst)))
    res = transfer(a.src, a.dst, a.amount, a.execute)
    if res is not None:
        print('result:', json.dumps(res)[:200])
        time.sleep(4)
        print('after : %s=%.2f  %s=%.2f' % (a.src, balance(a.src), a.dst, balance(a.dst)))
