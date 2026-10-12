"""Offline regressions for the 100% copy listener (copy_listener.py). No exchange requests."""
from __future__ import annotations
import os, sys, tempfile, json, copy
from datetime import datetime, timezone

os.environ['DRY_RUN'] = 'false'
os.environ['WEBHOOK_SHARED_SECRET'] = 'testsecret'
os.environ['COPY_PROFILE'] = 'testprof'
os.environ['COPY_PORTFOLIO_ID'] = '11111111-1111-1111-1111-111111111111'
os.environ['COPY_NAME'] = 'testcopy'
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

state_dir = tempfile.mkdtemp()
os.environ['COPY_STATE_FILE'] = os.path.join(state_dir, 'state.json')
os.environ['COPY_LOG_FILE'] = os.path.join(state_dir, 'log.txt')

import copy_listener, bybit_api
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_rules import FakeClient, pos, make_book

PASS = 0
def ok(cond, label):
    global PASS
    assert cond, label
    PASS += 1
    print('  ok - ' + label)

def fresh_state():
    st = {"baseline": [], "mirrored": {}, "skip_watch": {}, "manual": [], "entry_blocks": {},
          "books": None, "last_poll": None, "errors_last_run": 0, "armed": False}
    copy_listener._GLOBAL_STATE["state"] = st
    return st

def cpos(ticker, side, sid, roi=0.0, lev=10, pct=2.0, sl=None, tp=None, qty=None):
    """Source position dict as normalize_source produces."""
    entry_sim = 100.0
    p = pos(ticker=ticker, side=side, sid=sid, ep=100, cp=100, lev=lev, alloc_pct=pct)
    p['entry_sim'] = entry_sim
    p['last_sim'] = entry_sim * (1 + roi)
    p['source_qty'] = qty if qty is not None else entry_sim * lev / 100.0
    p['stop_loss'] = sl
    p['price_target'] = tp
    return p

# --- 1. first poll baselines everything, no orders ---
st = fresh_state()
st['books'] = {'positions': [cpos('BTC', 'long', 'b1'), cpos('ETH', 'short', 'e1')]}
c = FakeClient(wallet=1000, avail=1000)
p_new = cpos('SOL', 'long', 's1', roi=0.01)
st['books'] = {'positions': [cpos('BTC', 'long', 'b1'), cpos('ETH', 'short', 'e1'), p_new]}
a, rec = copy_listener.open_mirror(c, p_new, st)
ok(rec is not None and a.startswith('OPENED'), 'new trade at +1% ROI opens (below 2% skip)')
ok('s1' not in st['skip_watch'], 'opened trade not placed on watch list')
ok(len(c.orders) == 1, 'one order placed')
ok(abs(rec['margin'] - 20.0) < 1e-9, '1:1 percentage sizing: 2% allocation x 1000 wallet = 20 USDT margin')
ok(rec['leverage'] == 10, 'leverage matched')

# --- 2. 2% skip and watch registration ---
st = fresh_state()
c = FakeClient(wallet=1000, avail=1000)
p_hot = cpos('SOL', 'long', 's2', roi=0.025)
a, rec = copy_listener.open_mirror(c, p_hot, st)
ok(rec is None and a.startswith('SKIP-WATCH'), 'trade at +2.5% ROI skipped and watched')
ok('testprof|SOL/long|s2' in st['skip_watch'], 'watch entry recorded for auto re-entry')
ok(len(c.orders) == 0, 'no order for skipped trade')

# --- 3. auto re-entry when ROI falls below 2% (owner spec 2026-10-12) ---
p_cooled = copy.deepcopy(p_hot)
p_cooled['last_sim'] = p_cooled['entry_sim'] * 1.015  # +1.5%
a2, rec2 = copy_listener.open_mirror(c, p_cooled, st)
ok(rec2 is not None and a2.startswith('OPENED'), 'watched trade at +1.5% opens automatically')
ok('testprof|SOL/long|s2' not in st['skip_watch'], 'watch entry cleared after open')

# --- 4. boundary: exactly 2.0% stays watched ---
st = fresh_state()
c = FakeClient(wallet=1000, avail=1000)
p_edge = cpos('SOL', 'long', 's3', roi=0.02)
a3, rec3 = copy_listener.open_mirror(c, p_edge, st)
ok(rec3 is None and a3.startswith('SKIP-WATCH'), 'trade at exactly +2.00% stays skipped/watched')

# --- 5. 100% follow: source close in loss is followed at market ---
st = fresh_state()
c = FakeClient(wallet=1000, avail=1000)
p_open = cpos('BTC', 'short', 'b2', roi=0.01)
a4, rec4 = copy_listener.open_mirror(c, p_open, st)
ok(rec4 is not None, 'setup: mirror opened for close-follow test')
p_loss = copy.deepcopy(p_open)
p_loss['last_sim'] = p_loss['entry_sim'] * 0.97  # source closed at -3%
st['mirrored'][copy_listener.tkey(p_loss)]['qty'] = 0.5
c.live_positions = [{'symbol': 'BTCUSDT', 'size': '0.5', 'side': 'Sell', 'avgPrice': '100'}]
msg = copy_listener.follow_source_close(c, p_loss, st['mirrored'][copy_listener.tkey(p_loss)])
ok(msg.startswith('CLOSED'), 'source close in LOSS followed at market (100% copy, no retention)')
ok('loss followed' in msg, 'close log documents loss-following')
last = c.orders[-1]
ok(last['reduce'] is True and last['side'] == 'Buy', 'close order reduce-only opposite side')

# --- 6. sizing guards: min notional, margin cap, leverage cap ---
st = fresh_state()
c = FakeClient(wallet=30, avail=30)
p_small = cpos('DOGE', 'long', 'd1', roi=0.0, pct=0.1, lev=3)
a6, _ = copy_listener.open_mirror(c, p_small, st)
ok(a6.startswith('SKIP') and 'minimum' in a6, 'notional below 5 USDT minimum is skipped')
st = fresh_state()
c = FakeClient(wallet=1000, avail=1000)
p_big = cpos('BTC', 'long', 'b3', roi=0.0, pct=80.0, lev=60)
a7, _ = copy_listener.open_mirror(c, p_big, st)
ok(a7.startswith('SKIP') and ('margin cap' in a7 or 'leverage' in a7), 'oversized allocation/leverage refused')
st = fresh_state()
c = FakeClient(wallet=1000, avail=1000)
p_lev = cpos('BTC', 'long', 'b4', roi=0.0, pct=1.0, lev=60)
a8, rec8 = copy_listener.open_mirror(c, p_lev, st)
ok(rec8 is not None and rec8['leverage'] == 50, 'leverage capped at 50x')

# --- 7. baseline positions never mirror; blocks dedup ---
st = fresh_state()
st['baseline'] = ['b1']
c = FakeClient(wallet=1000, avail=1000)
p_base = cpos('BTC', 'long', 'b1', roi=0.05)
a9, _ = copy_listener.open_mirror(c, p_base, st)
ok(a9.startswith('SKIP'), 'baseline position with +5% ROI not mirrored even under 100% copy rules')
ok(len(c.orders) == 0, 'no order for baseline position')

# --- 8. SL/TP sync follows source ---
st = fresh_state()
c = FakeClient(wallet=1000, avail=1000)
p_sl = cpos('ETH', 'long', 'e2', roi=0.01, sl=95.0)
a10, rec10 = copy_listener.open_mirror(c, p_sl, st)
ok(rec10 is not None, 'setup: mirror opened with source SL')
p_sl2 = copy.deepcopy(p_sl)
p_sl2['stop_loss'] = 97.0
p_sl2['price_target'] = 130.0
c.ticker_prices['ETHUSDT'] = 100
msg = copy_listener.sync_sltp(c, p_sl2, rec10)
ok(msg is not None and msg.startswith('SYNC'), 'SL/TP change synced from source')
ok(rec10['stop_loss'] == 97.0 and rec10['price_target'] == 130.0, 'recorded SL/TP match source')

# --- 9. size add/reduce mirrored proportionally ---
st = fresh_state()
c = FakeClient(wallet=1000, avail=1000)
p_sz = cpos('SOL', 'long', 's9', roi=0.01, qty=50.0)
a11, rec11 = copy_listener.open_mirror(c, p_sz, st)
ok(rec11 is not None, 'setup: mirror opened for size-sync test')
orders_before = len(c.orders)
qty_before = rec11['qty']
p_add = copy.deepcopy(p_sz)
p_add['source_qty'] = 75.0  # +50%
msg = copy_listener.sync_size(c, p_add, rec11)
ok(msg is not None and msg.startswith('ADDED'), 'source size add mirrored proportionally')
ok(abs(rec11['qty'] - qty_before * 1.5) < qty_before * 0.02, 'our qty scaled by source ratio (+50%)')
qty_after_add = rec11['qty']
p_cut = copy.deepcopy(p_add)
p_cut['source_qty'] = 37.5  # -50% of the bigger size
msg = copy_listener.sync_size(c, p_cut, rec11)
ok(msg is not None and msg.startswith('REDUCED'), 'source size reduction mirrored proportionally')
ok(abs(rec11['qty'] - qty_after_add * 0.5) < qty_after_add * 0.02, 'our qty halved on -50% source cut')

# --- 10. manual-protect: existing live position blocks entry ---
st = fresh_state()
c = FakeClient(wallet=1000, avail=1000)
c.live_positions = [{'symbol': 'XRPUSDT', 'size': '1000', 'side': 'Buy', 'avgPrice': '2'}]
p_x = cpos('XRP', 'long', 'x1', roi=0.0)
a12, _ = copy_listener.open_mirror(c, p_x, st)
ok(a12.startswith('SKIP') and 'conflict' in a12, 'live owner position conflict blocks new mirror')
ok('XRP/long' in st['manual'], 'conflicting coin/side recorded as manual-protect')

# --- 11. delisted symbol skips cleanly ---
st = fresh_state()
class _NoSymClient(FakeClient):
    def get_positions(self, sym=None):
        raise bybit_api.BybitError('retCode=10001 symbol not exist')
c = _NoSymClient(wallet=1000, avail=1000)
p_dead = cpos('FETCLOSED', 'long', 'f1', roi=0.0)
a13, _ = copy_listener.open_mirror(c, p_dead, st)
ok(a13.startswith('SKIP') and 'delisted' in a13, 'delisted symbol SKIP, not ERROR')

print('ALL ' + str(PASS) + ' COPY-ACCOUNT CHECKS PASSED (c1.0.0)')

# --- 12. full poll-cycle integration: baseline -> open -> watch -> auto re-entry -> loss close ---
fake_client = FakeClient(wallet=1000, avail=1000)
fake_client.ticker_prices = {'BTCUSDT': '100', 'SOLUSDT': '100', 'DOGEUSDT': '100'}
copy_listener.BybitClient = lambda: fake_client
st = fresh_state()

book_v1 = {'positions': [cpos('BTC', 'long', 'b1', roi=0.0)], 'source_equity': 5000, 'remaining_sim': 4800}
copy_listener.source_api.fetch_source_book = lambda profile: copy.deepcopy(book_v1)
copy_listener.poll_once()
ok(st['baseline'] == ['b1'] and st['armed'] is True, 'poll 1: existing positions baselined, instance armed')
ok(len(fake_client.orders) == 0, 'poll 1: no orders on baseline run')

book_v2 = {'positions': [cpos('BTC', 'long', 'b1', roi=0.01), cpos('SOL', 'long', 's1', roi=0.01)]}
copy_listener.source_api.fetch_source_book = lambda profile: copy.deepcopy(book_v2)
copy_listener.poll_once()
ok(len(fake_client.orders) == 1 and 's1' in list(st['mirrored'])[0], 'poll 2: new source trade mirrored at market')

book_v3 = {'positions': [cpos('BTC', 'long', 'b1', roi=0.01), cpos('SOL', 'long', 's1', roi=0.03)]}
copy_listener.source_api.fetch_source_book = lambda profile: copy.deepcopy(book_v3)
copy_listener.poll_once()
ok(len(fake_client.orders) == 1, 'poll 3: pure source ROI change (PnL-only) generates no order')

book_v4 = {'positions': [cpos('BTC', 'long', 'b1', roi=0.01),
                         cpos('SOL', 'long', 's1', roi=0.03),
                         cpos('DOGE', 'long', 'd9', roi=0.04)]}
copy_listener.source_api.fetch_source_book = lambda profile: copy.deepcopy(book_v4)
copy_listener.poll_once()
ok(len(fake_client.orders) == 1 and 'DOGE' in list(st['skip_watch'])[0], 'poll 4: +4% new trade skipped into watch')

book_v5 = {'positions': [cpos('BTC', 'long', 'b1', roi=0.01),
                         cpos('SOL', 'long', 's1', roi=0.03),
                         cpos('DOGE', 'long', 'd9', roi=0.012)]}
copy_listener.source_api.fetch_source_book = lambda profile: copy.deepcopy(book_v5)
copy_listener.poll_once()
ok(len(fake_client.orders) == 2 and any('d9' in k for k in st['mirrored']), 'poll 5: watched trade auto-enters when ROI < 2%')
ok(len(st['skip_watch']) == 0, 'poll 5: watch list cleared after auto-entry')

book_v6 = {'positions': [cpos('BTC', 'long', 'b1', roi=0.01)]}
copy_listener.source_api.fetch_source_book = lambda profile: copy.deepcopy(book_v6)
fake_client.live_positions = [{'symbol': 'SOLUSDT', 'size': str(list(st['mirrored'].values())[0]['qty']), 'side': 'Buy', 'avgPrice': '100'},
                               {'symbol': 'DOGEUSDT', 'size': str(list(st['mirrored'].values())[1]['qty']), 'side': 'Buy', 'avgPrice': '100'}]
copy_listener.poll_once()
ok(len(st['mirrored']) == 0, 'poll 6: all mirrored trades closed at market when sources close (100% follow)')
ok(st['baseline'] == ['b1'], 'poll 6: baseline survives, only mirrors cleaned')

print('ALL ' + str(PASS) + ' COPY-ACCOUNT CHECKS PASSED (poll-cycle integration)')
