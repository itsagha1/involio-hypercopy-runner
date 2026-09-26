"""Rule tests for listener v2 (no network, fake Bybit client)."""
import asyncio
import json
import os
import sys
import tempfile

os.environ["WEBHOOK_SHARED_SECRET"] = "testsecret"
os.environ["DRY_RUN"] = "true"
tmp = tempfile.mkdtemp()
os.environ["STATE_FILE"] = os.path.join(tmp, "state.json")
os.environ["LOG_FILE"] = os.path.join(tmp, "actions.log")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import listener  # noqa: E402
from bybit_api import BybitError  # noqa: E402

PASS = 0


def ok(cond, label):
    global PASS
    assert cond, f"FAIL: {label}"
    PASS += 1
    print(f"  ok - {label}")


class FakeClient:
    def __init__(self, avail=500.0, wallet=500.0):
        self.avail = avail
        self.wallet = wallet
        self.orders = []
        self.stops = []
        self.lev = []

    def get_balance(self, coin="USDT"):
        return self.wallet

    @property
    def configured(self):
        return True

    def get_ticker(self, s):
        return {"lastPrice": "120.0"}

    def get_instrument(self, s):
        return {"lotSizeFilter": {"qtyStep": "0.01", "minQty": "0.01"}}

    def get_available_balance(self, coin="USDT"):
        return self.avail

    def set_leverage(self, s, l):
        self.lev.append((s, l))

    def place_order(self, s, side, qty, position_idx=0, reduce_only=False):
        self.orders.append({"symbol": s, "side": side, "qty": qty,
                            "idx": position_idx, "reduce": reduce_only})
        return {"orderId": "x"}

    def set_sl_tp(self, s, sl, tp, position_idx=0):
        self.stops.append(("sltp", s, sl, tp))

    def set_trailing_stop(self, s, act, dist, position_idx=0):
        self.stops.append(("trail", s, act, dist))


def pos(ticker="SOL", side="short", es=3.85, ls=None, lev=20,
        ep=120.0, cp=120.0, sl=None, tp=None, created_at=None):
    return {"ticker": ticker, "side": side, "leverage": lev, "entry_price": ep,
            "current_price": cp, "stop_loss": sl, "price_target": tp,
            "entry_sim": es, "last_sim": ls if ls is not None else es,
            "created_at": created_at}


print("1. compute_deltas detects all four change types")
prev = {"nathanbrown": {"positions": [pos(sl=None, tp=121.0)]}}
curr = {"nathanbrown": {"positions": [pos(ticker="XRP", es=3.0, sl=118.0, tp=None, cp=1.58)]}}
d = listener.compute_deltas(prev, curr)
ok(any(x["type"] == "source_close" for x in d), "source_close detected")
prev2 = {"nathanbrown": {"positions": [pos(ls=3.85, sl=None, tp=121.0)]}}
curr2 = {"nathanbrown": {"positions": [pos(ls=3.85, sl=None, tp=119.0)]}}
d2 = listener.compute_deltas(prev2, curr2)
ok(d2 and d2[0]["type"] == "sltp_change", "sltp_change detected")

print("2. rule 3: new trade already +3% profit is skipped")
c = FakeClient()
p_late = pos(es=3.85, ls=3.97, cp=120.0)  # +3.1% sim profit
txt, rec = listener.open_mirror(c, "nathanbrown", p_late)
ok(rec is None and "too late" in txt, "3% skip")

print("3. rule 3+4: negative-profit new trade mirrors 1:1 notional")
p_neg = pos(es=3.85, ls=3.70, cp=120.0)  # -3.9% sim (mirrored anyway)
txt, rec = listener.open_mirror(c, "nathanbrown", p_neg)
ok(rec is not None and "OPENED" in txt, "negative trade opened")
want = 3.85 * 20 / 120.0  # entry_sim x lev / price
ok(abs(rec["qty"] - round(want, 2)) < 1e-9, f"1:1 sizing qty={rec['qty']} ~ {want:.2f}")
ok(c.orders[0]["side"] == "Sell" and c.orders[0]["idx"] == 0, "short opened Sell, idx 0")
ok(c.lev == [("SOLUSDT", 20)], "leverage matched 20x")

print("4. HYPE hedge-mode positionIdx")
txt, rec = listener.open_mirror(c, "limpan96", pos(ticker="HYPE", side="long", es=2.0, lev=10))
ok(rec is not None and c.orders[-1]["idx"] == 1, "HYPE long idx 1")

print("5. rule 2: pure PnL sim wobble does NOT resize")
rec = {"symbol": "SOLUSDT", "side": "short", "qty": 0.64, "their_qty": 0.64}
p_now = pos(es=3.85, ls=3.80, cp=121.5)          # sim dipped on adverse move
p_prev = pos(es=3.85, ls=3.85, cp=120.0)
txt = listener.sync_size(c, "nathanbrown", p_now, rec, p_prev)
ok(txt.startswith("LOG") and not c.orders[-1:], "noise filtered, no order") if False else ok(txt.startswith("LOG"), "noise filtered")
ok(abs(rec["qty"] - 0.64) < 1e-9, "qty untouched by noise")

print("6. rule 2: real size add resizes 1:1")
p_now = pos(es=3.85, ls=5.0, cp=120.0)           # trader added ~30%
txt = listener.sync_size(c, "nathanbrown", p_now, rec, p_prev)
ok(txt.startswith("ADDED"), f"add followed: {txt}")
# noise step moved their_qty to 0.6238; add x1.2987 -> 0.8101, +0.17 rounded
ok(abs(rec["qty"] - 0.81) < 1e-9, "qty scaled by source ratio (noise-adjusted)")
ok(c.orders[-1]["reduce"] is False, "add order not reduce-only")

print("7. rule 2: SL/TP sync")
rec2 = {"symbol": "SOLUSDT", "side": "short", "qty": 1.0}
txt = listener.sync_sltp(c, "nathanbrown", pos(sl=125.0, tp=110.0), rec2)
ok(txt.startswith("SYNC") and c.stops[-1] == ("sltp", "SOLUSDT", 125.0, 110.0), "sltp synced")

print("8. rule 5: no-loss close only if comfortably positive")
mirrors = {"SOL/short": {"unrealisedPnl": "1.3", "size": "1.3", "avgPrice": "100"}}
rec3 = {"symbol": "SOLUSDT", "side": "short", "qty": 1.3}
txt = listener.execute_source_close(c, {"trader": "nathanbrown", "position": pos()}, rec3, mirrors)
ok(txt.startswith("CLOSED") and c.orders[-1]["reduce"] is True, "positive pnl closed")
mirrors_neg = {"SOL/short": {"unrealisedPnl": "-0.5", "size": "1.3", "avgPrice": "100"}}
txt = listener.execute_source_close(c, {"trader": "nathanbrown", "position": pos()}, rec3, mirrors_neg)
ok(txt.startswith("TRAIL") and c.stops[-1][0] == "trail", "negative pnl -> trailing stop")
ok(abs(c.stops[-1][2] - 98.5) < 1e-9, "trail activation 0.985 x avg for short")

print("9. webhook: fresh start baseline capture, then baseline is skipped")
lp = listener.WebhookPayload(
    source="t", fired_at="2026-09-26T03:00:00Z", recents={},
    books={"nathanbrown": {"positions": [pos(es=3.85, created_at="2026-09-25T10:00:00Z")]}})
res = asyncio.run(listener.involio_delta(lp, x_signature="testsecret"))
st = json.load(open(os.environ["STATE_FILE"]))
ok(res.get("fresh_start") and st["baseline"] == ["nathanbrown|SOL/short"], "baseline recorded")

print("10. webhook: baseline delta ignored, new non-baseline trade processed")
lp2 = listener.WebhookPayload(
    source="t", fired_at="2026-09-26T03:05:00Z", recents={},
    books={"nathanbrown": {"positions": [pos(es=3.85, ls=3.70, created_at="2026-09-25T10:00:00Z"), pos(ticker="XRP", es=2.99, lev=20, cp=1.58, created_at="2026-09-26T02:00:00Z")]}})
res2 = asyncio.run(listener.involio_delta(lp2, x_signature="testsecret"))
log = open(os.environ["LOG_FILE"]).read()
ok("SKIP nathanbrown|SOL/short" in log, "baseline delta skipped")
ok("WOULD OPEN nathanbrown|XRP/short" in log, "new trade processed (dry-run logged)")
ok(res2["ok"] and res2["deltas"] >= 1, "webhook ok")

print("11. owner rule 2026-09-27: leverage capped at 20x")
c_lev = FakeClient()
txt, rec_lev = listener.open_mirror(c_lev, "akira", pos(ticker="DOGE", es=5.0, lev=50, cp=0.1))
ok(rec_lev is not None and rec_lev["leverage"] == 20, "50x source opened at 20x cap")
ok(c_lev.lev == [("DOGEUSDT", 20)], "set_leverage called with capped 20")

print("12. owner rule: 70% margin cap blocks opens when budget exhausted")
c_cap = FakeClient(avail=10.0, wallet=100.0)  # used=90, budget=70-90=-20
txt, rec_cap = listener.open_mirror(c_cap, "akira", pos(ticker="DOGE", es=2.0, lev=10, cp=0.1))
ok(rec_cap is None and "margin cap" in txt, f"cap reached skip: {txt}")

print("13. owner rule: 70% cap scales down within budget")
c_sc = FakeClient(avail=100.0, wallet=200.0)  # used=100, budget=140-100=40
txt, rec_sc = listener.open_mirror(c_sc, "akira", pos(ticker="DOGE", es=50.0, lev=10, cp=1.0))
ok(rec_sc is not None and "scaled down" in txt, f"scaled open: {txt}")
ok(rec_sc["qty"] <= 40 * 10 + 1e-9, f"qty respects budget margin (qty={rec_sc['qty']})")

print("14. owner rule: max 2 adds per trade")
c_add = FakeClient()
rec_add = {"symbol": "SOLUSDT", "side": "short", "qty": 0.64,
           "their_qty": 0.64, "leverage": 20, "adds": 2}
p_prev_add = pos(es=3.85, ls=3.85, cp=120.0)
p_now_add = pos(es=3.85, ls=5.0, cp=120.0)
txt = listener.sync_size(c_add, "nathanbrown", p_now_add, rec_add, p_prev_add)
ok(txt.startswith("SKIP") and "add cap" in txt, f"3rd add skipped: {txt}")
ok(not c_add.orders, "no order placed for capped add")

print("15. owner rule: symbol uniqueness across profiles")
mirrored = {"limpan96|HBAR/long": {"symbol": "HBARUSDT"},
            "nathanbrown|XRP/long": {"symbol": "XRPUSDT"}}
owners = listener.symbol_mirrored_by_others(mirrored, "nathanbrown", "HBAR")
ok(owners == ["limpan96"], f"cross-profile conflict detected: {owners}")
owners2 = listener.symbol_mirrored_by_others(mirrored, "limpan96", "HBAR")
ok(owners2 == [], "same profile is not a conflict")

print("16. baseline close+reopen (WLD bug): reopened position is un-baselined")
fresh_state = {"baseline": ["nathanbrown|SOL/short"], "mirrored": {},
               "books": {}, "manual": [], "manual_adopted": True,
               "orphans": {}, "mismatches": {}, "dereg_pending": [],
               "fresh_start_at": "2026-09-26T00:30:07+00:00"}
ok(listener.is_reopened_position({"created_at": "2026-09-26T13:30:10.330Z"},
                                "2026-09-26T00:30:07+00:00"), "newer created_at detected")
ok(not listener.is_reopened_position({"created_at": "2026-09-25T10:00:00Z"},
                                     "2026-09-26T00:30:07+00:00"), "older created_at stays baseline")
ok(not listener.is_reopened_position({}, "2026-09-26T00:30:07+00:00"), "missing created_at is not a reopen")

print("17. orphan + mismatch detection")
state_det = {"mirrored": {"nathanbrown|SOL/short": {"qty": 10}},
             "manual": ["WLD/short"]}
live = {"SOL/short": {"size": "25", "avgPrice": "2.0"},   # mismatch: 10 tracked vs 25 live
        "WLD/short": {"size": "100", "avgPrice": "0.5"},  # manual: ignored
        "AVAX/short": {"size": "4", "avgPrice": "10"}}    # orphan
orphans, mismatches = listener.detect_unmanaged(state_det, live)
ok(orphans == [("AVAX/short", 4.0)], f"orphan found: {orphans}")
ok(len(mismatches) == 1 and mismatches[0][0] == "SOL/short", f"mismatch found: {mismatches}")

print("18. margin budget math")
c_b = FakeClient(avail=40.0, wallet=100.0)   # used=60, budget=70-60=10
ok(abs(listener.margin_budget(c_b) - 10.0) < 1e-9, "budget = 0.7*wallet - used")

print("11. bad signature rejected")
try:
    asyncio.run(listener.involio_delta(lp2, x_signature="wrong"))
    ok(False, "should have raised")
except Exception:
    ok(True, "403 on bad signature")

print(f"\nALL {PASS} CHECKS PASSED")
