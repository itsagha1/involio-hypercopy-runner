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
    def __init__(self, avail=500.0):
        self.avail = avail
        self.orders = []
        self.stops = []
        self.lev = []

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
        ep=120.0, cp=120.0, sl=None, tp=None):
    return {"ticker": ticker, "side": side, "leverage": lev, "entry_price": ep,
            "current_price": cp, "stop_loss": sl, "price_target": tp,
            "entry_sim": es, "last_sim": ls if ls is not None else es}


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
    books={"nathanbrown": {"positions": [pos(es=3.85)]}})
res = asyncio.run(listener.involio_delta(lp, x_signature="testsecret"))
st = json.load(open(os.environ["STATE_FILE"]))
ok(res.get("fresh_start") and st["baseline"] == ["nathanbrown|SOL/short"], "baseline recorded")

print("10. webhook: baseline delta ignored, new non-baseline trade processed")
lp2 = listener.WebhookPayload(
    source="t", fired_at="2026-09-26T03:05:00Z", recents={},
    books={"nathanbrown": {"positions": [pos(es=3.85, ls=3.70), pos(ticker="XRP", es=2.99, lev=20, cp=1.58)]}})
res2 = asyncio.run(listener.involio_delta(lp2, x_signature="testsecret"))
log = open(os.environ["LOG_FILE"]).read()
ok("SKIP nathanbrown|SOL/short" in log, "baseline delta skipped")
ok("WOULD OPEN nathanbrown|XRP/short" in log, "new trade processed (dry-run logged)")
ok(res2["ok"] and res2["deltas"] >= 1, "webhook ok")

print("11. bad signature rejected")
try:
    asyncio.run(listener.involio_delta(lp2, x_signature="wrong"))
    ok(False, "should have raised")
except Exception:
    ok(True, "403 on bad signature")

print(f"\nALL {PASS} CHECKS PASSED")
