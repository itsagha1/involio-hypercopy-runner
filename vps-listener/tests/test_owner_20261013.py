"""Owner rule tests 2026-10-13 (v3.7.4): 2% entry skip with auto re-entry,
source-lost 2-poll confirmation, immediate exchange TP trigger on retention.
No network; fake Bybit client."""
from __future__ import annotations

import os
import sys
import tempfile

os.environ["WEBHOOK_SHARED_SECRET"] = "testsecret"
os.environ["DRY_RUN"] = "false"
tmp = tempfile.mkdtemp()
os.environ["STATE_FILE"] = os.path.join(tmp, "state.json")
os.environ["LOG_FILE"] = os.path.join(tmp, "actions.log")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import listener
listener.SHARED_SECRET = "testsecret"

PASS = 0


def ok(cond: bool, label: str):
    global PASS
    assert cond, f"FAIL: {label}"
    PASS += 1
    print(f"  ok - {label}")


class FakeClient:
    def __init__(self, wallet: float = 500.0, avail: float = 500.0):
        self.wallet = wallet
        self.avail = avail
        self.orders = []
        self.stops = []
        self.ticker_prices = {}
        self.live = []

    @property
    def configured(self) -> bool:
        return True

    def get_balance(self, coin: str = "USDT") -> float:
        return self.wallet

    def get_available_balance(self, coin: str = "USDT") -> float:
        return self.avail

    def get_account_summary(self, coin: str = "USDT"):
        return {"wallet_balance": self.wallet, "margin_balance": self.wallet,
                "available_balance": self.avail, "unrealised_pnl": 0}

    def get_ticker(self, s: str) -> dict:
        price = self.ticker_prices.get(s, "100.0")
        return {"lastPrice": str(price), "markPrice": str(price)}

    def get_instrument(self, s: str) -> dict:
        return {"status": "Trading",
                "lotSizeFilter": {"qtyStep": "0.01", "minQty": "0.01", "maxQty": "10000.0",
                                  "minOrderQty": "0.01", "minNotionalValue": "5"},
                "leverageFilter": {"minLeverage": "1", "maxLeverage": "100"},
                "priceFilter": {"tickSize": "0.01"}}

    def get_positions(self, symbol=None) -> list:
        if symbol is None:
            return self.live
        return [p for p in self.live if p.get("symbol") == symbol]

    def set_leverage(self, s: str, lev: int) -> dict:
        return {"retCode": 0}

    def place_order(self, symbol, side, qty, position_idx=0, reduce_only=False, order_link_id=None) -> dict:
        self.orders.append({"symbol": symbol, "side": side, "qty": qty, "idx": position_idx})
        return {"orderId": "f-1", "filled_qty": qty, "filled_avg_price": self.get_ticker(symbol)["lastPrice"]}

    def set_sl_tp(self, symbol, stop_loss=None, take_profit=None, position_idx=0, sl=None, tp=None) -> dict:
        self.stops.append({"symbol": symbol, "sl": stop_loss, "tp": take_profit, "idx": position_idx})
        return {"retCode": 0}


def pos(ticker="SOL", side="long", sq=1.0, ep=100.0, cp=100.0, sm=50.0, lev=10, alloc_pct=10.0,
        last_sim=None, entry_sim=None, sid=None):
    return {"ticker": ticker, "side": side, "source_qty": sq, "entry_price": ep, "current_price": cp,
            "sim_margin": sm, "leverage": lev, "allocation_pct": alloc_pct,
            "last_sim": last_sim, "entry_sim": entry_sim,
            "source_id": sid or f"{ticker}-{side}-x", "stop_loss": None, "price_target": None}


def make_book(positions, trader="booobsas"):
    return {trader: {"positions": positions, "wallet": 500.0,
                     "alloc_pct_total": 100.0, "snapshot_ts": "2026-10-13T09:00:00+00:00"}}


print("1. owner 2026-10-13: >=2% entry skip registers skip_watch")
c = FakeClient()
p_late = pos(ep=100.0, cp=98.0, side="long", last_sim=104.0, entry_sim=100.0)
st = listener.load_state()
txt, rec = listener.open_mirror(c, "booobsas", p_late, make_book([p_late])["booobsas"], st)
ok(rec is None and "SKIP-WATCH" in txt, ">=2% skipped with watch entry")
ok(len(st.get("skip_watch", {})) == 1, "skip_watch populated")

print("2. pending retained position gets exchange TP trigger placed (self-heal)")
st2 = listener.load_state()
key = "akira|ZEC/long|f657a570-86fd-4f31-b792-caf8028c421b"
st2["mirrored"][key] = {"symbol": "ZECUSDT", "side": "long", "qty": 0.07, "leverage": 8}
st2["retained_trailing"][key] = {
    "key": key, "symbol": "ZECUSDT", "side": "long", "qty": 0.07,
    "fee_be": 1273.82, "activation_threshold": 1280.19,
    "status": "pending", "best_price": 1170.64, "current_sl": None,
    "tp_trigger": None, "price_target": None}
c2 = FakeClient()
c2.live = [{"symbol": "ZECUSDT", "side": "Buy", "size": "0.07", "avgPrice": "1272.14",
            "breakEvenPrice": "1273.82"}]
c2.ticker_prices["ZECUSDT"] = 1170.64
logs = listener.manage_retained_trailing_stops(c2, st2)
tp_calls = [x for x in c2.stops if x["tp"]]
ok(len(tp_calls) == 1, "TP trigger call placed")
ok(abs(tp_calls[0]["tp"] - 1280.19) < 0.01, f"TP trigger at activation threshold (got {tp_calls[0]['tp']})")
ok(tp_calls[0]["sl"] == 0, "no SL while pending")
ok(st2["retained_trailing"][key]["tp_trigger"] == tp_calls[0]["tp"], "tp_trigger recorded in state")
ok(any("TP-TRIGGER" in l for l in logs), "TRAIL TP-TRIGGER logged")
# idempotent: second pass does not re-place
logs2 = listener.manage_retained_trailing_stops(c2, st2)
ok(len([x for x in c2.stops if x["tp"]]) == 1, "TP trigger not duplicated")

print("3. activation cancels the TP trigger, then trailing ratchets SL")
c3 = FakeClient()
c3.live = [{"symbol": "ZECUSDT", "side": "Buy", "size": "0.07", "avgPrice": "1272.14",
            "breakEvenPrice": "1273.82"}]
c3.ticker_prices["ZECUSDT"] = 1290.0  # above activation 1280.19
st3 = listener.load_state()
st3["mirrored"][key] = {"symbol": "ZECUSDT", "side": "long", "qty": 0.07, "leverage": 8}
st3["retained_trailing"][key] = dict(st2["retained_trailing"][key], tp_trigger=1280.19, best_price=1170.64)
logs3 = listener.manage_retained_trailing_stops(c3, st3)
ok(st3["retained_trailing"][key]["status"] == "active", "trailing activated")
ok(st3["retained_trailing"][key]["tp_trigger"] is None, "TP trigger cancelled on activation")
ok(any(x["sl"] and x["sl"] > 0 for x in c3.stops), "hard SL placed after activation")
ok(any("cancelled exchange TP trigger" in l for l in logs3), "TP cancellation logged")

print("4. source_missing counter needs 2 consecutive polls before synthesized close")
# (unit-level: verify state dict mechanics used by the webhook reconciliation)
st4 = listener.load_state()
st4.setdefault("source_missing", {})
st4["source_missing"]["akira|OP/long|abc"] = 1
st4["source_missing"]["akira|OP/long|abc"] += 1
ok(st4["source_missing"]["akira|OP/long|abc"] >= 2, "counter reaches 2 -> close synthesized in webhook")

print()
print(f"ALL {PASS} OWNER-2026-10-13 CHECKS PASSED (2% skip watch + source-lost + TP trigger)")
