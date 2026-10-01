"""Rule tests for cutover listener v3.3 (no network, fake Bybit client)."""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from datetime import datetime, timezone

os.environ["WEBHOOK_SHARED_SECRET"] = "testsecret"
os.environ["DRY_RUN"] = "false"
tmp = tempfile.mkdtemp()
os.environ["STATE_FILE"] = os.path.join(tmp, "state.json")
os.environ["LOG_FILE"] = os.path.join(tmp, "actions.log")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import listener
listener.SHARED_SECRET = "testsecret"
from bybit_api import BybitError

PASS = 0


def ok(cond: bool, label: str):
    global PASS
    assert cond, f"FAIL: {label}"
    PASS += 1
    print(f"  ok - {label}")


class FakeClient:
    def __init__(self, avail: float = 500.0, wallet: float = 500.0):
        self.avail = avail
        self.wallet = wallet
        self.orders = []
        self.stops = []
        self.lev = []
        self.ticker_prices = {}
        self.instruments = {}
        self.live_positions = []

    def get_account_summary(self, coin="USDT"):
        return {"wallet_balance":self.wallet,"margin_balance":self.wallet,"available_balance":self.avail,"unrealised_pnl":0}

    def get_balance(self, coin: str = "USDT") -> float:
        return self.wallet

    @property
    def configured(self) -> bool:
        return True

    def get_ticker(self, s: str) -> dict:
        price = self.ticker_prices.get(s, "120.0")
        return {"lastPrice": str(price), "markPrice": str(price)}

    def get_instrument(self, s: str) -> dict:
        if s in self.instruments:
            return self.instruments[s]
        return {
            "status": "Trading",
            "lotSizeFilter": {"qtyStep": "0.01", "minQty": "0.01", "maxQty": "10000.0"},
            "priceFilter": {"tickSize": "0.01"},
            "leverageFilter": {"minLeverage": "1", "maxLeverage": "100"}
        }

    def get_available_balance(self, coin: str = "USDT") -> float:
        return self.avail

    def get_positions(self, symbol: str | None = None) -> list:
        if symbol:
            return [p for p in self.live_positions if p.get("symbol") == symbol]
        return self.live_positions

    def set_leverage(self, s: str, l: int) -> dict:
        self.lev.append((s, l))
        return {}

    def place_order(self, s: str, side: str, qty: float, position_idx: int = 0, reduce_only: bool = False, order_link_id=None) -> dict:
        self.orders.append({"symbol": s, "side": side, "qty": qty,
                            "idx": position_idx, "reduce": reduce_only})
        return {"orderId": "x"}

    def set_sl_tp(self, symbol: str, stop_loss=None, take_profit=None, position_idx: int = 0, sl=None, tp=None) -> dict:
        actual_sl = stop_loss if stop_loss is not None else sl
        actual_tp = take_profit if take_profit is not None else tp
        sl_val = f"{actual_sl}" if actual_sl else "0"
        tp_val = f"{actual_tp}" if actual_tp else "0"
        self.stops.append(("sltp", symbol, sl_val, tp_val))
        return {}

    def set_trailing_stop(self, s: str, act: float, dist: float, position_idx: int = 0) -> dict:
        self.stops.append(("trail", s, act, dist))
        return {}


def pos(ticker: str = "SOL", side: str = "short", sm: float = 10.0, sq: float = 1.0, lev: int = 20,
        ep: float = 120.0, cp: float = 120.0, sl=None, tp=None, sid: str = "sol-short-1",
        last_sim=None, entry_sim=None, alloc_pct=1.0, alloc_ver=True) -> dict:
    d = {
        "source_id": sid,
        "ticker": ticker,
        "side": side,
        "leverage": lev,
        "entry_price": ep,
        "current_price": cp,
        "stop_loss": sl,
        "price_target": tp,
        "source_margin": sm,
        "source_qty": sq,
        "allocation_verified": alloc_ver,
    }
    if last_sim is not None:
        d["last_sim"] = last_sim
    if entry_sim is not None:
        d["entry_sim"] = entry_sim
    if alloc_pct is not None:
        d["source_allocation_pct"] = alloc_pct
    return d


def make_book(positions: list, trader: str = "booobsas") -> dict:
    return {
        trader: {
            "positions": positions,
            "source_equity": 100.0,
            "equity_verified": True,
            "snapshot_at": datetime.now(timezone.utc).isoformat(),
            "complete": True,
        }
    }


def test_rules():
    global PASS
    PASS = 0
    print("=== TEST RULES (booobsas sole source strict contract) ===")

    print("1. compute_deltas detects all change types for booobsas / Onlybooobsas")
    prev = make_book([pos(sid="sol-1", sl=None, tp=121.0)])
    curr = make_book([pos(sid="xrp-1", ticker="XRP", sm=10.0, sq=1.0, sl=118.0, tp=None, cp=1.58)])
    d = listener.compute_deltas(prev, curr)
    ok(any(x["type"] == "source_close" for x in d), "source_close detected")
    ok(any(x["type"] == "new_entry" for x in d), "new_entry detected")

    prev2 = make_book([pos(sid="sol-1", sq=1.0, sl=None, tp=121.0)])
    curr2 = make_book([pos(sid="sol-1", sq=1.0, sl=None, tp=119.0)])
    d2 = listener.compute_deltas(prev2, curr2)
    ok(d2 and d2[0]["type"] == "sltp_change", "sltp_change detected")

    prev3 = make_book([pos(sid="sol-1", sq=1.0)])
    curr3 = make_book([pos(sid="sol-1", sq=1.5)])
    d3 = listener.compute_deltas(prev3, curr3)
    ok(d3 and d3[0]["type"] == "size_add", "size_add detected")

    print("2. rule: source ROI profit filter (+3% ROI on margin skip)")
    c = FakeClient()
    # Position with +4% ROI on margin (last_sim=104, entry_sim=100)
    p_late = pos(ep=100.0, cp=98.0, side="short", last_sim=104.0, entry_sim=100.0)
    st = listener.load_state()
    txt, rec = listener.open_mirror(c, "booobsas", p_late, make_book([p_late])["booobsas"], st)
    ok(rec is None and "too late" in txt, "3% source ROI skip")

    # Position with +2% ROI on margin (last_sim=102, entry_sim=100) -> allowed
    p_allow = pos(ep=100.0, cp=100.0, side="short", last_sim=102.0, entry_sim=100.0, sid="allow-1")
    txt_a, rec_a = listener.open_mirror(c, "booobsas", p_allow, make_book([p_allow])["booobsas"], st)
    ok(rec_a is not None and "OPENED" in txt_a, "+2% source ROI allowed")

    print("3. rule: 1:1 percentage margin allocation with full budget 1.0")
    p_norm = pos(sm=10.0, ep=120.0, cp=120.0, side="short", alloc_pct=10.0, sid="norm-1")  # 10% of 500 = 50 USDT margin
    c_norm = FakeClient()
    txt_n, rec_n = listener.open_mirror(c_norm, "booobsas", p_norm, make_book([p_norm])["booobsas"], st)
    ok(rec_n is not None and "OPENED" in txt_n, "trade opened")
    ok(abs(rec_n["notional"] - 1000.0) < 1.0, f"notional ~ 1000 USDT (got {rec_n['notional']:.2f})")

    print("4. HYPE hedge-mode positionIdx")
    p_hype = pos(ticker="HYPE", side="long", sm=10.0, lev=10, sid="hype-1")
    txt_h, rec_h = listener.open_mirror(c_norm, "booobsas", p_hype, make_book([p_hype])["booobsas"], st)
    ok(rec_h is not None and c_norm.orders[-1]["idx"] == 1, "HYPE long idx 1")

    print("5. stable source_qty size add follow")
    rec_s = {"symbol": "SOLUSDT", "side": "short", "qty": 0.64, "last_applied_source_qty": 1.0, "desired_source_qty": 1.0, "leverage": 20}
    p_now = pos(sm=10.0, sq=1.5, cp=120.0, sid="sol-1")
    txt_s = listener.sync_size(c_norm, "booobsas", p_now, rec_s, st)
    ok(txt_s.startswith("ADDED"), f"add followed: {txt_s}")
    ok(rec_s["last_applied_source_qty"] == 1.5, "last_applied_source_qty updated")

    print("6. SL/TP sync (including clear fields)")
    rec2 = {"symbol": "SOLUSDT", "side": "short", "qty": 1.0}
    txt_s1 = listener.sync_sltp(c_norm, "booobsas", pos(sl=125.0, tp=110.0), rec2)
    ok(txt_s1.startswith("SYNC") and c_norm.stops[-1] == ("sltp", "SOLUSDT", "125.0", "110.0"), "sltp synced")

    txt_clear = listener.sync_sltp(c_norm, "booobsas", pos(sl=None, tp=None), rec2)
    ok(txt_clear.startswith("SYNC") and c_norm.stops[-1] == ("sltp", "SOLUSDT", '0', '0'), "sltp cleared")

    print("7. source close when owner exit net positive after fees -> market closed")
    mirrors_prof = {"SOL/short": {"size": "1.3", "avgPrice": "120.0"}}
    rec4 = {"symbol": "SOLUSDT", "side": "short", "qty": 1.3, "entry_price": 120.0}
    st_close = listener.load_state()
    st_close["mirrored"]["booobsas|SOL/short|sol-1"] = rec4
    c_prof = FakeClient()
    c_prof.ticker_prices["SOLUSDT"] = 115.0  # short entry 120, current 115 -> profitable exit
    txt_prof = listener.execute_source_close(c_prof, {"trader": "booobsas", "position": pos(cp=115.0, side="short", sid="sol-1")}, rec4, mirrors_prof, st_close)
    ok(txt_prof.startswith("CLOSED"), f"profitable source close market closed: {txt_prof}")

    print("8. source close when losing -> retain, cancel bot SL, set BE floor trailing state")
    mirrors_loss = {"SOL/long": {"size": "1.0", "avgPrice": "100.0"}}
    rec_loss = {"symbol": "SOLUSDT", "side": "long", "qty": 1.0, "entry_price": 100.0, "price_target": 120.0}
    st_loss = listener.load_state()
    st_loss["mirrored"]["booobsas|SOL/long|sol-loss"] = rec_loss
    c_loss = FakeClient()
    c_loss.ticker_prices["SOLUSDT"] = 99.0  # long entry 100, current 99 -> net losing
    txt_loss = listener.execute_source_close(c_loss, {"trader": "booobsas", "position": pos(cp=99.0, side="long", sid="sol-loss")}, rec_loss, mirrors_loss, st_loss)
    ok(txt_loss.startswith("TRAIL RETAINED"), f"losing source close retained: {txt_loss}")
    ok(st_loss["retained_trailing"].get("booobsas|SOL/long|sol-loss") is not None, "retained record saved")
    ok(c_loss.stops[-1] == ("sltp", "SOLUSDT", '0', '120.0'), "bot SL canceled while preserving TP")

    print("9. retained trailing manager: pending status below activation threshold (no invalid SL)")
    c_mgr = FakeClient()
    c_mgr.ticker_prices["SOLUSDT"] = 100.1  # activation threshold is ~100.52 (100 * 1.0012 * 1.004)
    c_mgr.live_positions = [{"symbol": "SOLUSDT", "side": "Buy", "size": "1.0", "avgPrice": "100.0"}]
    logs_p = listener.manage_retained_trailing_stops(c_mgr, st_loss)
    ret_p = st_loss["retained_trailing"]["booobsas|SOL/long|sol-loss"]
    ok(ret_p["status"] == "pending", "status remains pending below threshold")
    ok(ret_p["current_sl"] is None, "no invalid exchange SL placed while pending below threshold")

    print("10. retained trailing manager: activation at +0.4% beyond fee BE & BE floor ratcheting")
    c_mgr.ticker_prices["SOLUSDT"] = 101.0  # > 100.52 -> activates!
    logs_a = listener.manage_retained_trailing_stops(c_mgr, st_loss)
    ret_a = st_loss["retained_trailing"]["booobsas|SOL/long|sol-loss"]
    ok(ret_a["status"] == "active", "status activated on crossing +0.4% threshold")
    ok(ret_a["current_sl"] is not None and ret_a["current_sl"] >= 100.12, f"hard SL set at/above BE floor ({ret_a['current_sl']})")

    print("11. never ratchet down for long")
    first_sl = ret_a["current_sl"]
    c_mgr.ticker_prices["SOLUSDT"] = 100.5  # price pulled back, but best price high mark preserved
    logs_down = listener.manage_retained_trailing_stops(c_mgr, st_loss)
    ok(ret_a["current_sl"] == first_sl, "hard SL never ratcheted down on price pullback")

    print("12. short position retained trailing stop & never ratchet up for short")
    st_short = listener.load_state()
    rec_short = {"symbol": "BTCUSDT", "side": "short", "qty": 0.1, "entry_price": 50000.0}
    st_short["mirrored"]["booobsas|BTC/short|btc-s"] = rec_short
    c_s = FakeClient()
    c_s.ticker_prices["BTCUSDT"] = 50100.0  # losing short
    mirrors_s = {"BTC/short": {"size": "0.1", "avgPrice": "50000.0"}}
    listener.execute_source_close(c_s, {"trader": "booobsas", "position": pos(ticker="BTC", side="short", cp=50100.0, sid="btc-s")}, rec_short, mirrors_s, st_short)
    
    # Activate short trailing
    c_s.ticker_prices["BTCUSDT"] = 49600.0  # short profitable drop < activation threshold ~49740
    c_s.live_positions = [{"symbol": "BTCUSDT", "side": "Sell", "size": "0.1", "avgPrice": "50000.0"}]
    listener.manage_retained_trailing_stops(c_s, st_short)
    ret_s = st_short["retained_trailing"]["booobsas|BTC/short|btc-s"]
    ok(ret_s["status"] == "active", "short trailing activated")
    short_sl_1 = ret_s["current_sl"]
    
    # Price rises slightly, verify short SL does not ratchet up
    c_s.ticker_prices["BTCUSDT"] = 49800.0
    listener.manage_retained_trailing_stops(c_s, st_short)
    ok(ret_s["current_sl"] == short_sl_1, "short SL never ratcheted up on price rise")

    print("13. manual position conflict / ignore (WLD/short & manual list)")
    p_wld = pos(ticker="WLD", side="short", sid="wld-1")
    txt_w, rec_w = listener.open_mirror(c_norm, "booobsas", p_wld, make_book([p_wld])["booobsas"], st)
    ok(rec_w is None and "manual protect active" in txt_w, "WLD/short manual protect active")

    print("14. unsupported market status (e.g. FETClosed)")
    c_fet = FakeClient()
    c_fet.instruments["FETUSDT"] = {"status": "Closed", "lotSizeFilter": {}, "priceFilter": {}, "leverageFilter": {}}
    p_fet = pos(ticker="FET", side="long", sid="fet-1")
    txt_f, rec_f = listener.open_mirror(c_fet, "booobsas", p_fet, make_book([p_fet])["booobsas"], st)
    ok(rec_f is None and "is not Trading" in txt_f, "unsupported market status skipped")

    print("15. explicit coin mapping (KPEPE -> 1000PEPEUSDT <-> kPEPE)")
    from bybit_api import coin_to_symbol, symbol_to_coin
    ok(coin_to_symbol("KPEPE") == "1000PEPEUSDT", "KPEPE -> 1000PEPEUSDT")
    ok(coin_to_symbol("KBONK") == "1000BONKUSDT", "KBONK -> 1000BONKUSDT")
    ok(symbol_to_coin("1000PEPEUSDT") == "kPEPE", "1000PEPEUSDT -> kPEPE")

    print("16. malformed / stale (>10min or future >30s) payload fail closed")
    stale_ts = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() - 700, timezone.utc).isoformat()
    bad_bk = {"booobsas": {"positions": [pos()], "complete": True, "equity_verified": True, "source_equity": 100.0, "snapshot_at": stale_ts}}
    lp_bad = listener.WebhookPayload(source="t", fired_at=stale_ts, recents={}, books=bad_bk)
    res_bad = asyncio.run(listener.involio_delta(lp_bad, x_signature="testsecret"))
    ok(res_bad.get("fail_closed") is True, "stale payload fail closed")

    print("17. idempotence & fill confirmation before SL/TP")
    c_idem = FakeClient()
    p_idem = pos(ticker="ETH", side="long", sm=10.0, alloc_pct=10.0, sid="eth-idem")
    st_idem = {"mirrored":{},"manual":[],"parked_ambiguous":{}}
    st_idem["hold_new"] = False
    st_idem["cutover_armed"] = True
    txt_i1, rec_i1 = listener.open_mirror(c_idem, "booobsas", p_idem, make_book([p_idem])["booobsas"], st_idem)
    ok(rec_i1 is not None, "first entry opened")
    ok("booobsas|ETH/long|eth-idem" in st_idem["mirrored"], "record saved in mirrored immediately")
    
    # Second attempt on same position blocked
    txt_i2, rec_i2 = listener.open_mirror(c_idem, "booobsas", p_idem, make_book([p_idem])["booobsas"], st_idem)
    ok(rec_i2 is None and "repeated entry blocked" in txt_i2, "repeated entry blocked idempotently")

    print(f"\nALL {PASS} RULES CHECKS PASSED")
    return PASS


if __name__ == "__main__":
    test_rules()
