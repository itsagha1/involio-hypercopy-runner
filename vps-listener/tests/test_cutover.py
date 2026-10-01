"""Cutover test suite for sole source 'booobsas' (no network, fake Bybit client)."""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from datetime import datetime, timezone

os.environ["WEBHOOK_SHARED_SECRET"] = "cutoversecret"
os.environ["DRY_RUN"] = "false"
tmp = tempfile.mkdtemp()
os.environ["STATE_FILE"] = os.path.join(tmp, "cutover_state.json")
os.environ["LOG_FILE"] = os.path.join(tmp, "cutover_actions.log")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import listener
listener.SHARED_SECRET = "cutoversecret"
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
        price = self.ticker_prices.get(s, "100.0")
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


def pos(ticker: str = "SOL", side: str = "long", sm: float = 10.0, sq: float = 1.0, lev: int = 10,
        ep: float = 100.0, cp: float = 100.0, sl=None, tp=None, sid: str = "sol-1",
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


listener.BybitClient=FakeClient

def test_cutover_rules():
    global PASS
    PASS = 0
    print("=== BYBIT CUTOVER TEST SUITE (booobsas sole source) ===")

    # Test 1: Sole source profile filtering
    print("\n1. Sole source profile filtering (booobsas only)")
    prev = make_book([])
    curr = make_book([pos(ticker="SOL", side="long")])
    curr["limpan96"] = {"positions": [pos(ticker="BTC", side="long")]}
    deltas = listener.compute_deltas(prev, curr)
    ok(len(deltas) == 1 and deltas[0]["trader"] == "booobsas", "only booobsas deltas computed")

    # Test 2: Legacy mirror migration to manual owner-managed positions
    print("\n2. Legacy mirror migration to manual owner-managed positions")
    st = {
        "mirrored": {
            "limpan96|BTC/long|btc-1": {"symbol": "BTCUSDT", "qty": 0.1},
            "booobsas|SOL/long|sol-1": {"symbol": "SOLUSDT", "qty": 1.0},
        },
        "manual": ["WLD/short"]
    }
    migrated = listener.migrate_legacy_mirrors(st)
    ok(migrated == ["limpan96|BTC/long|btc-1"], "legacy mirror migrated")
    ok("limpan96|BTC/long|btc-1" not in st["mirrored"], "legacy removed from mirrored")
    ok(st["manual"] == ["BTC/long", "WLD/short"], "BTC/long and existing WLD/short preserved in manual")

    # Test 3: 1:1 Percentage margin allocation calculation
    print("\n3. 1:1 Percentage margin allocation math with verified DECLARED allocation pct")
    p_sample = pos(sm=10.0, alloc_pct=10.0)
    book_info = make_book([p_sample])["booobsas"]
    desired_margin, err = listener.compute_desired_margin(p_sample, book_info, owner_wallet=500.0)
    ok(err == "", "no margin calculation error")
    ok(abs(desired_margin - 50.0) < 1e-9, f"10% source equity -> 50 USDT owner margin (got {desired_margin})")

    # Test 4: Unresolved source total equity returns BLOCKED sizing
    print("\n4. Unresolved source total equity returns BLOCKED sizing")
    book_bad = {"source_equity": 0.0, "equity_verified": False, "complete": False, "positions": []}
    p_unver = pos(sm=10.0, alloc_ver=False)
    desired_margin_bad, err_bad = listener.compute_desired_margin(p_unver, book_bad, owner_wallet=500.0)
    ok(desired_margin_bad == 0.0 and "BLOCKED" in err_bad, f"unresolved equity blocked: {err_bad}")

    c = FakeClient(avail=500.0, wallet=500.0)
    st_run = listener.load_state()
    txt, rec = listener.open_mirror(c, "booobsas", p_unver, book_bad, st_run)
    ok(rec is None and "BLOCKED" in txt, f"open_mirror blocked on unresolved equity: {txt}")

    # Test 5: Match source leverage directly (no silent capping)
    print("\n5. Match source leverage directly")
    p_high_lev = pos(ticker="SOL", side="long", sm=10.0, alloc_pct=10.0, lev=25, sid="sol-25x")
    c_lev = FakeClient(avail=500.0, wallet=500.0)
    txt, rec_lev = listener.open_mirror(c_lev, "booobsas", p_high_lev, book_info, st_run)
    ok(rec_lev is not None and rec_lev["leverage"] == 25, f"25x leverage matched exactly: {rec_lev['leverage']}")
    ok(c_lev.lev == [("SOLUSDT", 25)], "set_leverage called with 25x")

    # Test 6: Full budget 1.0 (remove 50% cap)
    print("\n6. Full budget 1.0 (remove 50% cap)")
    c_budget = FakeClient(avail=500.0, wallet=500.0)
    st_budget = {"mirrored": {"booobsas|ETH/long|eth-1": {"qty": 1.0, "entry_price": 2000.0, "leverage": 10}}} # 200 USDT margin
    b_margin = listener.margin_budget(c_budget, st_budget) # 1.0*500 - 200 = 300 USDT
    ok(b_margin == 300.0, f"full budget allows remaining margin up to wallet (got {b_margin})")

    # Test 7: Full desired allocation policy (no silent shrinking)
    print("\n7. Full desired allocation policy (no silent shrinking)")
    c_tight = FakeClient(avail=30.0, wallet=500.0)
    txt_shrink, rec_shrink = listener.open_mirror(c_tight, "booobsas", p_sample, book_info, st_run)
    ok(rec_shrink is None and "no silent shrinking" in txt_shrink, f"skipped exceeding margin without shrinking: {txt_shrink}")

    # Min notional test
    p_tiny = pos(sm=0.1, alloc_pct=0.1, lev=1, sid="tiny-1")  # 0.1% of 500 = 0.5 USDT notional < 5 USDT min
    c_tiny = FakeClient(avail=500.0, wallet=500.0)
    txt_tiny, rec_tiny = listener.open_mirror(c_tiny, "booobsas", p_tiny, book_info, st_run)
    ok(rec_tiny is None and "below minimum order size" in txt_tiny, f"skipped below minnotional: {txt_tiny}")

    # Test 8: Source ROI profit filter (+3% ROI on margin)
    print("\n8. Source ROI profit filter (+3% ROI on margin)")
    p_prof = pos(ticker="SOL", side="long", ep=100.0, cp=103.5, last_sim=103.5, entry_sim=100.0, sid="sol-prof")
    c_prof = FakeClient(avail=500.0, wallet=500.0)
    txt_prof, rec_prof = listener.open_mirror(c_prof, "booobsas", p_prof, book_info, st_run)
    ok(rec_prof is None and "already +" in txt_prof and "source ROI" in txt_prof, f"+3.5% ROI skipped: {txt_prof}")

    # Test 9: Size add follow based on stable source_qty
    print("\n9. Size add follow based on stable source_qty")
    rec_adds = {"symbol": "SOLUSDT", "side": "long", "qty": 5.0, "last_applied_source_qty": 1.0, "desired_source_qty": 1.0, "leverage": 10}
    p_now_add = pos(sm=10.0, sq=1.2, cp=100.0, sid="sol-add")
    c_adds = FakeClient(avail=500.0, wallet=500.0)
    txt_add = listener.sync_size(c_adds, "booobsas", p_now_add, rec_adds, st_run)
    ok(txt_add.startswith("ADDED"), f"add followed: {txt_add}")
    ok(rec_adds["last_applied_source_qty"] == 1.2, "last_applied_source_qty updated")

    # Test 10: Source close -> fee-aware breakeven trailing stop vs profitable close
    print("\n10. Source close -> fee-aware breakeven trailing stop vs profitable close")
    c_close = FakeClient(avail=500.0, wallet=500.0)
    open_mirrors = {"SOL/long": {"size": "5.0", "avgPrice": "100.0"}}
    rec_close = {"symbol": "SOLUSDT", "side": "long", "qty": 5.0, "entry_price": 100.0}
    d_close = {"trader": "booobsas", "position": pos(ticker="SOL", side="long", cp=100.0)}
    st_close = listener.load_state()
    st_close["mirrored"]["booobsas|SOL/long|sol-1"] = rec_close

    txt_close = listener.execute_source_close(c_close, d_close, rec_close, open_mirrors, st_close)
    ok(txt_close.startswith("TRAIL RETAINED"), f"source_close set retained trailing stop: {txt_close}")
    ok(st_close["retained_trailing"].get("booobsas|SOL/long|sol-1") is not None, "retained state saved")

    # Test 11: Cutover config endpoint (prepare & arm)
    print("\n11. Cutover configuration endpoint (prepare & arm)")
    st_cfg = listener.load_state()
    st_cfg["books"] = make_book([pos()])
    listener.save_state(st_cfg)

    req_prep = listener.CutoverConfigRequest(action="prepare", secret="cutoversecret")
    res_prep = asyncio.run(listener.cutover_config(req_prep, x_signature="cutoversecret"))
    ok(res_prep.get("ok") and res_prep.get("hold_new") is True and res_prep.get("armed") is False, "prepare success")

    req_arm = listener.CutoverConfigRequest(action="arm", hold_new=False, secret="cutoversecret")
    res_arm = asyncio.run(listener.cutover_config(req_arm, x_signature="cutoversecret"))
    ok(res_arm.get("ok") and res_arm.get("armed") is True and res_arm.get("hold_new") is False, "arm success")

    # Test 12: Manual protect verification (WLD short & manual positions)
    print("\n12. Manual protect verification (WLD short & manual positions)")
    p_wld = pos(ticker="WLD", side="short", sid="wld-1")
    txt_wld, rec_wld = listener.open_mirror(c, "booobsas", p_wld, book_info, st_run)
    ok(rec_wld is None and "manual protect active" in txt_wld, "WLD short protected")

    # Test 13: Backwards compatibility for /status and /health
    print("\n13. Backwards compatibility for /status and /health")
    res_health = listener.health()
    ok(res_health["status"] == "ok" and res_health["version"] == listener.LISTENER_VERSION, "health endpoint compatible")

    print(f"\nALL {PASS} CUTOVER CHECKS PASSED SUCCESSFULLY!")
    return PASS


if __name__ == "__main__":
    test_cutover_rules()
