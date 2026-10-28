"""
100% COPY listener: one instance per Bybit subaccount, bound to ONE Involio profile.

Owner spec 2026-10-12 (separate from the protected-mirror main listener):
- Full copy of the bound profile: sizes, adds/reductions, SL/TP, leverage synced.
- Source-lost rule (owner 2026-10-13): a mirrored trade whose source disappears from
  the books for 2 consecutive polls (closed, lost, or unavailable) is treated as a
  source close: profitable -> closed at market; losing -> retained with an exchange-side
  TP trigger at fee-adjusted breakeven +/- 0.5% (exits at breakeven+ even if the bot dies),
  then trailing management exactly like the main listener: once price reaches the activation
  threshold the TP trigger is cancelled and a hard SL is ratcheted on the exchange, 0.5%
  behind the best price, never worse than the fee-adjusted breakeven floor (owner 2026-10-13).
- New entries already at >= 2% source-margin ROI are skipped as "watch" candidates;
  each poll re-checks watched trades and opens at market once ROI < 2% again.
- Existing positions at first run are baselined (never mirrored).
- 1:1 percentage-based sizing: margin = source declared allocation % x full subaccount
  wallet, leverage matched (cap 50x), min notional 5 USDT, 70% margin cap as guard.
- Self-polls Involio directly (no Base44 credits); /status + /log for digest/health.

Config via environment (per-instance root-owned env file):
  COPY_PROFILE, COPY_PORTFOLIO_ID, COPY_PORT, COPY_NAME,
  INVOLIO_REFRESH_TOKEN, BYBIT_API_KEY (+ RSA file or BYBIT_API_SECRET), BYBIT_PROXY,
  POLL_SECONDS (default 300), DRY_RUN (default true), COPY_STATE_FILE, COPY_LOG_FILE.
"""

from __future__ import annotations

import asyncio
import threading
import hashlib
import json
import math
import hmac
import os
import time
from datetime import datetime, timezone
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).with_name(".env"), override=False)
except (ImportError, PermissionError):
    pass

from fastapi import FastAPI, Request, Header, HTTPException, Body
from starlette.concurrency import run_in_threadpool

import source_api
from source_api import SourceDataError
import bybit_api
from bybit_api import (BybitClient, BybitError, bybit_side, close_side,
                       coin_to_symbol, position_idx, round_qty, symbol_to_coin)

APP_VERSION = "c1.0.9"
PROFILE = os.environ.get("COPY_PROFILE", "")
PROFILE_NAMES = [x.strip() for x in PROFILE.split(",") if x.strip()]
PORTFOLIO_ID = os.environ.get("COPY_PORTFOLIO_ID", "")
PORTFOLIO_IDS = [p.strip() for p in PORTFOLIO_ID.split(",") if p.strip()]
PORT = int(os.environ.get("COPY_PORT", "8001"))
NAME = os.environ.get("COPY_NAME", PROFILE or "copy")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "300"))
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"
STATE_FILE = os.environ.get("COPY_STATE_FILE", f"/var/lib/hypercopy-copy/{NAME}_state.json")
LOG_FILE = os.environ.get("COPY_LOG_FILE", f"/var/lib/hypercopy-copy/{NAME}_actions.log")
STATE_LOCK = __import__("threading").Lock()

PROFIT_SKIP_ROI = 0.02          # 2% profit-skip threshold (owner 2026-10-12)
FEE_ALLOWANCE = 0.0012      # 0.12% roundtrip fee allowance for source-lost breakeven math
TP_TRIGGER_OFFSET = 0.005  # +0.5% beyond fee-adjusted breakeven for the retained TP trigger
TRAILING_DIST_PCT = 0.005  # 0.5% price trailing distance (same rule as the main listener)
MAX_LEVERAGE = 50
MIN_NOTIONAL = 5.0
MARGIN_CAP = 0.70               # guard: total margin cap of wallet (like main listener)
MAX_AVAIL_PER_TRADE = 0.90
LOG_TAIL_LINES = 400

# Bind the configured profile into the shared source adapter (validated on every fetch).
if PROFILE and PORTFOLIO_ID:
    # multi-owner subaccounts (owner 2026-10-16: bluespire + b4451): bind every
    # configured profile name to its portfolio; ownership is re-validated per
    # fetch against the portfolio owner username in normalize_source.
    for _name, _pid in zip(PROFILE_NAMES, PORTFOLIO_IDS):
        source_api.PROFILES[_name] = _pid


def log_action(line: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{ts} UTC] {NAME}: {line}"
    try:
        Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a") as f:
            f.write(entry + "\n")
    except OSError:
        pass


def log_tail(n: int = LOG_TAIL_LINES) -> List[str]:
    try:
        with open(LOG_FILE) as f:
            return f.readlines()[-n:]
    except OSError:
        return []


def load_state() -> Dict[str, Any]:
    with STATE_LOCK:
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE) as f:
                    s = json.load(f)
                for key, default in (("baseline", []), ("mirrored", {}), ("skip_watch", {}),
                                      ("manual", []), ("entry_blocks", {}), ("books", None),
                                      ("last_poll", None), ("errors_last_run", 0), ("armed", False),
                                      ("retained", {}), ("source_missing", {})):
                    s.setdefault(key, default)
                return s
            except (json.JSONDecodeError, OSError):
                pass
    return {"version": APP_VERSION, "baseline": [], "mirrored": {}, "skip_watch": {},
            "manual": [], "entry_blocks": {}, "books": None, "last_poll": None,
            "errors_last_run": 0, "armed": False, "retained": {}, "source_missing": {}}


def save_state(state: Dict[str, Any]) -> None:
    with STATE_LOCK:
        Path(STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2, default=str)
        os.replace(tmp, STATE_FILE)


def tkey(p: Dict[str, Any]) -> str:
    # Owner 2026-10-20: the key uses the PER-POSITION source profile (tagged by
    # fetch_merged_books from the portfolio the trade came from), never the
    # account-level COPY_PROFILE string - adding a profile to a subaccount no
    # longer rewrites the keys of existing mirrors.
    return f"{p.get('profile') or PROFILE}|{p['ticker']}/{p['side']}|{p['source_id']}"


def bybit_key(p: Dict[str, Any]) -> str:
    return f"{p['ticker']}/{p['side']}"


def order_link(key: str, operation: str, generation: int = 0) -> str:
    return "CC" + hashlib.sha256((key + "|" + operation + "|" + str(generation)).encode()).hexdigest()[:32]


def source_roi(p: Dict[str, Any]) -> Optional[float]:
    es, ls = p.get("entry_sim"), p.get("last_sim")
    try:
        es, ls = float(es), float(ls)
        if es > 0 and math.isfinite(ls):
            return ls / es - 1.0
    except (TypeError, ValueError):
        pass
    return None


# ---------------------------------------------------------------- sizing

def desired_margin(p: Dict[str, Any], wallet: float) -> Tuple[float, Optional[str]]:
    """1:1 percentage-based: our margin = source declared allocation % x our wallet."""
    pct = float(p.get("source_allocation_pct") or 0)
    if pct <= 0:
        return 0.0, "missing declared allocation percentage"
    if not p.get("allocation_verified"):
        return 0.0, "source allocation not verified"
    return (pct / 100.0) * wallet, None


def margin_used(state: Dict[str, Any]) -> float:
    return sum(float(r.get("margin") or 0) for r in state["mirrored"].values())


# ---------------------------------------------------------------- entry

def open_mirror(client: BybitClient, p: Dict[str, Any], state: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    key, bk = tkey(p), bybit_key(p)
    coin, side = p["ticker"], str(p["side"]).lower()
    symbol = coin_to_symbol(coin)

    if key in state["mirrored"]:
        return f"LOG {key} new_entry: mirror already exists", None
    if bk in state["manual"]:
        return f"SKIP {key} new_entry: manual protect active for {bk}", None
    if state.get("margin_hold"):
        return (f"SKIP {key} new_entry: margin hold active ({state.get('margin_pct', 0):.1f}% used); "
                f"entries resume when margin <= 50%", None)

    roi = source_roi(p)
    if roi is not None and roi >= PROFIT_SKIP_ROI:
        state["skip_watch"][key] = {"roi": roi, "since": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        return f"SKIP-WATCH {key} new_entry: source ROI +{roi*100:.2f}% >= +2%; watching for ROI drop below 2%", None

    try:
        try:
            live_positions = client.get_positions(symbol)
        except BybitError as e:
            if "retCode=10001" in str(e) or "symbol not exist" in str(e):
                return f"SKIP {key} new_entry: symbol delisted on Bybit", None
            raise
        for lp in live_positions:
            if float(lp.get("size") or 0) > 0:
                state["manual"].append(bk) if bk not in state["manual"] else None
                return f"SKIP {key} new_entry: live position conflict on Bybit for {symbol}/{side}", None

        inst = client.get_instrument(symbol)
        if inst.get("status") != "Trading":
            return f"SKIP {key} new_entry: instrument status '{inst.get('status')}' not Trading", None

        lev = min(int(float(p.get("leverage") or 1)) or 1, MAX_LEVERAGE)
        lev_filter = inst.get("leverageFilter", {})
        min_lev = int(float(lev_filter.get("minLeverage", "1")))
        max_lev = int(float(lev_filter.get("maxLeverage", "100")))
        if not (min_lev <= lev <= max_lev):
            return f"SKIP {key} new_entry: leverage {lev}x outside exchange bounds", None

        wallet = float(client.get_balance() or 0)
        avail = float(client.get_available_balance() or 0)
        dm, err = desired_margin(p, wallet)
        if err:
            return f"SKIP {key} new_entry: {err}", None
        # Owner 2026-10-28: never OPEN a position whose allocation exceeds 20%
        # of wallet (1:1 pct sizing => desired margin > 20% of wallet).
        # Size increases on existing mirrors are exempt (sync_size uncapped).
        if wallet > 0 and dm > 0.20 * wallet:
            return (f"SKIP {key} new_entry: desired margin {dm:.2f} exceeds 20% of wallet "
                    f"({wallet:.2f}) - no oversized entries", None)
        notional = dm * lev
        min_notional = max(MIN_NOTIONAL, float(inst.get("lotSizeFilter", {}).get("minNotionalValue") or MIN_NOTIONAL))

        if dm > wallet * MARGIN_CAP - margin_used(state):
            return f"SKIP {key} new_entry: desired margin {dm:.2f} exceeds remaining margin cap", None
        if dm > avail * MAX_AVAIL_PER_TRADE:
            return f"SKIP {key} new_entry: desired margin {dm:.2f} exceeds 90% of available", None
        if notional < min_notional:
            return f"SKIP {key} new_entry: notional {notional:.2f} below minimum {min_notional:.0f} USDT", None

        price = float(client.get_ticker(symbol)["lastPrice"])
        sl = float(p.get("stop_loss") or 0)
        tp = float(p.get("price_target") or 0)
        if sl and ((side == "long" and sl >= price) or (side == "short" and sl <= price)):
            return f"SKIP {key} new_entry: source stop already crossed", None
        if tp and ((side == "long" and tp <= price) or (side == "short" and tp >= price)):
            return f"SKIP {key} new_entry: source target already crossed", None

        lot = inst.get("lotSizeFilter", {})
        min_qty = float(lot.get("minOrderQty") or lot.get("minQty") or "0")
        qty_raw = notional / price
        if min_qty and qty_raw < min_qty:
            return f"SKIP {key} new_entry: mirror qty {qty_raw:.10f} below exchange minimum", None
        qty = round_qty(qty_raw, inst)
        max_qty = float(lot.get("maxMktOrderQty") or lot.get("maxOrderQty") or lot.get("maxQty", "99999999"))
        if qty > max_qty:
            return f"SKIP {key} new_entry: qty {qty} exceeds exchange maxQty", None
        if qty * price < min_notional:
            return f"SKIP {key} new_entry: notional after quantization below minimum", None

        client.set_leverage(symbol, lev)
        requested_qty = qty
        fill = place_order_mode_safe(client, symbol, bybit_side(side), qty,
                                  position_idx=position_idx(symbol, side),
                                  order_link_id=order_link(key, "open"))
        qty = float(fill.get("filled_qty") or qty)
        price = float(fill.get("filled_avg_price") or price)

        rec = {"symbol": symbol, "side": side, "qty": qty, "margin": dm,
               "source_id": p.get("source_id"),
               "last_applied_source_qty": float(p.get("source_qty") or 0) * (qty / requested_qty if requested_qty else 1),
               "desired_source_qty": float(p.get("source_qty") or 0),
               "leverage": lev, "opened_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "entry_price": price, "price_target": p.get("price_target"), "stop_loss": p.get("stop_loss")}
        state["mirrored"][key] = rec
        state["skip_watch"].pop(key, None)

        if p.get("stop_loss") or p.get("price_target"):
            try:
                client.set_sl_tp(symbol, p.get("stop_loss"), p.get("price_target"),
                                position_idx=position_idx(symbol, side))
            except BybitError as e:
                rec["sltp_pending"] = True
                log_action(f"ERROR {key} initial_sltp_failed: {e} - will retry on poll")

        return (f"OPENED {key} 100% copy: {qty} {coin} (~{qty*price:.2f} USDT notional, "
                f"margin {dm:.2f} USDT, lev {lev}x)"), rec
    except BybitError as e:
        return f"ERROR {key} open_mirror: {e}", None


# ---------------------------------------------------------------- follow source

def sync_size(client: BybitClient, p: Dict[str, Any], rec: Dict[str, Any]) -> Optional[str]:
    key = tkey(p)
    curr_sq = float(p.get("source_qty") or 0)
    last_sq = float(rec.get("last_applied_source_qty") or rec.get("desired_source_qty") or curr_sq)
    if curr_sq <= 0 or last_sq <= 0:
        return None
    if abs(curr_sq - last_sq) <= max(1e-9, 1e-9 * curr_sq):
        rec["last_applied_source_qty"] = curr_sq
        rec["desired_source_qty"] = curr_sq
        return None
    ratio = curr_sq / last_sq
    try:
        symbol, side = rec["symbol"], rec["side"]
        inst = client.get_instrument(symbol)
        idx = position_idx(symbol, side)
        if curr_sq > last_sq:
            delta = round_qty(rec["qty"] * ratio - rec["qty"], inst)
            if delta <= 0:
                return None
            last_px = float(client.get_ticker(symbol)["lastPrice"] or 0)
            if last_px > 0 and delta * last_px < 5.0:
                # Owner 2026-10-18: Bybit minimum order value is 5 USDT. A
                # sub-minimum size-add is ACCUMULATED, not an error: the desired
                # source qty is kept and the full remaining gap is retried every
                # poll until it clears the exchange minimum (bigger source adds
                # or wallet growth). Never counts as an engine error.
                rec["desired_source_qty"] = curr_sq
                return (f"SKIP {key} size_add: +{delta} (~{delta * last_px:.2f} USDT) "
                        f"below Bybit 5 USDT minimum - accumulated, retried next poll")
            avail = float(client.get_available_balance() or 0)
            lev = int(rec.get("leverage") or 1) or 1
            if delta * float(client.get_ticker(symbol)["lastPrice"]) / lev > avail * MAX_AVAIL_PER_TRADE:
                rec["desired_source_qty"] = curr_sq
                return f"SKIP {key} size_add: insufficient available balance - target kept for retry"
            place_order_mode_safe(client, symbol, bybit_side(side), delta, position_idx=idx,
                               order_link_id=order_link(key, "add", int(rec.get("gen", 0))))
            rec["qty"] = rec["qty"] + delta
            action = f"ADDED {key} 100% follow: +{delta} (now {rec['qty']}, source qty {curr_sq:.6g})"
        else:
            delta = round_qty(rec["qty"] * (1.0 - ratio), inst)
            if delta <= 0:
                rec["last_applied_source_qty"] = curr_sq
                rec["desired_source_qty"] = curr_sq
                return None
            place_order_mode_safe(client, symbol, close_side(side), delta, position_idx=idx, reduce_only=True,
                               order_link_id=order_link(key, "reduce", int(rec.get("gen", 0))))
            rec["qty"] = max(0.0, rec["qty"] - delta)
            action = f"REDUCED {key} 100% follow: -{delta} (now {rec['qty']}, source qty {curr_sq:.6g})"
        rec["gen"] = int(rec.get("gen", 0)) + 1
        rec["last_applied_source_qty"] = curr_sq
        rec["desired_source_qty"] = curr_sq
        return action
    except BybitError as e:
        return f"ERROR {key} sync_size: {e}"


def sync_sltp(client: BybitClient, p: Dict[str, Any], rec: Dict[str, Any]) -> Optional[str]:
    key = tkey(p)
    sl, tp = p.get("stop_loss"), p.get("price_target")
    if rec.get("stop_loss") == sl and rec.get("price_target") == tp and not rec.get("sltp_pending"):
        return None
    try:
        price = float(client.get_ticker(rec["symbol"])["lastPrice"])
        if sl and ((rec["side"] == "long" and float(sl) >= price) or (rec["side"] == "short" and float(sl) <= price)):
            return None
        if tp and ((rec["side"] == "long" and float(tp) <= price) or (rec["side"] == "short" and float(tp) >= price)):
            return None
        client.set_sl_tp(rec["symbol"], sl, tp, position_idx=position_idx(rec["symbol"], rec["side"]))
        rec["stop_loss"] = sl
        rec["price_target"] = tp
        rec.pop("sltp_pending", None)
        return f"SYNC {key} SL/TP: sl={sl} tp={tp}"
    except BybitError as e:
        return f"ERROR {key} sync_sltp: {e}"


def follow_source_close(client: BybitClient, p: Dict[str, Any], rec: Dict[str, Any]) -> str:
    """100% copy: source closed -> we close at market regardless of P/L."""
    key = tkey(p)
    symbol, side = rec["symbol"], rec["side"]
    roi = source_roi(p)
    try:
        live = {lp.get("symbol"): lp for lp in client.get_positions(symbol)}
        lp = live.get(symbol)
        qty_live = float(lp.get("size") or 0) if lp else 0
        qty = min(rec["qty"], qty_live) if qty_live > 0 else rec["qty"]
        if qty_live <= 0:
            return f"CLOSED {key}: position already flat on exchange; deregistered"
        inst = client.get_instrument(symbol)
        qty = round_qty(qty, inst)
        place_order_mode_safe(client, symbol, close_side(side), qty, position_idx=position_idx(symbol, side),
                           reduce_only=True, order_link_id=order_link(key, "close", int(rec.get("gen", 0))))
        return (f"CLOSED {key} 100% follow: source closed"
                f"{' at source ROI %+.2f%%' % (roi * 100) if roi is not None else ''} "
                f"- mirror closed at market (loss followed, no retention)")
    except BybitError as e:
        return f"ERROR {key} follow_source_close: {e} - will retry next poll"


def handle_source_lost(client: BybitClient, rec_key: str, state: Dict[str, Any]) -> str:
    """Owner rule 2026-10-13 (source-lost, subaccounts): a mirrored trade whose source is
    absent from the books 2 consecutive polls (closed, lost, or unavailable) is treated as
    a source close. Profitable -> closed at market; losing -> retained with an exchange-side
    TP trigger order at fee-adjusted breakeven +/- 0.5%, so the position exits at
    breakeven+ even if the bot dies. Retained positions are deregistered from mirroring
    and left to the exchange TP order; the TP trigger itself is never managed further."""
    rec = state["mirrored"].get(rec_key)
    if rec is None:
        return f"LOG {rec_key} source-lost: mirror already gone; nothing to do"
    symbol, side = rec["symbol"], rec["side"]
    key = rec_key
    try:
        live = [lp for lp in client.get_positions(symbol)
                if lp.get("side") == ("Buy" if side == "long" else "Sell")
                and float(lp.get("size") or 0) > 0]
        lp = live[0] if live else None
        if lp is None:
            return f"SOURCE-LOST-CLOSED {key}: source gone 2 polls; position already flat on exchange; deregistered"
        qty = float(lp["size"])
        avg = float(lp.get("avgPrice") or 0)
        mark = float(lp.get("markPrice") or avg)
        direction = 1.0 if side == "long" else -1.0
        notional = avg * qty
        gross = (mark - avg) * direction * qty
        net_return = (gross - notional * FEE_ALLOWANCE) / notional if notional > 0 else 0.0
        if net_return >= 0.005:
            # profitable -> close at market (100% copy behaviour for winning closes)
            inst = client.get_instrument(symbol)
            q = round_qty(min(qty, float(rec["qty"]) or qty), inst)
            place_order_mode_safe(client, symbol, close_side(side), q,
                                  position_idx=position_idx(symbol, side), reduce_only=True,
                                  order_link_id=order_link(key, "lostclose", int(rec.get("gen", 0))))
            return (f"SOURCE-LOST-CLOSED {key}: source gone 2 polls, net {net_return * 100:+.2f}% "
                    f"-> closed at market (profitable)")
        # losing -> retain with exchange-side TP trigger at fee-adjusted breakeven +/- 0.5%
        fee_be = float(lp.get("breakEvenPrice") or 0)
        if fee_be <= 0:
            fee_be = avg * (1.0 + FEE_ALLOWANCE * direction)
        tp = fee_be * (1.0 + TP_TRIGGER_OFFSET) if side == "long" else fee_be * (1.0 - TP_TRIGGER_OFFSET)
        inst = client.get_instrument(symbol)
        tick = float((inst.get("priceFilter") or {}).get("tickSize") or "0.01")
        grid = Decimal(str(tick))
        rounding = ROUND_FLOOR if side == "long" else ROUND_CEILING
        tp = float((Decimal(str(tp)) / grid).to_integral_value(rounding=rounding) * grid)
        src_tp = float(rec.get("price_target") or 0)
        if src_tp > 0:
            tp = max(src_tp, tp) if side == "long" else min(src_tp, tp)
        client.set_sl_tp(symbol, stop_loss=0, take_profit=tp, position_idx=position_idx(symbol, side))
        act_thresh = fee_be * (1.0 + TP_TRIGGER_OFFSET) if side == "long" else fee_be * (1.0 - TP_TRIGGER_OFFSET)
        state.setdefault("retained", {})[key] = {
            "profile": rec.get("profile"), "symbol": symbol, "side": side, "qty": qty, "entry": avg,
            "fee_be": fee_be, "tp_trigger": tp, "activation_threshold": act_thresh,
            "status": "pending", "best_price": mark, "current_sl": None,
            "source_tp": src_tp,
            "retained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source_id": rec.get("source_id")}
        return (f"SOURCE-LOST-RETAINED {key}: source gone 2 polls, net {net_return * 100:+.2f}% "
                f"-> retained with exchange TP trigger at {tp} (BE {fee_be:.6f}); "
                f"exits at breakeven+ even if the bot dies")
    except BybitError as e:
        return f"ERROR {key} source-lost handling: {e} - will retry next poll"


_GLOBAL_STATE: Dict[str, Any] = {"state": load_state()}


def state_get() -> Dict[str, Any]:
    return _GLOBAL_STATE["state"]


_MIRROR_STATS = {"ts": 0.0, "data": None}

def mirror_stats_payload():
    """Owner 2026-10-20: per-source stats of the trades WE mirrored (actions log
    joined to Bybit closed-PnL records) plus the account's cumulative realized
    P/L. Cached 5 minutes. Failure degrades to the previous payload."""
    import time as _t
    if _MIRROR_STATS["data"] is not None and _t.time() - _MIRROR_STATS["ts"] < 300:
        return _MIRROR_STATS["data"]
    client = BybitClient()
    if not client.configured or DRY_RUN:
        return _MIRROR_STATS["data"] or {"profiles": {}, "cumulative_pnl": None}
    try:
        from mirror_stats import build as _ms_build
        recs = client.get_closed_pnl_records()
        try:
            with open(LOG_FILE) as f:
                lines = f.readlines()
        except OSError:
            lines = []
        state = state_get()
        idmap = {}
        current_open = {}
        for k, rec in list((state.get("mirrored") or {}).items()):
            prof = rec.get("profile") or str(k).split("|")[0].split(",")[0]
            current_open[str(k)] = prof
            if rec.get("profile") and rec.get("source_id"):
                idmap[str(rec["source_id"])] = rec["profile"]
        for p in (state.get("books") or {}).get("positions") or []:
            if p.get("profile") and p.get("source_id"):
                idmap[str(p["source_id"])] = p["profile"]
        for k, rec in list((state.get("retained") or {}).items()):
            if rec.get("profile") and rec.get("source_id"):
                idmap[str(rec["source_id"])] = rec["profile"]
        try:
            for _i, _pid in enumerate(PORTFOLIO_IDS):
                _name = PROFILE_NAMES[_i] if _i < len(PROFILE_NAMES) else PROFILE_NAMES[-1]
                for _bid in source_api.fetch_closed_base_ids(_name, _pid, pages=2):
                    idmap.setdefault(str(_bid), _name)
        except Exception:
            pass
        data = _ms_build(lines, recs, idmap, current_open, coin_to_symbol)
        _MIRROR_STATS["data"] = data
        _MIRROR_STATS["ts"] = _t.time()
        return data
    except Exception:
        return _MIRROR_STATS["data"] or {"profiles": {}, "cumulative_pnl": None}


def fetch_merged_books(profile: str) -> Any:
    """Fetch and merge the books of every portfolio bound to this subaccount.
    COPY_PORTFOLIO_ID may be a comma-separated list (owner 2026-10-13: luxora
    Treasury + Odyssey on one subaccount). Source investment ids are UUIDs and
    never collide across portfolios; each position is tagged with its portfolio
    short-id for status/log clarity."""
    positions: List[Dict[str, Any]] = []
    equity = 0.0
    remaining = 0.0
    degraded: List[str] = []
    for i, pid in enumerate(PORTFOLIO_IDS):
        name = PROFILE_NAMES[i] if i < len(PROFILE_NAMES) else PROFILE_NAMES[-1]
        try:
            book = source_api.fetch_source_book(name, pid)
        except Exception as e:
            degraded.append(pid)
            log_action(f"LOG {name} book degraded ({pid[:8]}): {type(e).__name__}: {str(e)[:90]}; "
                       f"portfolio frozen this poll, other portfolios continue")
            continue
        for p in book.get("positions", []):
            p["portfolio"] = pid[:8]
            p["profile"] = name
            positions.append(p)
        equity += float(book.get("source_equity") or 0)
        remaining += float(book.get("remaining_sim") or 0)
    if degraded and len(degraded) == len(PORTFOLIO_IDS):
        raise SourceDataError("all bound portfolios degraded")
    return {"positions": positions, "source_equity": equity,
            "remaining_sim": remaining}, degraded


# ---------------------------------------------------------------- poll cycle

def fetch_closed_map() -> Dict[str, Dict[str, Any]]:
    """Closed-book map across every portfolio bound to this subaccount
    (owner 2026-10-18). Returns {original_open_investment_id:
    {ticker, closedAt, reason}}. Involio closed records reference the original
    open investment id in baseId."""
    merged: Dict[str, Dict[str, Any]] = {}
    # indexed loop (owner 2026-10-19): a profile may bind MULTIPLE portfolios
    # (luxora Treasury + Odyssey); zip() silently dropped extras.
    for i, pid in enumerate(PORTFOLIO_IDS):
        name = PROFILE_NAMES[i] if i < len(PROFILE_NAMES) else PROFILE_NAMES[-1]
        merged.update(source_api.fetch_closed_base_ids(name, pid, pages=2))
    return merged


def close_confirmed_at_market(client: BybitClient, rec_key: str,
                              rec: Dict[str, Any], info: Dict[str, Any]) -> str:
    """Owner 2026-10-18: the source trade is CONFIRMED CLOSED on Involio (its id
    appears in the closed book). 100% mirror: close our mirror at market regardless
    of P/L, winning or losing. Retention is reserved for positions absent from BOTH
    the open and closed books (genuinely lost)."""
    key = rec_key
    symbol, side = rec["symbol"], rec["side"]
    try:
        live = [lp for lp in client.get_positions(symbol)
                if lp.get("side") == ("Buy" if side == "long" else "Sell")
                and float(lp.get("size") or 0) > 0]
        if not live:
            return (f"CLOSED {key} 100% follow: source confirmed closed on Involio "
                    f"(reason={info.get('reason')}); position already flat; deregistered")
        lp = live[0]
        qty = round_qty(min(float(lp["size"]), float(rec["qty"]) or float(lp["size"])),
                        client.get_instrument(symbol))
        place_order_mode_safe(client, symbol, close_side(side), qty,
                              position_idx=position_idx(symbol, side), reduce_only=True,
                              order_link_id=order_link(key, "confclose", int(rec.get("gen", 0))))
        return (f"CLOSED {key} 100% follow: source confirmed closed on Involio "
                f"(reason={info.get('reason')}, closedAt={str(info.get('closedAt'))[:19]}) "
                f"-> mirror closed at market (loss followed, no retention)")
    except BybitError as e:
        return f"ERROR {key} confirmed-close: {e} - will retry next poll"


def poll_once() -> None:
    state = state_get()
    client = BybitClient()
    errors = 0
    try:
        book, degraded_pids = fetch_merged_books(PROFILE)
    except Exception as e:
        state["errors_last_run"] = state.get("errors_last_run", 0) + 1
        log_action(f"ERROR source fetch: {type(e).__name__}: {e}; retry next poll")
        return  # never continue without a book (was an UnboundLocalError crash)

    # --- owner 2026-10-21: margin monitor: instant 70% WhatsApp alert + entry hold ---
    try:
        if not (DRY_RUN or not client.configured):
            from margin_alert import check as _margin_check, margin_pct as _margin_pct
            _s = client.get_account_summary()
            _pos = [p for p in client.get_positions() if float(p.get("size") or 0) > 0]
            _wallet = float(_s.get("wallet_balance") or 0)
            _pct, _used = _margin_pct(_wallet, _pos)
            state["margin_pct"] = round(_pct, 2)
            _margin_check(NAME, _wallet, _pos,
                          os.environ.get("WEBHOOK_SHARED_SECRET", ""), log_action)
            # owner 2026-10-21: no NEW entries while margin >= 70%; entries
            # resume once margin is back to 50% or below. Existing mirrors keep
            # running unchanged (size syncs, stops, close-follow all active).
            if not state.get("margin_hold") and _pct >= 70.0:
                state["margin_hold"] = True
                log_action(f"LOG MARGIN-HOLD engaged: {_pct:.1f}% of wallet used; no new entries until <= 50%")
            elif state.get("margin_hold") and _pct <= 50.0:
                state["margin_hold"] = False
                log_action(f"LOG MARGIN-HOLD released: {_pct:.1f}% <= 50%; new entries allowed")
    except Exception as _e:
        log_action(f"LOG margin monitor failed ({type(_e).__name__}: {str(_e)[:60]})")

    # --- per-source dashboard stats (owner 2026-10-19); failure is non-fatal ---
    try:
        _stats: Dict[str, Any] = {}
        for _i, _pid in enumerate(PORTFOLIO_IDS):
            _name = PROFILE_NAMES[_i] if _i < len(PROFILE_NAMES) else PROFILE_NAMES[-1]
            _st = source_api.fetch_source_stats(_name, _pid)
            if _name in _stats:
                for _k in ("trades_24h", "closed_24h", "opened_24h", "total_trades",
                           "wins", "losses", "total_pnl", "pnl_24h", "open_now"):
                    _stats[_name][_k] = float(_stats[_name].get(_k) or 0) + float(_st.get(_k) or 0)
            else:
                _stats[_name] = _st
        state["source_stats"] = _stats
    except Exception as _e:
        log_action(f"LOG source-stats refresh failed ({type(_e).__name__}: {str(_e)[:80]})")
        save_state(state)
        return

    prev = state.get("books")
    positions = book.get("positions", [])
    by_id = {p["source_id"]: p for p in positions}
    # owner 2026-10-20: per-record source attribution for the dashboard. The
    # mirrored KEY carries the account-level profile string (which may be
    # comma-joined for multi-profile subaccounts); the RECORD now carries the
    # actual per-trade profile from the book it was mirrored from.
    for _rk, _rec in list(state.get("mirrored", {}).items()):
        if "profile" not in _rec:
            _p = by_id.get(str(_rk.split("|")[-1]))
            if _p is not None and _p.get("profile"):
                _rec["profile"] = _p["profile"]

    if prev is None:
        # First run: baseline everything; only FUTURE trades are mirrored.
        state["baseline"] = sorted(by_id.keys())
        state["books"] = book
        state["armed"] = True
        state["last_poll"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        save_state(state)
        log_action(f"BASELINE {NAME}: {len(by_id)} existing position(s) excluded; future new trades mirror (100% copy rules)")
        return

    prev_ids = {p["source_id"]: p for p in (prev.get("positions") or [])}

    if not (DRY_RUN or client.configured):
        log_action(f"ERROR poll: Bybit not configured but DRY_RUN=false - cannot manage account")
        state["errors_last_run"] = state.get("errors_last_run", 0) + 1
        save_state(state)
        return

    # --- new source trades ---
    for sid, p in by_id.items():
        if sid in prev_ids:
            continue
        if sid in state["baseline"]:
            blocks = state.setdefault("entry_blocks", {})
            key = tkey(p)
            if key not in blocks:
                blocks[key] = f"SKIP {key}: baseline position (never mirrored)"
                log_action(blocks[key])
            continue
        if DRY_RUN or not client.configured:
            roi = source_roi(p)
            log_action(f"DRY_RUN :: would process new {tkey(p)} (source ROI {roi*100:+.2f}%)" if roi is not None
                       else f"DRY_RUN :: would process new {tkey(p)}")
            continue
        action, rec = open_mirror(client, p, state)
        log_action(action)
        if action.startswith("ERROR"):
            errors += 1

    # --- closed source trades ---
    for sid, p in prev_ids.items():
        if sid in by_id:
            continue
        state["baseline"] = [b for b in state["baseline"] if b != sid]
        watch_keys = [k for k in state["skip_watch"] if k.split("|")[-1] == sid]
        for k in watch_keys:
            state["skip_watch"].pop(k, None)
            log_action(f"SKIP-WATCH-ENDED {k}: source closed while still above skip threshold; never mirrored")
        blocks = state.setdefault("entry_blocks", {})
        for k in [k for k in blocks if k.split("|")[-1] == sid]:
            del blocks[k]

    # --- source-close confirmation + source-lost rule (owner 2026-10-18) ---
    # When a mirrored trade vanishes from the open books, check Involio's CLOSED
    # book (closed records carry the original open investment id in baseId):
    #  - present there -> NORMAL source close: 100% mirror, close at market
    #    regardless of P/L (winning or losing). No retention.
    #  - absent from both books for 2 consecutive polls -> genuinely lost/hidden:
    #    profitable -> close at market; losing -> retained with breakeven TP
    #    trigger (the owner's only accepted retention case).
    missing = state.setdefault("source_missing", {})
    for sid in list(missing):
        if sid in by_id:
            missing.pop(sid, None)
    if not (DRY_RUN or not client.configured):
        degraded_names = {PROFILE_NAMES[i] if i < len(PROFILE_NAMES) else PROFILE_NAMES[-1]
                          for i, pid in enumerate(PORTFOLIO_IDS) if pid in (degraded_pids or [])}
        absent = [rk for rk in list(state["mirrored"].keys())
                  if rk.split("|")[-1] not in by_id
                  and (state["mirrored"][rk].get("profile") or "") not in degraded_names]
        closed_map = {}
        closed_map_ok = True
        if absent:
            try:
                closed_map = fetch_closed_map()
            except Exception as e:
                closed_map_ok = False
                log_action(f"LOG closed-book fetch failed ({type(e).__name__}: "
                           f"{str(e)[:100]}); waiting before close/retain decisions")
        for rec_key in absent:
            sid = rec_key.split("|")[-1]
            info = closed_map.get(sid)
            if info is not None:
                missing.pop(sid, None)
                action = close_confirmed_at_market(client, rec_key,
                                                   state["mirrored"][rec_key], info)
                log_action(action)
                if action.startswith("ERROR"):
                    errors += 1
                else:
                    state["mirrored"].pop(rec_key, None)
                continue
            if not closed_map_ok:
                continue  # decide only when the closed book is reachable
            missing[sid] = int(missing.get(sid, 0)) + 1
            if missing[sid] < 2:
                log_action(f"SOURCE-MISSING {rec_key}: source absent from open AND closed books "
                           f"(poll {missing[sid]}/2); awaiting confirmation")
                continue
            action = handle_source_lost(client, rec_key, state)
            log_action(action)
            if action.startswith("ERROR"):
                errors += 1
            else:
                missing.pop(sid, None)
                state["mirrored"].pop(rec_key, None)

    # --- re-evaluate watch list: auto re-entry when ROI drops below 2% ---
    for k, w in list(state["skip_watch"].items()):
        sid = k.split("|")[-1]
        p = by_id.get(sid)
        if p is None:
            state["skip_watch"].pop(k, None)
            continue
        roi = source_roi(p)
        if roi is None:
            continue
        w["roi"] = roi
        if roi < PROFIT_SKIP_ROI and not (DRY_RUN or not client.configured):
            action, rec = open_mirror(client, p, state)
            log_action(f"RE-ENTRY-CHECK {k}: source ROI now {roi*100:+.2f}% -> " + action)
            if action.startswith("OPENED"):
                state["skip_watch"].pop(k, None)
            elif action.startswith("ERROR"):
                errors += 1

    # --- follow live changes on mirrored trades ---
    for k, rec in list(state["mirrored"].items()):
        sid = k.split("|")[-1]
        p = by_id.get(sid)
        if p is None:
            continue
        if DRY_RUN or not client.configured:
            continue
        a = sync_size(client, p, rec)
        if a:
            log_action(a)
            if a.startswith("ERROR"):
                errors += 1
        b = sync_sltp(client, p, rec)
        if b:
            log_action(b)
            if b.startswith("ERROR"):
                errors += 1
        new_lev = min(int(float(p.get("leverage") or 1)) or 1, MAX_LEVERAGE)
        if new_lev != int(rec.get("leverage") or new_lev):
            try:
                client.set_leverage(rec["symbol"], new_lev)
                rec["leverage"] = new_lev
                log_action(f"SYNC {k} leverage={new_lev}")
            except BybitError as e:
                errors += 1
                log_action(f"ERROR {k} leverage update: {e}")

    # --- stale self-clean (owner 2026-10-12 directive) ---
    held_ids = set(by_id.keys())
    held_pairs = {(p["ticker"].upper(), p["side"].lower()) for p in positions}
    keep_manual = [m for m in state["manual"]
                   if len(m.split("/")) == 2 and (m.split("/")[0].upper(), m.split("/")[1].lower()) in held_pairs]
    if len(keep_manual) != len(state["manual"]):
        pruned_m = [m for m in state["manual"] if m not in keep_manual]
        state["manual"] = keep_manual
        log_action(f"LOG STALE-CLEAN: pruned {len(pruned_m)} manual entr(ies) not held by source: {pruned_m}")
    for k in [k for k in state.setdefault("entry_blocks", {}) if k.split("|")[-1] not in held_ids and k not in state["mirrored"]]:
        del state["entry_blocks"][k]

    # --- retained trailing (owner 2026-10-13): same protection as the main listener ---
    # Pending: exchange TP trigger at BE+/-0.5% guarantees a breakeven+ exit even if the
    # bot dies. Once price reaches the activation threshold the TP is cancelled and the
    # hard SL is ratcheted on the exchange, 0.5% behind the best price, never worse
    # than the fee-adjusted breakeven floor.
    retained = state.get("retained", {})
    if retained and client.configured and not DRY_RUN:
        for key, r in list(retained.items()):
            symbol, side = r.get("symbol"), r.get("side")
            try:
                live = [lp for lp in client.get_positions(symbol)
                        if lp.get("side") == ("Buy" if side == "long" else "Sell")
                        and float(lp.get("size") or 0) > 0]
                lp = live[0] if live else None
                if lp is None:
                    del retained[key]
                    log_action(f"RETAINED-EXITED {key}: exchange position gone "
                               f"(stop/TP filled or closed manually); deregistered")
                    continue
                cp = float(lp.get("markPrice") or 0)
                fee_be = float(r.get("fee_be") or 0)
                if cp <= 0 or fee_be <= 0:
                    continue
                inst = client.get_instrument(symbol)
                grid = Decimal(str(inst.get("priceFilter", {}).get("tickSize") or "0.01"))
                if r.get("status") != "active":
                    act = float(r.get("activation_threshold") or 0)
                    if act <= 0:
                        act = fee_be * ((1.0 + TP_TRIGGER_OFFSET) if side == "long" else (1.0 - TP_TRIGGER_OFFSET))
                        r["activation_threshold"] = act
                    if (cp >= act) if side == "long" else (cp <= act):
                        r["status"] = "active"
                        r["best_price"] = cp
                        if r.get("tp_trigger"):
                            try:
                                client.set_sl_tp(symbol, stop_loss=0, take_profit=0,
                                                 position_idx=position_idx(symbol, side))
                                log_action(f"TRAIL ACTIVATED {key}: price {cp} reached {act:.6f}; "
                                           f"cancelled TP trigger {r['tp_trigger']}; hard SL ratcheting begins")
                                r["tp_trigger"] = None
                            except BybitError as e:
                                errors += 1
                                log_action(f"ERROR {key} cancel tp_trigger: {e}")
                    else:
                        # self-heal: keep the exchange TP trigger in place while pending
                        if r.get("tp_trigger") is None:
                            tp = act
                            src_tp = float(r.get("source_tp") or 0)
                            if src_tp > 0:
                                tp = max(src_tp, tp) if side == "long" else min(src_tp, tp)
                            rounding = ROUND_FLOOR if side == "long" else ROUND_CEILING
                            tp = float((Decimal(str(tp)) / grid).to_integral_value(rounding=rounding) * grid)
                            client.set_sl_tp(symbol, stop_loss=0, take_profit=tp,
                                             position_idx=position_idx(symbol, side))
                            r["tp_trigger"] = tp
                            log_action(f"TRAIL TP-TRIGGER {key}: re-placed exchange TP trigger at {tp} "
                                       f"(BE floor {fee_be:.6f}); exits at breakeven+ even if bot dies")
                    if r.get("status") != "active":
                        continue
                if r.get("status") == "active":
                    if side == "long":
                        r["best_price"] = max(float(r.get("best_price") or cp), cp)
                        intended = max(fee_be, r["best_price"] * (1.0 - TRAILING_DIST_PCT))
                        rounding = ROUND_CEILING
                    else:
                        r["best_price"] = min(float(r.get("best_price") or cp), cp)
                        intended = min(fee_be, r["best_price"] * (1.0 + TRAILING_DIST_PCT))
                        rounding = ROUND_FLOOR
                    rounded_sl = float((Decimal(str(intended)) / grid).to_integral_value(rounding=rounding) * grid)
                    if (side == "long" and rounded_sl >= cp) or (side == "short" and rounded_sl <= cp):
                        if r.get("current_sl"):
                            log_action(f"LOG {key}: protective floor crossed before exchange update; "
                                       f"existing stop {r['current_sl']} still protects")
                        else:
                            errors += 1
                            log_action(f"ERROR {key}: intended protective floor crossed before exchange update "
                                       f"and no existing stop")
                        continue
                    prev_sl = r.get("current_sl")
                    should_update = (prev_sl is None or (side == "long" and rounded_sl > prev_sl)
                                      or (side == "short" and rounded_sl < prev_sl))
                    if should_update:
                        client.set_sl_tp(symbol, stop_loss=rounded_sl, take_profit=0,
                                         position_idx=position_idx(symbol, side))
                        r["current_sl"] = rounded_sl
                        log_action(f"TRAIL RATCHET {key}: hard SL -> {rounded_sl} "
                                   f"(best={r['best_price']}, floor={fee_be:.6f})")
            except BybitError as e:
                errors += 1
                log_action(f"ERROR {key} retained trailing: {e}")

    state["books"] = book
    state["last_poll"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state["errors_last_run"] = errors
    save_state(state)


def place_order_mode_safe(client, symbol, side, qty, position_idx, reduce_only=False, order_link_id=None):
    """place_order with one self-healing retry: if the account is in one-way
    mode (retCode 10001 'position idx not match position mode'), switch to
    hedge mode and retry once."""
    try:
        return client.place_order(symbol, side, qty, position_idx=position_idx,
                                  reduce_only=reduce_only, order_link_id=order_link_id)
    except BybitError as e:
        if "position mode" in str(e) or "10001" in str(e):
            client.ensure_hedge_mode()
            return client.place_order(symbol, side, qty, position_idx=position_idx,
                                      reduce_only=reduce_only, order_link_id=order_link_id)
        raise


def poll_loop_sync() -> None:
    # One-time setup: ensure the account is in hedge mode so positionIdx 1/2
    # orders are accepted (fresh subaccounts default to one-way mode).
    try:
        c = BybitClient()
        if c.configured and not DRY_RUN:
            if c.ensure_hedge_mode():
                log_action("SETUP ensured hedge position mode (positionIdx 1/2 ordering)")
    except Exception as e:
        log_action(f"ERROR ensure_hedge_mode: {type(e).__name__}: {e}")
    while True:
        try:
            poll_once()
        except Exception as e:
            log_action(f"ERROR poll_cycle: {type(e).__name__}: {e}")
        time.sleep(POLL_SECONDS)


app = FastAPI(title=f"100% Copy listener {NAME}")


@app.get("/status")
def status():
    state = state_get()
    client = BybitClient()
    bybit: Dict[str, Any] = {"configured": bool(client.configured), "positions": []}
    wallet = None
    if client.configured and not DRY_RUN:
        try:
            bybit["positions"] = [p for p in client.get_positions() if float(p.get("size") or 0) > 0]
            s = client.get_account_summary()
            wallet = s.get("wallet_balance")
        except BybitError as e:
            bybit["error"] = str(e)
    _ms = mirror_stats_payload()
    return {"ok": True, "code_version": APP_VERSION, "name": NAME, "profile": PROFILE,
            "portfolio_id": PORTFOLIO_ID, "dry_run": DRY_RUN, "armed": state.get("armed"),
            "mirror_stats": _ms, "cumulative_pnl": (_ms or {}).get("cumulative_pnl"),
            "margin_pct": state.get("margin_pct"), "margin_hold": state.get("margin_hold", False),
            "last_poll": state.get("last_poll"), "errors_last_run": state.get("errors_last_run"),
            "wallet": wallet, "bybit": bybit,
            "baseline_count": len(state.get("baseline", [])),
            "mirrored": state.get("mirrored", {}),
            "retained": state.get("retained", {}),
            "source_missing": state.get("source_missing", {}),
            "skip_watch": state.get("skip_watch", {}),
            "manual": state.get("manual", []),
            "source_stats": state.get("source_stats", {}),
            "log_tail": [l.strip() for l in log_tail(20)]}


@app.get("/log")
def log_endpoint(n: int = 400):
    return {"ok": True, "code_version": APP_VERSION,
            "lines": [l.strip() for l in log_tail(min(max(n, 1), 2000))]}


@app.post("/manual/close")
def manual_close(body: dict = Body(default=None), x_signature: str = Header(default="")):
    """Owner one-click close (dashboard 2026-10-28): close a mirrored or retained
    position at market, reduce-only, and deregister it from the engine.
    NOTE: load_state/save_state take STATE_LOCK internally (non-reentrant) - do
    not wrap this handler in the same lock."""
    secret = os.environ.get("WEBHOOK_SHARED_SECRET", "")
    if not secret:
        raise HTTPException(500, "listener secret not configured")
    if not hmac.compare_digest(x_signature, secret):
        raise HTTPException(403, "bad signature")
    body = body or {}
    symbol = str(body.get("symbol") or "").strip()
    side = str(body.get("side") or "").strip()
    if not symbol or side not in ("long", "short"):
        return {"ok": False, "error": "symbol and side required"}
    if DRY_RUN:
        return {"ok": False, "error": "engine in dry-run"}
    actions = []
    state = state_get()
    client = BybitClient()
    if not client.configured:
        return {"ok": False, "error": "Bybit API not configured"}
    for k in [k for k, r in (state.get("mirrored") or {}).items()
              if r.get("symbol") == symbol and r.get("side") == side]:
        info = {"reason": "owner_one_click", "closedAt": datetime.now(timezone.utc).isoformat()}
        line = close_confirmed_at_market(client, k, state["mirrored"][k], info)
        log_action(line)
        actions.append(line)
        if not line.startswith("ERROR"):
            state["mirrored"].pop(k, None)
    for k in [k for k, r in (state.get("retained") or {}).items()
              if r.get("symbol") == symbol and r.get("side") == side]:
        r = state["retained"][k]
        try:
            live = [lp for lp in client.get_positions(symbol)
                    if lp.get("side") == ("Buy" if side == "long" else "Sell")
                    and float(lp.get("size") or 0) > 0]
            if live:
                qty = float(live[0]["size"])
                inst = client.get_instrument(symbol)
                q = round_qty(min(qty, float(r.get("qty") or qty)), inst)
                place_order_mode_safe(client, symbol, close_side(side), q,
                                      position_idx=position_idx(symbol, side), reduce_only=True,
                                      order_link_id=order_link(k, "manualclose", int(time.time())))
            try:
                client.set_sl_tp(symbol, stop_loss=0, take_profit=0,
                                 position_idx=position_idx(symbol, side))
            except BybitError:
                pass
            del state["retained"][k]
            line = f"CLOSED {k} owner one-click close (retained position closed at market)"
            log_action(line)
            actions.append(line)
        except BybitError as e:
            line = f"ERROR {k} owner one-click close: {e}"
            log_action(line)
            actions.append(line)
    save_state(state)
    if not actions:
        return {"ok": False, "error": f"no mirrored/retained position found for {symbol} {side}"}
    ok = not any(a.startswith("ERROR") for a in actions)
    return {"ok": ok, "actions": actions}

POLL_TASK = None


@app.on_event("startup")
async def start_poll():
    global POLL_TASK
    threading.Thread(target=poll_loop_sync, name="copy-poll", daemon=True).start()


@app.on_event("shutdown")
async def stop_poll():
    if POLL_TASK:
        POLL_TASK.cancel()
