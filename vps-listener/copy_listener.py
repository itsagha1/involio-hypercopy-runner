"""
100% COPY listener: one instance per Bybit subaccount, bound to ONE Involio profile.

Owner spec 2026-10-12 (separate from the protected-mirror main listener):
- Full copy of the bound profile: sizes, adds/reductions, SL/TP, leverage synced.
- Source close is ALWAYS followed at market, including losses (no retention/trailing).
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
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).with_name(".env"), override=False)
except (ImportError, PermissionError):
    pass

from fastapi import FastAPI
from starlette.concurrency import run_in_threadpool

import source_api
import bybit_api
from bybit_api import (BybitClient, BybitError, bybit_side, close_side,
                       coin_to_symbol, position_idx, round_qty, symbol_to_coin)

APP_VERSION = "c1.0.0"
PROFILE = os.environ.get("COPY_PROFILE", "")
PORTFOLIO_ID = os.environ.get("COPY_PORTFOLIO_ID", "")
PORT = int(os.environ.get("COPY_PORT", "8001"))
NAME = os.environ.get("COPY_NAME", PROFILE or "copy")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "300"))
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"
STATE_FILE = os.environ.get("COPY_STATE_FILE", f"/var/lib/hypercopy-copy/{NAME}_state.json")
LOG_FILE = os.environ.get("COPY_LOG_FILE", f"/var/lib/hypercopy-copy/{NAME}_actions.log")
STATE_LOCK = __import__("threading").Lock()

PROFIT_SKIP_ROI = 0.02          # 2% profit-skip threshold (owner 2026-10-12)
MAX_LEVERAGE = 50
MIN_NOTIONAL = 5.0
MARGIN_CAP = 0.70               # guard: total margin cap of wallet (like main listener)
MAX_AVAIL_PER_TRADE = 0.90
LOG_TAIL_LINES = 400

# Bind the configured profile into the shared source adapter (validated on every fetch).
if PROFILE and PORTFOLIO_ID:
    source_api.PROFILES[PROFILE] = PORTFOLIO_ID


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
                                      ("last_poll", None), ("errors_last_run", 0), ("armed", False)):
                    s.setdefault(key, default)
                return s
            except (json.JSONDecodeError, OSError):
                pass
    return {"version": APP_VERSION, "baseline": [], "mirrored": {}, "skip_watch": {},
            "manual": [], "entry_blocks": {}, "books": None, "last_poll": None,
            "errors_last_run": 0, "armed": False}


def save_state(state: Dict[str, Any]) -> None:
    with STATE_LOCK:
        Path(STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2, default=str)
        os.replace(tmp, STATE_FILE)


def tkey(p: Dict[str, Any]) -> str:
    return f"{PROFILE}|{p['ticker']}/{p['side']}|{p['source_id']}"


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
        fill = client.place_order(symbol, bybit_side(side), qty,
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
            avail = float(client.get_available_balance() or 0)
            lev = int(rec.get("leverage") or 1) or 1
            if delta * float(client.get_ticker(symbol)["lastPrice"]) / lev > avail * MAX_AVAIL_PER_TRADE:
                rec["desired_source_qty"] = curr_sq
                return f"SKIP {key} size_add: insufficient available balance - target kept for retry"
            client.place_order(symbol, bybit_side(side), delta, position_idx=idx,
                               order_link_id=order_link(key, "add", int(rec.get("gen", 0))))
            rec["qty"] = rec["qty"] + delta
            action = f"ADDED {key} 100% follow: +{delta} (now {rec['qty']}, source qty {curr_sq:.6g})"
        else:
            delta = round_qty(rec["qty"] * (1.0 - ratio), inst)
            if delta <= 0:
                rec["last_applied_source_qty"] = curr_sq
                rec["desired_source_qty"] = curr_sq
                return None
            client.place_order(symbol, close_side(side), delta, position_idx=idx, reduce_only=True,
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
        client.place_order(symbol, close_side(side), qty, position_idx=position_idx(symbol, side),
                           reduce_only=True, order_link_id=order_link(key, "close", int(rec.get("gen", 0))))
        return (f"CLOSED {key} 100% follow: source closed"
                f"{' at source ROI %+.2f%%' % (roi * 100) if roi is not None else ''} "
                f"- mirror closed at market (loss followed, no retention)")
    except BybitError as e:
        return f"ERROR {key} follow_source_close: {e} - will retry next poll"


_GLOBAL_STATE: Dict[str, Any] = {"state": load_state()}


def state_get() -> Dict[str, Any]:
    return _GLOBAL_STATE["state"]


# ---------------------------------------------------------------- poll cycle

def poll_once() -> None:
    state = state_get()
    client = BybitClient()
    errors = 0
    try:
        book = source_api.fetch_source_book(PROFILE)
    except Exception as e:
        state["errors_last_run"] = state.get("errors_last_run", 0) + 1
        log_action(f"ERROR source fetch: {type(e).__name__}: {e}; retry next poll")
        save_state(state)
        return

    prev = state.get("books")
    positions = book.get("positions", [])
    by_id = {p["source_id"]: p for p in positions}

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
        if DRY_RUN or not client.configured:
            continue
        rec_key = next((k for k in state["mirrored"] if k.split("|")[-1] == sid), None)
        if rec_key:
            action = follow_source_close(client, prev_ids[sid], state["mirrored"][rec_key])
            log_action(action)
            if action.startswith("ERROR"):
                errors += 1
            else:
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

    state["books"] = book
    state["last_poll"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state["errors_last_run"] = errors
    save_state(state)


def poll_loop_sync() -> None:
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
            wallet = s.get("wallet")
        except BybitError as e:
            bybit["error"] = str(e)
    return {"ok": True, "code_version": APP_VERSION, "name": NAME, "profile": PROFILE,
            "portfolio_id": PORTFOLIO_ID, "dry_run": DRY_RUN, "armed": state.get("armed"),
            "last_poll": state.get("last_poll"), "errors_last_run": state.get("errors_last_run"),
            "wallet": wallet, "bybit": bybit,
            "baseline_count": len(state.get("baseline", [])),
            "mirrored": state.get("mirrored", {}),
            "skip_watch": state.get("skip_watch", {}),
            "manual": state.get("manual", []),
            "log_tail": [l.strip() for l in log_tail(20)]}


@app.get("/log")
def log_endpoint(n: int = 400):
    return {"ok": True, "code_version": APP_VERSION,
            "lines": [l.strip() for l in log_tail(min(max(n, 1), 2000))]}


POLL_TASK = None


@app.on_event("startup")
async def start_poll():
    global POLL_TASK
    threading.Thread(target=poll_loop_sync, name="copy-poll", daemon=True).start()


@app.on_event("shutdown")
async def stop_poll():
    if POLL_TASK:
        POLL_TASK.cancel()
