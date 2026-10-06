"""
Involio -> Bybit HyperCopy VPS listener v3.3 (CUTOVER FINAL DRAFT)

Receives webhook POSTs from the poller or Base44 function.
Sole authorized source profile for Bybit cutover: `booobsas` (app.invoapp.com/booobsas).
"""

from __future__ import annotations

import asyncio
import math
import time
import threading
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from starlette.concurrency import run_in_threadpool
import hmac
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).with_name(".env"), override=False)
except (ImportError, PermissionError):
    # systemd already loaded the root-only EnvironmentFile; do not weaken its permissions.
    pass

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from bybit_api import (BybitClient, BybitError, bybit_side, close_side,
                       coin_to_symbol, position_idx, round_qty, round_step,
                       symbol_to_coin)

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"
SHARED_SECRET = os.environ.get("WEBHOOK_SHARED_SECRET", "")
STATE_FILE = os.environ.get("STATE_FILE", "vps_state.json")
LOG_FILE = os.environ.get("LOG_FILE", "actions.log")

STATE_VERSION = 3
LISTENER_VERSION = "v3.6.1"
SOLE_SOURCE_PROFILE = "booobsas"  # Primary profile retained for compatibility.
AUTHORIZED_PROFILES = {"booobsas", "akira"}

PROFIT_SKIP_ROI = 0.03            # rule: skip new trades already >= +3% source ROI on margin
MIN_NOTIONAL = 5.0                # Bybit linear minimum order notional (USDT)
MARGIN_CAP = 1.0                  # owner budget: FULL current wallet balance (1.0)
FEE_BUFFER = 0.0012               # 0.12% roundtrip fee allowance for breakeven activation
ACTIVATION_OFFSET = 0.005        # +0.5% beyond fee-adjusted BE threshold for activation (owner 2026-10-02)
TRAILING_DIST_PCT = 0.005        # 0.5% price trailing distance (owner 2026-10-02)
PREPARE_MAX_AGE_SECONDS = 600    # 10 minutes max age for cutover arming after prepare
MISMATCH_MIN_USDT = 2.0          # orphan/mismatch reporting sensitivity
MISMATCH_QTY_FRAC = 0.05         # excess qty tolerance (5% of live qty)
LOG_TAIL_LINES = 60

STATE_LOCK = threading.RLock()
RISK_TASK = None

app = FastAPI(title="Bybit mirror v3.5: booobsas and akira Crypto")


class WebhookPayload(BaseModel):
    source: Optional[str] = ""
    fired_at: Optional[str] = ""
    recents: Optional[Dict[str, Any]] = {}
    books: Dict[str, Any]
    hold_new: Optional[bool] = True       # default draft mode: hold_new=True until armed
    cutover_action: Optional[str] = ""   # optional: 'prepare' or 'arm'


class CutoverConfigRequest(BaseModel):
    action: str                           # 'prepare' or 'arm'
    hold_new: Optional[bool] = None
    secret: Optional[str] = ""


# ---------------------------------------------------------------- helpers

def normalize_trader(trader: str) -> str:
    t = str(trader or "").lower()
    if t in ("booobsas", "onlybooobsas"):
        return SOLE_SOURCE_PROFILE
    return t


def get_shared_secret() -> str:
    return os.environ.get("WEBHOOK_SHARED_SECRET") or SHARED_SECRET


def log_action(line: str) -> None:
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    entry = f"[{stamp}] {line}"
    print(entry, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(entry + "\n")
    except OSError:
        pass


def log_tail(n: int = LOG_TAIL_LINES) -> List[str]:
    try:
        with open(LOG_FILE) as f:
            return f.read().splitlines()[-n:]
    except OSError:
        return []


def load_state() -> Dict[str, Any]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                s = json.load(f)
            s.setdefault("version", STATE_VERSION)
            s.setdefault("baseline", [])
            s.setdefault("mirrored", {})
            s.setdefault("retained_trailing", {})
            s.setdefault("books", {})
            s.setdefault("last_webhook", None)
            s.setdefault("fresh_start_at", None)
            s.setdefault("last_processed", None)
            s.setdefault("deltas_last_run", 0)
            s.setdefault("errors_last_run", 0)
            s.setdefault("manual", [])
            s.setdefault("manual_adopted", False)
            s.setdefault("orphans", {})
            s.setdefault("mismatches", {})
            s.setdefault("dereg_pending", [])
            s.setdefault("cutover_armed", False)
            s.setdefault("hold_new", True)
            s.setdefault("prepared_at", None)
            s.setdefault("prepared_profile", None)
            s.setdefault("parked_ambiguous", {})
            s.setdefault("entry_blocks", {})
            return s
        except (json.JSONDecodeError, OSError):
            pass
    return {"version": STATE_VERSION, "baseline": [], "mirrored": {},
            "retained_trailing": {}, "books": {}, "last_webhook": None,
            "fresh_start_at": None, "last_processed": None, "deltas_last_run": 0,
            "errors_last_run": 0, "manual": [], "manual_adopted": False,
            "orphans": {}, "mismatches": {}, "dereg_pending": [],
            "cutover_armed": False, "hold_new": True, "prepared_at": None,
            "prepared_profile": None, "parked_ambiguous": {}, "entry_blocks": {}}


def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_FILE)


def tkey(trader: str, p: Dict[str, Any]) -> str:
    trader_norm = normalize_trader(trader)
    ticker = p.get("ticker", "")
    side = str(p.get("side", "")).lower()
    sid = p.get("source_id", "")
    if sid:
        return f"{trader_norm}|{ticker}/{side}|{sid}"
    return f"{trader_norm}|{ticker}/{side}"


def baseline_id(trader,p):
    return normalize_trader(trader)+"|"+str(p.get("source_id") or tkey(trader,p))

def mirror_bybit_key(k: str) -> str:
    """'trader|COIN/side|source_id' -> 'COIN/side'."""
    parts = k.split("|")
    if len(parts) >= 2:
        return parts[1]
    return k


def parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def validate_booobsas_book(books: Dict[str, Any], snapshot_at_str: Optional[str] = None) -> Tuple[bool, str]:
    # Compatibility name. Every supplied book must belong to an authorized profile.
    if not isinstance(books,dict) or not books:return False,"REJECT: no source books"
    normalized=[normalize_trader(k) for k in books]
    if len(set(normalized))!=len(normalized) or any(k not in AUTHORIZED_PROFILES for k in normalized):
        return False,"REJECT: unauthorized or duplicate source profile"
    for key,book in books.items():
        valid,reason=validate_single_book({key:book},snapshot_at_str,normalize_trader(key))
        if not valid:return False,reason
    return True,""

def validate_single_book(books: Dict[str, Any], snapshot_at_str: Optional[str] = None, profile: str = SOLE_SOURCE_PROFILE) -> Tuple[bool, str]:
    """Strict contract validation for books['booobsas'].
    Fails closed if missing, unverified, incomplete, stale (>10min), or future (>30s).
    """
    if not isinstance(books, dict):
        return False, "REJECT: books payload is not a dict"
    
    target_key = None
    for k in books.keys():
        if normalize_trader(k) == profile:
            target_key = k
            break
            
    if len(books) != 1:
        return False, "REJECT: only one authorized source book is allowed"
    if not target_key:
        return False, f"REJECT: missing '{profile}' in books payload"
        
    book = books[target_key]
    if not isinstance(book, dict):
        return False, f"REJECT: '{profile}' book is not a dict"
    if book.get("complete") is not True:
        return False, f"REJECT: '{profile}' book complete flag is not True"
    if book.get("equity_verified") is not True:
        return False, f"REJECT: '{profile}' book equity_verified flag is not True"
    source_equity = float(book.get("source_equity") or 0)
    if not math.isfinite(source_equity) or source_equity <= 0:
        return False, f"REJECT: source_equity ({source_equity}) <= 0"

    snap_ts = book.get("snapshot_at") or snapshot_at_str
    if not snap_ts:
        return False, "REJECT: snapshot_at is missing"

    ts = parse_ts(str(snap_ts))
    if not ts or ts.tzinfo is None:
        return False, "REJECT: invalid snapshot_at timestamp format"

    now = datetime.now(timezone.utc)
    age = (now - ts).total_seconds()
    if age > PREPARE_MAX_AGE_SECONDS:  # 600s / 10min
        return False, f"REJECT: book snapshot is stale ({age:.0f}s > 10min)"
    if age < -30:                      # future >30s
        return False, f"REJECT: book snapshot timestamp is in the future ({age:.0f}s < -30s)"

    positions = book.get("positions")
    if not isinstance(positions, list):
        return False, "REJECT: positions is not a list"
    for p in positions:
        if not isinstance(p, dict):
            return False, "REJECT: position item in positions is not a dict"
        sm = float(p.get("source_margin") or 0)
        sq = float(p.get("source_qty") or 0)
        sid = str(p.get("source_id") or "")
        ep = float(p.get("entry_price") or 0)
        cp = float(p.get("current_price") or 0)
        lev = float(p.get("leverage") or 0)
        tk = str(p.get("ticker") or "")
        sd = str(p.get("side") or "").lower()
        if not all(math.isfinite(x) for x in (sm,sq,ep,cp,lev)) or sm <= 0 or sq <= 0 or not sid or ep <= 0 or cp <= 0 or lev < 1 or not lev.is_integer() or not tk or sd not in ("long", "short"):
            return False, f"REJECT: position {tk}/{sd} has incomplete or invalid contract fields"
    return True, ""


def price_pnl_ratio(p: Dict[str, Any]) -> float:
    """Calculates percentage price PnL (cp - ep)/ep for long, (ep - cp)/ep for short."""
    ep = float(p.get("entry_price") or 0)
    cp = float(p.get("current_price") or 0)
    if ep <= 0 or cp <= 0:
        return 0.0
    side = str(p.get("side", "long")).lower()
    if side == "long":
        return (cp - ep) / ep
    else:
        return (ep - cp) / ep


def source_margin_roi(p: Dict[str, Any]) -> float:
    """Calculates source return on MARGIN (ROI).
    Uses (last_sim / entry_sim - 1.0) if last_sim and entry_sim are present and valid (> 0).
    Alternatively uses price_pnl_ratio * leverage.
    """
    last_sim = p.get("last_sim")
    entry_sim = p.get("entry_sim")
    if last_sim is not None and entry_sim is not None:
        try:
            ls = float(last_sim)
            es = float(entry_sim)
            if es > 0:
                return (ls / es) - 1.0
        except (ValueError, TypeError):
            pass
    # Alternative: price_pnl_ratio * leverage
    pnl = price_pnl_ratio(p)
    lev = float(p.get("leverage") or 1.0) or 1.0
    return pnl * lev


def compute_desired_margin(p: Dict[str, Any], book_info: Dict[str, Any], owner_wallet: float) -> Tuple[float, str]:
    """Calculates desired margin for initial allocation.
    Uses verified API DECLARED entrySize / 100 provided p.source_allocation_pct and p.allocation_verified true.
    NOT inferred source equity. Keep book source_equity in diagnostic / complete check.
    """
    if p.get("allocation_verified") is True:
        alloc_pct_val = p.get("source_allocation_pct")
        if alloc_pct_val is None:
            alloc_pct_val = p.get("entrySize")
        if alloc_pct_val is not None:
            try:
                alloc_pct = float(alloc_pct_val)
                if math.isfinite(alloc_pct) and 0 < alloc_pct <= 100:
                    return owner_wallet * (alloc_pct / 100.0), ""
            except (ValueError, TypeError):
                pass

    return 0.0, "BLOCKED: verified declared allocation percentage is required; no inferred sizing fallback"


def profile_allocated_margin(state: Dict[str, Any]) -> float:
    """Margin currently allocated for mirrored positions of this profile."""
    tot = 0.0
    for rec in state.get("mirrored", {}).values():
        qty = float(rec.get("qty") or 0)
        ep = float(rec.get("entry_price") or 0)
        lev = float(rec.get("leverage") or 1) or 1.0
        tot += (qty * ep) / lev
    return tot


def margin_budget(client: BybitClient, state: Dict[str, Any]) -> float:
    """Extra USDT margin allowed under FULL wallet balance (MARGIN_CAP = 1.0)."""
    wallet = float(client.get_balance() or 0)
    used_by_profile = profile_allocated_margin(state)
    return max(0.0, MARGIN_CAP * wallet - used_by_profile)


def migrate_legacy_mirrors(state: Dict[str, Any]) -> List[str]:
    """Migrates mirrors from old profiles to manual (owner-managed) without placing orders."""
    mirrored = state.get("mirrored", {})
    manual = set(state.get("manual", []))
    migrated = []
    for k in list(mirrored.keys()):
        trader = normalize_trader(k.split("|")[0])
        if trader not in AUTHORIZED_PROFILES:
            bk = mirror_bybit_key(k)
            manual.add(bk)
            del mirrored[k]
            migrated.append(k)
    state["manual"] = sorted(list(manual))
    if migrated:
        log_action(f"CUTOVER MIGRATION: converted {len(migrated)} old profile mirror(s) to manual owner-managed: {migrated}")
    return migrated


def compute_deltas(prev_books: Dict[str, Any], books: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Diff book snapshots for SOLE_SOURCE_PROFILE using stable source_id."""
    deltas = []
    for raw_trader, book in books.items():
        trader = normalize_trader(raw_trader)
        if trader not in AUTHORIZED_PROFILES:
            continue
        prev_positions = {}
        for p in prev_books.get(raw_trader, {}).get("positions", []):
            sid = p.get("source_id") or f"{p.get('ticker')}/{p.get('side')}"
            prev_positions[sid] = p

        curr_positions = book.get("positions", [])
        curr_sids = set()
        for p in curr_positions:
            sid = p.get("source_id") or f"{p.get('ticker')}/{p.get('side')}"
            curr_sids.add(sid)
            old = prev_positions.get(sid)
            if old is None:
                deltas.append({"trader": trader, "type": "new_entry", "position": p})
            else:
                sq_n = float(p.get("source_qty") or 0)
                sq_p = float(old.get("source_qty") or 0)
                if sq_n != sq_p:
                    kind = "size_add" if sq_n > sq_p else "size_reduce"
                    deltas.append({"trader": trader, "type": kind, "position": p})
                elif (old.get("stop_loss") != p.get("stop_loss")
                      or old.get("price_target") != p.get("price_target")):
                    deltas.append({"trader": trader, "type": "sltp_change", "position": p})

        for sid, old in prev_positions.items():
            if sid not in curr_sids:
                deltas.append({"trader": trader, "type": "source_close", "position": old})

    return deltas


def fetch_open_mirrors(client: BybitClient) -> Dict[str, Dict[str, Any]]:
    mirrors = {}
    for pos in client.get_positions():
        if pos.get("size") in ("0", 0, 0.0, None, ""):
            continue
        side = "long" if pos.get("side") == "Buy" else "short"
        coin = symbol_to_coin(pos["symbol"])
        mirrors[coin + "/" + side] = pos
    return mirrors


def detect_unmanaged(state: Dict[str, Any], open_mirrors: Dict[str, Dict[str, Any]]):
    tracked: Dict[str, float] = {}
    for k, rec in state.get("mirrored", {}).items():
        bk = mirror_bybit_key(k)
        tracked[bk] = tracked.get(bk, 0.0) + float(rec.get("qty") or 0)
    manual = set(state.get("manual", []))
    orphans, mismatches = [], []
    for bk, pos in open_mirrors.items():
        if bk in manual:
            continue
        qty = float(pos.get("size") or 0)
        if bk not in tracked:
            orphans.append((bk, qty))
            continue
        expected = tracked[bk]
        excess = qty - expected
        price = float(pos.get("avgPrice") or 0)
        if excess > 0 and excess * price > max(MISMATCH_MIN_USDT,
                                               MISMATCH_QTY_FRAC * qty * price):
            mismatches.append((bk, qty, expected))
    return orphans, mismatches


# ---------------------------------------------------------------- actions

def order_link(key,operation,generation=0):
    return "BC"+hashlib.sha256((key+"|"+operation+"|"+str(generation)).encode()).hexdigest()[:32]

def open_mirror(client: BybitClient, trader: str, p: Dict[str, Any], book_info: Dict[str, Any], state: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Open 1:1 percentage margin allocation mirror for sole source booobsas."""
    trader_norm = normalize_trader(trader)
    key = tkey(trader_norm, p)
    coin, side = p["ticker"], str(p["side"]).lower()
    symbol = coin_to_symbol(coin)
    bk = f"{coin}/{side}"

    # Check if position is already recorded or parked ambiguous
    if key in state.get("mirrored", {}) or key in state.get("parked_ambiguous", {}):
        return f"LOG {key} new_entry: mirror already exists or parked ambiguous - repeated entry blocked", None

    # Check for manual conflict / protected position
    manual_set = set(state.get("manual", []))
    if bk in manual_set or (coin.upper() == "WLD" and side == "short"):
        if bk not in state.get("manual", []):
            state.setdefault("manual", []).append(bk)
        return f"SKIP {key} new_entry: manual protect active for {bk} (never manage owner manual positions)", None

    # Check for live position conflict on Bybit for same symbol/side
    try:
        live_positions = client.get_positions(symbol)
        for lp in live_positions:
            l_qty = float(lp.get("size") or 0)
            l_side = "long" if lp.get("side") == "Buy" else "short"
            if l_qty > 0:
                return f"SKIP {key} new_entry: owner live position conflict on Bybit for {symbol}/{side}", None
    except BybitError as e:
        return f"ERROR {key} new_entry: cannot verify live position ownership: {e}", None

    # Source ROI profit skip check (+3% on margin)
    s_roi = source_margin_roi(p)
    if s_roi >= PROFIT_SKIP_ROI:
        return (f"SKIP {key} new_entry: already +{s_roi * 100:.1f}% source ROI "
                f"(>= +3%) - too late to mirror"), None

    try:
        # Instrument status check (must be Trading, e.g. skip FETClosed)
        inst = client.get_instrument(symbol)
        if inst.get("status") != "Trading":
            return f"SKIP {key} new_entry: instrument status '{inst.get('status')}' is not Trading", None

        wallet = float(client.get_balance() or 0)
        avail = float(client.get_available_balance() or 0)
        budget = margin_budget(client, state)

        # Leverage check
        lev = int(float(p.get("leverage") or 1)) or 1
        lev_filter = inst.get("leverageFilter", {})
        min_lev = int(float(lev_filter.get("minLeverage", "1")))
        max_lev = int(float(lev_filter.get("maxLeverage", "100")))
        if not (min_lev <= lev <= max_lev):
            return f"SKIP {key} new_entry: leverage {lev}x outside exchange bounds [{min_lev}, {max_lev}]", None

        # Desired margin calculation
        desired_margin, err = compute_desired_margin(p, book_info, wallet)
        if err:
            return f"SKIP {key} new_entry: {err}", None

        desired_notional = desired_margin * lev
        min_notional = max(MIN_NOTIONAL, float(inst.get("lotSizeFilter", {}).get("minNotionalValue") or MIN_NOTIONAL))

        # Full desired allocation policy (NO silent shrinking)
        if desired_margin > budget:
            return (f"SKIP {key} new_entry: full desired margin ({desired_margin:.2f} USDT) "
                    f"exceeds remaining budget ({budget:.2f} USDT) - no silent shrinking"), None

        if desired_margin > avail:
            return (f"SKIP {key} new_entry: full desired margin ({desired_margin:.2f} USDT) "
                    f"exceeds available balance ({avail:.2f} USDT) - no silent shrinking"), None

        if desired_notional < min_notional:
            return (f"SKIP {key} new_entry: full desired notional ({desired_notional:.2f} USDT) "
                    f"below minimum order size ({MIN_NOTIONAL:.0f} USDT) - no silent shrinking"), None

        ticker = client.get_ticker(symbol)
        price = float(ticker["lastPrice"])

        sl=float(p.get("stop_loss") or 0);tp=float(p.get("price_target") or 0)
        if sl and ((side=="long" and sl>=price) or (side=="short" and sl<=price)):
            return f"SKIP {key}: source stop already crossed; exchange cannot place the mirrored protection",None
        if tp and ((side=="long" and tp<=price) or (side=="short" and tp>=price)):
            return f"SKIP {key}: source target already crossed; exchange cannot place the mirrored target",None

        their_qty = desired_notional / price
        lot_filter = inst.get("lotSizeFilter", {})
        min_qty = float(lot_filter.get("minOrderQty") or lot_filter.get("minQty") or "0")
        if min_qty and their_qty < min_qty:
            return f"SKIP {key} new_entry: exact mirror qty {their_qty:.10f} below exchange minimum qty {min_qty}; no oversizing", None
        qty = round_qty(their_qty, inst)

        max_qty = float(lot_filter.get("maxMktOrderQty") or lot_filter.get("maxOrderQty") or lot_filter.get("maxQty", "99999999"))
        if qty > max_qty:
            return f"SKIP {key} new_entry: calculated qty {qty} exceeds exchange maxQty {max_qty}", None

        notional_after_quant = qty * price
        if notional_after_quant < min_notional:
            return f"SKIP {key} new_entry: notional after quantization ({notional_after_quant:.2f} USDT) below minimum ({MIN_NOTIONAL:.0f} USDT)", None

        client.set_leverage(symbol, lev)
        state.setdefault("parked_ambiguous", {})[key] = {"reason":"order submission in progress","symbol":symbol,"order_link_id":order_link(key,"open"),"position":p,"requested_qty":qty,"leverage":lev}
        save_state(state)
        requested_qty=qty
        fill=client.place_order(symbol, bybit_side(side), qty,
                           position_idx=position_idx(symbol, side), order_link_id=order_link(key,"open"))
        qty=float(fill.get("filled_qty") or qty)
        price=float(fill.get("filled_avg_price") or price)
        if fill.get("partial_fill"):log_action(f"ERROR {key}: market order partially filled; stops applied to actual fill, remainder queued")

        # IMMEDIATELY create and record rec after confirmed API fill
        rec = {
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "source_id": p.get("source_id"),
            "last_applied_source_qty": float(p.get("source_qty") or 0)*qty/requested_qty,
            "desired_source_qty": float(p.get("source_qty") or 0),
            "notional": qty * price,
            "leverage": lev,
            "opened_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "entry_price": price,
            "price_target": p.get("price_target"),
            "stop_loss": p.get("stop_loss"),
        }

        state.setdefault("mirrored", {})[key] = rec
        state.setdefault("parked_ambiguous", {}).pop(key, None)
        save_state(state)

        # Attempt initial SL/TP if present
        if p.get("stop_loss") or p.get("price_target"):
            try:
                client.set_sl_tp(symbol, p.get("stop_loss"), p.get("price_target"),
                                position_idx=position_idx(symbol, side))
            except BybitError as e:
                rec["sltp_pending"] = True
                save_state(state)
                log_action(f"ERROR {key} initial_sltp_failed: {e} - position retained, will retry SL/TP on poll")

        return (f"OPENED {key} 1:1 equity mirror: {qty} {coin} "
                f"(~{qty * price:.2f} USDT notional, margin {desired_margin:.2f} USDT, lev {lev}x)"), rec

    except BybitError as e:
        log_action(f"ERROR {key} open_mirror: {e}")
        return f"ERROR {key} open_mirror: {e}", None


def sync_size(client: BybitClient, trader: str, p: Dict[str, Any], rec: Dict[str, Any], state: Dict[str, Any]) -> str:
    """Follow source size adds/reductions 1:1 based on stable source_qty."""
    key = tkey(trader, p)
    symbol, side = rec["symbol"], rec["side"]
    coin = symbol_to_coin(symbol)

    pending=rec.get("pending_resize")
    if pending and hasattr(client,"get_linked_order"):
        try:order=client.get_linked_order(symbol,pending["link"])
        except BybitError as e:return f"ERROR {key}: resize reconciliation unavailable: {e}"
        if not order or order.get("orderStatus") in ("New","PartiallyFilled"):
            return f"LOG {key}: prior resize outcome awaiting reconciliation; no replacement order"
        filled=float(order.get("cumExecQty") or 0)
        requested=float(pending.get("delta_qty") or 0)
        if requested<=0:return f"ERROR {key}: invalid resize intent; no replacement order"
        if order.get("orderStatus") not in ("Filled","Cancelled","PartiallyFilledCanceled","Rejected"):
            return f"ERROR {key}: unknown resize status; no replacement order"
        if filled>0:
            prev_sq=float(pending["prev_source_qty"])
            rec["last_applied_source_qty"]=prev_sq+(float(pending["target_source_qty"])-prev_sq)*filled/requested
            rec["qty"]=float(pending["prev_owner_qty"])+(filled if pending["direction"]=="add" else -filled)
        rec["resize_generation"]=int(rec.get("resize_generation",0))+1
        rec.pop("pending_resize",None)
        save_state(state)

    curr_sq = float(p.get("source_qty") or 0)
    last_sq = float(rec.get("last_applied_source_qty") or rec.get("desired_source_qty") or curr_sq)

    if curr_sq <= 0 or last_sq <= 0:
        return f"LOG {key} size: incomplete source_qty data, skipped sync"

    if curr_sq == last_sq:
        return f"LOG {key} size: source_qty unchanged ({curr_sq})"

    ratio = curr_sq / last_sq

    try:
        price = float(client.get_ticker(symbol)["lastPrice"])
        inst = client.get_instrument(symbol)
        idx = position_idx(symbol, side)

        if curr_sq > last_sq:  # Size add
            target_qty = rec["qty"] * ratio
            delta_qty = target_qty - rec["qty"]
            dqty = round_qty(delta_qty, inst)
            lev = int(rec.get("leverage") or 1) or 1
            need_margin = dqty * price / lev
            budget = margin_budget(client, state)
            avail = float(client.get_available_balance() or 0)

            if need_margin > budget or need_margin > avail or dqty * price < MIN_NOTIONAL:
                rec["desired_source_qty"] = curr_sq
                return (f"SKIP {key} size_add: add margin ({need_margin:.2f} USDT) "
                        f"exceeds budget ({budget:.2f} USDT) / available balance - target {curr_sq} kept for retry")

            link=order_link(key,"resize",int(rec.get("resize_generation",0)))
            rec["pending_resize"]={"link":link,"target_source_qty":curr_sq,"delta_qty":dqty,"prev_source_qty":last_sq,"prev_owner_qty":rec["qty"],"direction":"add"}
            save_state(state)
            filled=client.place_order(symbol, bybit_side(side), dqty, position_idx=idx,order_link_id=link)
            actual_dqty=float(filled.get("filled_qty") or dqty)
            applied_target=last_sq+(curr_sq-last_sq)*actual_dqty/dqty
            dqty=actual_dqty
            rec["resize_generation"]=int(rec.get("resize_generation",0))+1
            rec.pop("pending_resize",None)
            rec["qty"] = rec.get("qty", 0.0) + dqty
            rec["last_applied_source_qty"] = applied_target
            rec["desired_source_qty"] = curr_sq
            save_state(state)
            return (f"ADDED {key} 1:1 follow: +{dqty} {coin} "
                    f"(now {rec['qty']}, source qty {curr_sq})")

        else:  # Size reduction
            dqty = round_qty(rec["qty"] * (1.0 - ratio), inst)
            link=order_link(key,"resize",int(rec.get("resize_generation",0)))
            rec["pending_resize"]={"link":link,"target_source_qty":curr_sq,"delta_qty":dqty,"prev_source_qty":last_sq,"prev_owner_qty":rec["qty"],"direction":"reduce"}
            save_state(state)
            filled=client.place_order(symbol, close_side(side), dqty, position_idx=idx, reduce_only=True,order_link_id=link)
            actual_dqty=float(filled.get("filled_qty") or dqty)
            applied_target=last_sq+(curr_sq-last_sq)*actual_dqty/dqty
            dqty=actual_dqty
            rec["resize_generation"]=int(rec.get("resize_generation",0))+1
            rec.pop("pending_resize",None)
            rec["qty"] = max(0.0, rec.get("qty", 0.0) - dqty)
            rec["last_applied_source_qty"] = applied_target
            rec["desired_source_qty"] = curr_sq
            save_state(state)
            return (f"REDUCED {key} 1:1 follow: -{dqty} {coin} "
                    f"(now {rec['qty']}, source qty {curr_sq})")

    except BybitError as e:
        return f"{'SKIP' if 'too small' in str(e) else 'ERROR'} {key} sync_size: {e}"


def sync_sltp(client: BybitClient, trader: str, p: Dict[str, Any], rec: Dict[str, Any]) -> str:
    key = tkey(trader, p)
    sl, tp = p.get("stop_loss"), p.get("price_target")
    try:
        client.set_sl_tp(rec["symbol"], sl, tp,
                        position_idx=position_idx(rec["symbol"], rec["side"]))
        rec["stop_loss"] = sl
        rec["price_target"] = tp
        rec["sltp_pending"] = False
        return f"SYNC {key} sltp: SL={sl if sl else 'cleared'} TP={tp if tp else 'cleared'}"
    except BybitError as e:
        return f"ERROR {key} sync_sltp: {e}"


def execute_source_close(client: BybitClient, delta: Dict[str, Any], rec: Dict[str, Any],
                         open_mirrors: Dict[str, Dict[str, Any]], state: Dict[str, Any],
                         bybit_ok: bool = True) -> str:
    """Source close rule:
    1. Check OWNER net exit PnL after estimated fees (FEE_BUFFER = 0.0012) using live Bybit ticker price.
    2. If net positive after fees: market close immediately and remove mirror.
    3. Else: retain losing position, cancel ONLY bot-copied source SL (preserve TP), set fee-adjusted BE floor,
       and monitor trailing stop state in local 2-second manager.
    """
    p = delta["position"]
    key = tkey(delta["trader"], p)
    symbol, side = rec["symbol"], rec["side"]
    coin = symbol_to_coin(symbol)
    pos = open_mirrors.get(coin + "/" + side)

    if pos is None:
        if not bybit_ok:
            # Bybit position poll failed this cycle (proxy/API timeout): an empty
            # open_mirrors does NOT prove the position is gone. Keep the ownership
            # record; source_close_pending will retry the close on the next poll.
            return (f"SKIP {key} source_close: Bybit position poll unavailable - "
                    f"mirror record kept, will retry next cycle")
        if key in state.get("mirrored", {}):
            del state["mirrored"][key]
        if key in state.get("retained_trailing", {}):
            del state["retained_trailing"][key]
        save_state(state)
        return f"LOG {key} source_close: no live Bybit position (removed stale record)"

    if key not in state.get("mirrored",{}):
        return f"ERROR {key}: ownership record missing; no close or stop changes"
    idx = position_idx(symbol, side)
    try:
        avg = float(pos.get("avgPrice") or rec.get("entry_price") or 0)
        ticker = client.get_ticker(symbol)
        cp = float(ticker.get("lastPrice") or ticker.get("markPrice") or avg)

        # Gross price return
        gross_return = (cp - avg) / avg if side == "long" else (avg - cp) / avg
        estimated_be = avg * (1.0 + FEE_BUFFER) if side == "long" else avg * (1.0 - FEE_BUFFER)
        exchange_be = float(pos.get("breakEvenPrice") or estimated_be)
        fee_be = max(estimated_be, exchange_be) if side == "long" else min(estimated_be, exchange_be)
        net_return = (cp-fee_be)/avg if side == "long" else (fee_be-cp)/avg

        if net_return > 0:
            # Net positive exit after fees -> market close immediately
            qty = float(pos.get("size") or rec.get("qty") or 0)
            close_fill=client.place_order(symbol, close_side(side), qty, position_idx=idx, reduce_only=True,order_link_id=order_link(key,"close",int(rec.get("close_generation",0))))
            rec["close_generation"]=int(rec.get("close_generation",0))+1
            if close_fill.get("partial_fill"):
                rec["qty"]=max(0,float(pos.get("size") or rec.get("qty") or 0)-float(close_fill["filled_qty"]))
                save_state(state)
                return f"ERROR {key}: source-close order partially filled; remaining owned quantity preserved for retry"
            if key in state.get("mirrored", {}):
                del state["mirrored"][key]
            if key in state.get("retained_trailing", {}):
                del state["retained_trailing"][key]
            save_state(state)
            return (f"CLOSED {key} source_close: profitable owner exit net positive after fees "
                    f"(+{net_return * 100:.2f}%) -> market closed")

        # Retain losing position: cancel bot-copied source SL (preserve TP), configure BE floor trailing
        client.set_sl_tp(symbol, stop_loss=0, take_profit=rec.get("price_target"), position_idx=idx)

        act_thresh = fee_be * (1.0 + ACTIVATION_OFFSET) if side == "long" else fee_be * (1.0 - ACTIVATION_OFFSET)

        ret_info = {
            "key": key,
            "symbol": symbol,
            "side": side,
            "qty": float(pos.get("size") or rec.get("qty") or 0),
            "owner_entry_price": avg,
            "fee_be": fee_be,
            "activation_threshold": act_thresh,
            "status": "pending",
            "best_price": cp,
            "current_sl": None,
            "price_target": rec.get("price_target"),
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds")
        }

        state.setdefault("retained_trailing", {})[key] = ret_info
        save_state(state)

        return (f"TRAIL RETAINED {key} source_close: losing exit (net {net_return * 100:+.2f}%) "
                f"-> fee-aware BE floor trailing pending (act={act_thresh:.4f}, BE={fee_be:.4f})")

    except BybitError as e:
        return f"ERROR {key} source_close: {e}"


def manage_retained_trailing_stops(client: BybitClient, state: Dict[str, Any]) -> List[str]:
    """Local 2-second code-only manager for retained bot-owned positions.
    Ratchets exchange hard SL = max(BE, best*.99) long / min(BE, best*1.01) short.
    Pending until +0.5% beyond fee-adjusted BE threshold; 0.5% trailing distance.
    Avoids placing invalid SL while price is below floor.
    """
    logs = []
    retained_dict = state.get("retained_trailing", {})
    if not retained_dict:
        return logs

    open_mirrors = fetch_open_mirrors(client) if client.configured else {}

    for key in list(retained_dict.keys()):
        ret = retained_dict[key]
        symbol, side = ret["symbol"], ret["side"]
        if key not in state.get("mirrored", {}) or mirror_bybit_key(key) in state.get("manual", []):
            retained_dict.pop(key, None)
            continue
        coin = symbol_to_coin(symbol)
        pos = open_mirrors.get(coin + "/" + side)

        if pos is None and client.configured:
            del retained_dict[key]
            if key in state.get("mirrored", {}):
                del state["mirrored"][key]
            logs.append(f"CLOSED {key}: position no longer open on Bybit; ownership record removed")
            continue

        if pos is not None:
            expected = float(state.get("mirrored", {}).get(key, {}).get("qty") or ret.get("qty") or 0)
            actual = float(pos.get("size") or 0)
            if abs(actual-expected) > max(1e-9,expected*1e-6):
                state.setdefault("manual", []).append(coin+"/"+side)
                retained_dict.pop(key,None)
                state.get("mirrored",{}).pop(key,None)
                logs.append(f"ERROR {key}: position size changed outside bot; relinquished ownership without orders")
                continue
            exchange_be = float(pos.get("breakEvenPrice") or ret["fee_be"])
            ret["fee_be"] = max(ret["fee_be"],exchange_be) if side == "long" else min(ret["fee_be"],exchange_be)
            ret["activation_threshold"] = ret["fee_be"]*(1+ACTIVATION_OFFSET) if side=="long" else ret["fee_be"]*(1-ACTIVATION_OFFSET)
        try:
            ticker = client.get_ticker(symbol)
            cp = float(ticker.get("lastPrice") or ticker.get("markPrice") or 0)
            if cp <= 0:
                continue

            inst = client.get_instrument(symbol)
            tick_size = float(inst.get("priceFilter", {}).get("tickSize", "0.01"))

            # Check activation
            if ret.get("status") == "pending":
                activated = (cp >= ret["activation_threshold"]) if side == "long" else (cp <= ret["activation_threshold"])
                if activated:
                    ret["status"] = "active"
                    ret["best_price"] = cp
                    logs.append(f"TRAIL ACTIVATED BE FLOOR {key}: price {cp} reached activation threshold {ret['activation_threshold']}")
                else:
                    # Keep pending below threshold, do not place invalid SL
                    continue

            if ret.get("status") == "active":
                if side == "long":
                    ret["best_price"] = max(float(ret.get("best_price") or cp), cp)
                    trail_sl = ret["best_price"] * (1.0 - TRAILING_DIST_PCT)
                    intended_sl = max(ret["fee_be"], trail_sl)
                else:
                    ret["best_price"] = min(float(ret.get("best_price") or cp), cp)
                    trail_sl = ret["best_price"] * (1.0 + TRAILING_DIST_PCT)
                    intended_sl = min(ret["fee_be"], trail_sl)

                grid = Decimal(str(tick_size))
                rounding = ROUND_CEILING if side == "long" else ROUND_FLOOR
                rounded_sl = float((Decimal(str(intended_sl))/grid).to_integral_value(rounding=rounding)*grid)
                if (side=="long" and rounded_sl>=cp) or (side=="short" and rounded_sl<=cp):
                    logs.append(f"ERROR {key}: intended protective floor crossed before exchange update; existing stops preserved")
                    continue
                prev_sl = ret.get("current_sl")

                should_update = False
                if prev_sl is None:
                    should_update = True
                elif side == "long" and rounded_sl > prev_sl:
                    should_update = True
                elif side == "short" and rounded_sl < prev_sl:
                    should_update = True

                if should_update:
                    idx = position_idx(symbol, side)
                    client.set_sl_tp(symbol, stop_loss=rounded_sl, take_profit=ret.get("price_target"), position_idx=idx)
                    ret["current_sl"] = rounded_sl
                    ret["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    logs.append(f"LOG RATCHET SL {key}: updated hard SL to {rounded_sl} (best={ret['best_price']}, floor={ret['fee_be']})")

        except BybitError as e:
            logs.append(f"ERROR {key} manage_retained_trailing: {e}")

    save_state(state)
    return logs


# ---------------------------------------------------------------- endpoints

@app.post("/webhook/involio-delta")
async def involio_delta(payload: WebhookPayload, x_signature: str = Header(default="")):
    with STATE_LOCK:
        secret = get_shared_secret()
        if not secret:
            log_action("REJECT: WEBHOOK_SHARED_SECRET not set on VPS")
            raise HTTPException(500, "listener secret not configured")
        if not hmac.compare_digest(x_signature, secret):
            log_action("REJECT: bad signature")
            raise HTTPException(403, "bad signature")

        valid, err_msg = validate_booobsas_book(payload.books, payload.fired_at)
        if not valid:
            log_action(f"FAIL CLOSED: {err_msg} - preserving existing state")
            return {"ok": False, "error": err_msg, "fail_closed": True}

        state = load_state()

        # Automatic migration of old profile mirrors on payload processing
        migrate_legacy_mirrors(state)

        # Fresh start baseline capture for sole source profile booobsas
        if state.get("fresh_start_at") is None or (state.get("cutover_armed") is False and payload.cutover_action == "prepare"):
            target_key = SOLE_SOURCE_PROFILE
            for bk in payload.books.keys():
                if normalize_trader(bk) == SOLE_SOURCE_PROFILE:
                    target_key = bk
                    break
            baseline = [p.get("source_id") or tkey(SOLE_SOURCE_PROFILE, p)
                        for p in payload.books.get(target_key, {}).get("positions", [])]
            baseline=[baseline_id(trader,p) for trader,book in payload.books.items() for p in book.get("positions",[])]
            state["baseline"] = baseline
            state["fresh_start_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            state["books"] = payload.books
            state["last_webhook"] = payload.fired_at
            state["last_processed"] = state["fresh_start_at"]
            state["hold_new"] = True
            save_state(state)
            log_action(f"CUTOVER BASELINE RECORDED: {len(baseline)} existing '{SOLE_SOURCE_PROFILE}' "
                       f"position(s) will NOT be mirrored: {baseline}")
            return {"ok": True, "cutover_prepared": True, "baseline": len(baseline)}

        if payload.cutover_action == "arm":
            prep_at = parse_ts(state.get("prepared_at"))
            now = datetime.now(timezone.utc)
            if not prep_at or (now - prep_at).total_seconds() > PREPARE_MAX_AGE_SECONDS:
                log_action("REJECT ARM: cutover not properly prepared or prepared >10min ago")
                return {"ok": False, "error": "unsafe arm: cutover prepare expired or missing (>10min)"}
            state["cutover_armed"] = True
            state["hold_new"] = False
            log_action(f"CUTOVER ARMED: new mirrors enabled for sole source '{SOLE_SOURCE_PROFILE}'")

        if payload.hold_new is not None and state.get("hold_new") != bool(payload.hold_new):
            state["hold_new"] = bool(payload.hold_new)
            log_action(f"HOLD MODE {'ON' if state['hold_new'] else 'OFF'}: new mirrors "
                       f"{'blocked' if state['hold_new'] else 'resumed'}")

        prev_books = dict(state.get("books", {}))
        for trader,book in payload.books.items():
            if normalize_trader(trader) in AUTHORIZED_PROFILES and trader not in prev_books:
                added=[baseline_id(trader,p) for p in book.get("positions",[])]
                state["baseline"]=sorted(set(state.get("baseline",[])+added))
                prev_books[trader]=book
                log_action(f"PROFILE BASELINE {trader}: {len(added)} existing positions excluded; future new trades eligible")
        effective_books={**prev_books,**payload.books}
        deltas = compute_deltas(prev_books, payload.books)
        close_pending=state.setdefault("source_close_pending",{})
        live_source={tkey(trader,p) for trader,b in effective_books.items() for p in b.get("positions",[])}
        close_keys={tkey(d["trader"],d["position"]) for d in deltas if d["type"]=="source_close"}
        for d in deltas:
            if d["type"]=="source_close" and tkey(d["trader"],d["position"]) in state.get("mirrored",{}):
                close_pending[tkey(d["trader"],d["position"])]=d
        for k,d in list(close_pending.items()):
            if k not in state.get("mirrored",{}) or k in state.get("retained_trailing",{}):close_pending.pop(k,None)
            elif k not in live_source and k not in close_keys:deltas.append(d)
        save_state(state)
        # Retry/fix path: if a verified current source position is not baseline and has no mirror,
        # process it as a new entry on every fresh profile snapshot. This prevents a transient
        # exchange/API/mapping failure from consuming the source entry forever, while duplicate
        # orders remain blocked by mirrored/parked checks and idempotent order_link ids.
        existing_delta_keys={tkey(d["trader"],d["position"]) for d in deltas}
        for trader,book in payload.books.items():
            trader_norm=normalize_trader(trader)
            if trader_norm not in AUTHORIZED_PROFILES:continue
            for p in book.get("positions",[]):
                k=tkey(trader_norm,p);bid=baseline_id(trader_norm,p)
                if bid in state.get("baseline",[]) or (trader_norm==SOLE_SOURCE_PROFILE and p["source_id"] in state.get("baseline",[])) or k in state.get("mirrored",{}) or k in existing_delta_keys:
                    continue
                deltas.append({"trader":trader_norm,"type":"new_entry","position":p,"retry_current":True})
                existing_delta_keys.add(k)


        client = BybitClient()
        open_mirrors: Dict[str, Dict[str, Any]] = {}
        bybit_ok = False
        errors = 0
        if not DRY_RUN and client.configured:
            try:
                open_mirrors = fetch_open_mirrors(client)
                bybit_ok = True
                log_action(f"LIVE: {len(open_mirrors)} open Bybit position(s): {sorted(open_mirrors)}")
            except BybitError as e:
                errors += 1
                log_action(f"BYBIT POLL FAILED: {e}")

        # Process local retained trailing stops
        if bybit_ok or DRY_RUN:
            m_logs = manage_retained_trailing_stops(client, state)
            for ml in m_logs:
                log_action(ml)

        # Reconcile deregistration
        if bybit_ok:
            missing_now = {mirror_bybit_key(k) for k in state["mirrored"]
                           if mirror_bybit_key(k) not in open_mirrors}
            pending = set(state.get("dereg_pending", []))
            for k in list(state["mirrored"].keys()):
                if mirror_bybit_key(k) in missing_now and mirror_bybit_key(k) in pending:
                    log_action(f"DEREGISTER {k}: position missing in Bybit - leaving unmanaged")
                    del state["mirrored"][k]
            state["dereg_pending"] = sorted(missing_now)

        # Orphan & mismatch detection
        if bybit_ok:
            orphans, mismatches = detect_unmanaged(state, open_mirrors)
            if not state.get("manual_adopted"):
                for bk, qty in orphans:
                    if bk not in state["manual"]:
                        state["manual"].append(bk)
                    log_action(f"ADOPT-MANUAL {bk}: untracked position registered as owner-managed")
                state["manual_adopted"] = True

        if bybit_ok and not DRY_RUN:
            for trader,b in payload.books.items():
                for p in b.get("positions",[]):
                    k=tkey(trader,p);r=state.get("mirrored",{}).get(k)
                    if r and int(r.get("leverage") or p["leverage"])!=int(p["leverage"]):
                        try:
                            client.set_leverage(r["symbol"],int(p["leverage"]))
                            r["leverage"]=int(p["leverage"]);r.pop("leverage_blocked",None)
                            log_action(f"SYNC {k} source leverage={r['leverage']}")
                        except BybitError as e:
                            r["leverage_blocked"]=True;errors+=1;log_action(f"ERROR {k} leverage update: {e}")

        executed = 0
        for d in deltas:
            p = d["position"]
            trader_norm = normalize_trader(d["trader"])
            k = tkey(trader_norm, p)
            sid = p.get("source_id") or k
            t = d["type"]

            if trader_norm not in AUTHORIZED_PROFILES:
                log_action(f"SKIP {k} {t}: non-authorized profile (sole source is {SOLE_SOURCE_PROFILE})")
                continue

            bid=baseline_id(trader_norm,p)
            protected=bid if bid in state.get("baseline",[]) else (sid if trader_norm==SOLE_SOURCE_PROFILE and sid in state.get("baseline",[]) else None)
            if t == "source_close" and protected:
                state["baseline"].remove(protected)
                log_action(f"BASELINE-CLOSED {sid}: removed from baseline (re-opens will be mirrored as new trades)")
                continue

            if protected:
                log_action(f"SKIP {sid} {t}: baseline position")
                continue

            rec = state["mirrored"].get(k)

            if t == "new_entry":
                if state.get("hold_new", True) or not state.get("cutover_armed", False):
                    log_action(f"SKIP {k} new_entry: cutover not armed or hold mode active (hold_new=True)")
                    continue
                if rec:
                    log_action(f"LOG {k} new_entry: mirror already exists")
                    continue

                target_bk_key = d["trader"]
                for bk in payload.books.keys():
                    if normalize_trader(bk) == trader_norm:
                        target_bk_key = bk
                        break
                book_info = payload.books.get(target_bk_key, {})

                if DRY_RUN or not client.configured:
                    s_roi = source_margin_roi(p)
                    log_action(f"DRY_RUN :: WOULD OPEN {k} (source ROI {s_roi*100:+.1f}%)")
                else:
                    action, new_rec = open_mirror(client, d["trader"], p, book_info, state)
                    if new_rec:
                        executed += 1
                        state.setdefault("entry_blocks", {}).pop(k, None)
                        log_action(action)
                    elif action.startswith("ERROR"):
                        errors += 1
                        log_action(action)
                    else:
                        blocks=state.setdefault("entry_blocks", {})
                        if blocks.get(k) != action:
                            blocks[k]=action
                            log_action(action)

            elif t in ("size_add", "size_reduce"):
                if rec and rec.get("leverage_blocked"):continue
                if not rec:
                    log_action(f"SKIP {k} {t}: no mirror of this position")
                    continue
                if DRY_RUN or not client.configured:
                    log_action(f"DRY_RUN :: WOULD SYNC size {k}")
                else:
                    action = sync_size(client, d["trader"], p, rec, state)
                    if action.startswith(("ADDED", "REDUCED")):
                        executed += 1
                    elif action.startswith("ERROR"):
                        errors += 1
                    log_action(action)

            elif t == "sltp_change":
                if not rec:
                    log_action(f"SKIP {k} sltp_change: no mirror of this position")
                    continue
                if DRY_RUN or not client.configured:
                    log_action(f"DRY_RUN :: WOULD SYNC sltp {k}")
                else:
                    action = sync_sltp(client, d["trader"], p, rec)
                    if action.startswith("SYNC"):
                        executed += 1
                    elif action.startswith("ERROR"):
                        errors += 1
                    log_action(action)

            elif t == "source_close":
                if not rec:
                    log_action(f"SKIP {k} source_close: no mirror of this position")
                    continue
                if DRY_RUN or not client.configured:
                    log_action(f"DRY_RUN :: WOULD CLOSE/TRAIL {k}")
                else:
                    action = execute_source_close(client, d, rec, open_mirrors, state, bybit_ok=bybit_ok)
                    if action.startswith(("CLOSED", "TRAIL RETAINED")):
                        executed += 1
                    elif action.startswith("ERROR"):
                        errors += 1
                    log_action(action)
                    if k not in state.get("mirrored",{}) or k in state.get("retained_trailing",{}):
                        state.get("source_close_pending",{}).pop(k,None)

        if bybit_ok and not DRY_RUN:
            for trader, book in payload.books.items():
                for p in book.get("positions", []):
                    k=tkey(trader,p); rec=state.get("mirrored",{}).get(k)
                    if not rec or k in state.get("retained_trailing",{}): continue
                    desired_qty=float(p["source_qty"])
                    if not rec.get("leverage_blocked") and (rec.get("pending_resize") or desired_qty != float(rec.get("last_applied_source_qty") or desired_qty)):
                        action=sync_size(client,trader,p,rec,state)
                        log_action(action)
                        if action.startswith("ERROR"): errors+=1
                    if rec.get("sltp_pending") or rec.get("stop_loss")!=p.get("stop_loss") or rec.get("price_target")!=p.get("price_target"):
                        action=sync_sltp(client,trader,p,rec)
                        log_action(action)
                        if action.startswith("ERROR"): errors+=1
                    new_lev=int(p["leverage"])
                    if new_lev != int(rec.get("leverage") or new_lev):
                        try:
                            client.set_leverage(rec["symbol"],new_lev)
                            rec["leverage"]=new_lev
                            log_action(f"SYNC {k} leverage={new_lev}")
                        except BybitError as e:
                            errors+=1;log_action(f"ERROR {k} leverage update: {e}")

        state["books"] = effective_books
        state["last_webhook"] = payload.fired_at
        state["last_processed"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        state["deltas_last_run"] = len(deltas)
        state["errors_last_run"] = errors
        save_state(state)
        return {"ok": True, "deltas": len(deltas), "executed": executed,
                "dry_run": DRY_RUN, "errors": errors}


@app.post("/cutover/config")
@app.post("/webhook/cutover")
async def cutover_config(req: CutoverConfigRequest, x_signature: str = Header(default="")):
    with STATE_LOCK:
        """Signed configuration route for cutover preparation and arming."""
        secret = get_shared_secret()
        if not secret:
            raise HTTPException(503,"listener secret not configured")
        if secret:
            valid_sig = x_signature and hmac.compare_digest(x_signature, secret)
            valid_secret = req.secret and hmac.compare_digest(req.secret, secret)
            if not (valid_sig or valid_secret):
                raise HTTPException(403, "invalid authentication signature or secret")

        state = load_state()
        action = str(req.action).lower()

        if action == "prepare":
            books = state.get("books", {})
            if books:
                valid, err_msg = validate_booobsas_book(books)
                if not valid:
                    raise HTTPException(400, f"cannot prepare: current book payload is invalid: {err_msg}")

            migrated = migrate_legacy_mirrors(state)
            client=BybitClient()
            if not DRY_RUN and client.configured:
                try:
                    live=fetch_open_mirrors(client)
                    owned={mirror_bybit_key(k) for k in state.get("mirrored",{})}
                    state["manual"]=sorted({"WLD/short"}|(set(live)-owned))
                    state["manual_adopted"]=True
                    state["orphans"]={};state["mismatches"]={}
                except BybitError as e:raise HTTPException(503,"Cannot verify manual positions before preparation")
            target_bk_key = SOLE_SOURCE_PROFILE
            for bk in books.keys():
                if normalize_trader(bk) == SOLE_SOURCE_PROFILE:
                    target_bk_key = bk
                    break
            baseline = [p.get("source_id") or tkey(SOLE_SOURCE_PROFILE, p)
                        for p in books.get(target_bk_key, {}).get("positions", [])]
            baseline=[baseline_id(trader,p) for trader,book in books.items() for p in book.get("positions",[])]
            state["baseline"] = sorted(list(set(state.get("baseline", []) + baseline)))
            state["hold_new"] = True
            state["cutover_armed"] = False
            now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
            state["fresh_start_at"] = now_iso
            state["prepared_at"] = now_iso
            state["prepared_profile"] = SOLE_SOURCE_PROFILE
            save_state(state)
            log_action(f"CUTOVER CONFIG PREPARE: migrated {len(migrated)} old mirrors, baselined {len(baseline)} booobsas positions, hold_new=True")
            return {"ok": True, "action": "prepare", "migrated_legacy": len(migrated), "baseline_count": len(state["baseline"]), "hold_new": True, "armed": False}

        elif action == "arm":
            prep_profile = state.get("prepared_profile")
            prep_at = parse_ts(state.get("prepared_at"))
            now = datetime.now(timezone.utc)

            if prep_profile != SOLE_SOURCE_PROFILE or not prep_at or (now - prep_at).total_seconds() > PREPARE_MAX_AGE_SECONDS:
                raise HTTPException(400, "unsafe arm: cutover was not properly prepared for booobsas within 10 minutes")

            books = state.get("books", {})
            valid, err_msg = validate_booobsas_book(books)
            if not valid:
                raise HTTPException(400, f"cannot arm: book state invalid or stale: {err_msg}")

            state["cutover_armed"] = True
            state["hold_new"] = False if req.hold_new is None else bool(req.hold_new)
            save_state(state)
            log_action(f"CUTOVER CONFIG ARM: cutover_armed=True, hold_new={state['hold_new']}")
            return {"ok": True, "action": "arm", "armed": True, "hold_new": state["hold_new"]}

        else:
            raise HTTPException(400, f"unknown cutover action '{action}'")


@app.get("/health")
@app.get("/status")
def health():
    # Serve the risk loop's cached account snapshot. This endpoint never calls
    # Bybit directly, so a slow or flaky exchange proxy can never stall it.
    state=load_state();client=BybitClient()
    account=ACCOUNT_CACHE.get("account") or {};positions=ACCOUNT_CACHE.get("positions") or []
    age=time.time()-ACCOUNT_CACHE["ok_ts"] if ACCOUNT_CACHE.get("ok_ts") else None
    # Surface an exchange error only when it persisted past the retry window,
    # never on a single transient blip.
    error=None
    if ACCOUNT_CACHE.get("error") and (not ACCOUNT_CACHE.get("ok_ts") or time.time()-ACCOUNT_CACHE["ok_ts"]>300):
        error=ACCOUNT_CACHE["error"]
    wallet=account.get("wallet_balance");avail=account.get("available_balance")
    return {"ok":error is None,"status":"ok" if error is None else "error",
            "code_version":LISTENER_VERSION,"version":LISTENER_VERSION,"mode":"multi_profile_full_balance",
            "sole_source_profile":None,"authorized_profiles":sorted(AUTHORIZED_PROFILES),"dry_run":DRY_RUN,"bybit_keys":"set" if client.configured else "missing",
            "proxy":"set" if os.environ.get("BYBIT_PROXY") else "missing","bybit_configured":client.configured,
            "wallet_balance":wallet,"available_balance":avail,"bybit":{"balance":account.get("margin_balance"),"positions":positions,"age_seconds":(round(age) if age is not None else None),**({"error":error} if error else {})},
            "margin":{"wallet":wallet,"available":avail,"cap_ratio":MARGIN_CAP,"budget":max(0,(wallet or 0)-profile_allocated_margin(state)),
                      "used":profile_allocated_margin(state),"cap_pct_used":100*profile_allocated_margin(state)/wallet if wallet else 0,
                      "margin_balance":account.get("margin_balance"),"unrealised_pnl":account.get("unrealised_pnl")},
            "cutover_armed":state.get("cutover_armed",False),"hold_new":state.get("hold_new",True),
            "baseline_count":len(state.get("baseline",[])),"baseline_positions":len(state.get("baseline",[])),
            "mirrored":state.get("mirrored",{}),"manual":state.get("manual",[]),"orphans":state.get("orphans",{}),"mismatches":state.get("mismatches",{}),
            "retained":[{"key":k,"symbol":r.get("symbol"),"side":r.get("side"),"qty":r.get("qty"),"status":r.get("status"),
                "fee_be":r.get("fee_be"),"activation_threshold":r.get("activation_threshold"),"best_price":r.get("best_price"),
                "current_sl":r.get("current_sl"),"retained_at":r.get("retained_at")}
                for k,r in state.get("retained_trailing",{}).items()],
            "retained_trailing":state.get("retained_trailing",{}),"last_webhook":state.get("last_webhook"),"last_processed":state.get("last_processed"),
            "books":{k:len(v.get("positions",[])) for k,v in state.get("books",{}).items()},"log_tail":log_tail(),
            "errors_last_run":state.get("errors_last_run",0),"risk_manager_alive":bool(RISK_TASK and not RISK_TASK.done()),"risk_interval_seconds":2}

@app.get("/log")
def log_endpoint(n:int=400):
    return {"ok":True,"code_version":LISTENER_VERSION,"lines":log_tail(min(max(n,1),2000))}

@app.get("/source/snapshot")
async def source_snapshot(profile:str=SOLE_SOURCE_PROFILE,x_signature:str=Header(default="")):
    secret=get_shared_secret()
    if not secret or not hmac.compare_digest(x_signature,secret):raise HTTPException(403,"bad signature")
    if profile not in AUTHORIZED_PROFILES:raise HTTPException(400,"Unauthorized source profile")
    from source_api import fetch_source_book, SourceDataError
    book=None
    try:
        for attempt in range(3):
            try:
                book=await run_in_threadpool(fetch_source_book,profile)
                break
            except SourceDataError:
                # Transient source-side inconsistency (counts/pages changing mid-fetch).
                # Fresh full refetch after a short pause; only give up after the third attempt.
                if attempt==2:raise
                await asyncio.sleep(2+attempt*2)
        return {"ok":True,"book":book}
    except Exception as e:
        state=load_state()
        reason=type(e).__name__
        prefix="ERROR" if state.get("mirrored") or "credential" in str(e).lower() else "LOG"
        log_action(f"{prefix} {profile} source snapshot unavailable: {reason}; no closures inferred")
        raise HTTPException(503,"Source snapshot unavailable; no book forwarded")

def recover_parked_entries(client,state):
    for key,intent in list(state.get("parked_ambiguous",{}).items()):
        link=intent.get("order_link_id")
        if not link:continue
        try:
            order=client.get_linked_order(intent["symbol"],link)
        except BybitError as e:
            log_action(f"WARN {key}: parked recovery API error ({e}); retry next tick")
            continue
        if not order or order.get("orderStatus") not in ("Filled","Cancelled","PartiallyFilledCanceled","Rejected"):
            pos=intent.get("position",{});created=pos.get("created_at","")
            if created:
                try:
                    import datetime as _dt
                    ct=_dt.datetime.fromisoformat(created.replace("Z","+00:00"))
                    if (_dt.datetime.now(_dt.timezone.utc)-ct).total_seconds()>86400:
                        state.setdefault("baseline",[]).append(baseline_id(key.split("|")[0],pos))
                        state["parked_ambiguous"].pop(key,None)
                        log_action(f"LOG {key}: parked entry stale (>24h, order not found); cleared")
                        continue
                except Exception: pass
            continue
        filled=float(order.get("cumExecQty") or 0)
        p=intent["position"];bk=p["ticker"]+"/"+p["side"]
        if filled<=0:
            state.setdefault("baseline",[]).append(baseline_id(key.split("|")[0],p))
            state["parked_ambiguous"].pop(key,None)
            continue
        live=fetch_open_mirrors(client);position=live.get(bk)
        if position is None:
            state.setdefault("baseline",[]).append(baseline_id(key.split("|")[0],p))
            state["parked_ambiguous"].pop(key,None)
            log_action(f"LOG {key}: submitted trade already closed; not reopening")
            continue
        if bk in state.get("manual",[]) or abs(float(position["size"])-filled)>max(1e-9,filled*1e-8):
            log_action(f"ERROR {key}: linked fill overlaps changed/manual position; no protective orders changed")
            continue
        rec={"symbol":intent["symbol"],"side":p["side"],"qty":filled,"source_id":p["source_id"],
             "last_applied_source_qty":float(p["source_qty"])*filled/float(intent["requested_qty"]),
             "desired_source_qty":float(p["source_qty"]),"leverage":int(intent["leverage"]),
             "entry_price":float(position.get("avgPrice") or p["entry_price"]),"sltp_pending":True}
        state.setdefault("mirrored",{})[key]=rec
        state["parked_ambiguous"].pop(key,None)
        state.get("orphans",{}).pop(bk,None)
        current=next((x for b in state.get("books",{}).values() for x in b.get("positions",[]) if x.get("source_id")==p["source_id"]),None)
        save_state(state)
        if current:log_action(sync_sltp(client,key.split("|")[0],current,rec))
        else:
            d={"trader":key.split("|")[0],"type":"source_close","position":p}
            state.setdefault("source_close_pending",{})[key]=d
            log_action(execute_source_close(client,d,rec,live,state))
        log_action(f"SYNC {key}: prior linked fill recovered and ownership confirmed")
    save_state(state)

# Cached Bybit account snapshot refreshed by the risk loop. The /status
# endpoint serves this cache so monitoring never blocks on a slow or flaky
# exchange proxy; a hung Bybit call can never stall health checks.
ACCOUNT_CACHE: Dict[str, Any] = {"ok_ts": 0.0, "attempt_ts": 0.0, "account": {}, "positions": [], "error": None}
ACCOUNT_REFRESH_S = 15.0
ACCOUNT_RETRY_S = 5.0

def refresh_account_cache(client: BybitClient) -> None:
    now=time.time()
    window=ACCOUNT_RETRY_S if ACCOUNT_CACHE["error"] else ACCOUNT_REFRESH_S
    if now-ACCOUNT_CACHE["attempt_ts"] < window:
        return
    ACCOUNT_CACHE["attempt_ts"]=now
    try:
        account=client.get_account_summary()
        positions=[p for p in client.get_positions() if float(p.get("size") or 0)>0]
        ACCOUNT_CACHE["account"]=account
        ACCOUNT_CACHE["positions"]=positions
        ACCOUNT_CACHE["error"]=None
        ACCOUNT_CACHE["ok_ts"]=now
    except BybitError as e:
        ACCOUNT_CACHE["error"]=str(e)

def risk_tick():
    client=BybitClient()
    if client.configured and not DRY_RUN:
        refresh_account_cache(client)
    with STATE_LOCK:
        state=load_state()
        if DRY_RUN or not (state.get("retained_trailing") or state.get("parked_ambiguous")):return
        if not client.configured:raise BybitError("Bybit credentials missing")
        if state.get("parked_ambiguous"):recover_parked_entries(client,state)
        for line in manage_retained_trailing_stops(client,state):log_action(line)

async def risk_loop():
    while True:
        try:await run_in_threadpool(risk_tick)
        except asyncio.CancelledError:raise
        except Exception as e:log_action(f"ERROR risk manager: {type(e).__name__}; exchange stops preserved")
        await asyncio.sleep(2)

@app.on_event("startup")
async def startup_risk_manager():
    global RISK_TASK
    RISK_TASK=asyncio.create_task(risk_loop())

@app.on_event("shutdown")
async def shutdown_risk_manager():
    if RISK_TASK:
        RISK_TASK.cancel()
        try:await RISK_TASK
        except asyncio.CancelledError:pass
