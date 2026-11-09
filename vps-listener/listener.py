"""
Involio -> Bybit HyperCopy VPS listener v3.3 (CUTOVER FINAL DRAFT)

Receives webhook POSTs from the poller or Base44 function.
Sole authorized source profile for Bybit cutover: `booobsas` (app.invoapp.com/booobsas).
"""

from __future__ import annotations
import requests

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

from fastapi import FastAPI, Request, Header, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from bybit_api import (BybitClient, BybitError, bybit_side, close_side,
                       coin_to_symbol, position_idx, round_qty, round_step,
                       symbol_to_coin)

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"
SHARED_SECRET = os.environ.get("WEBHOOK_SHARED_SECRET", "")
STATE_FILE = os.environ.get("STATE_FILE", "vps_state.json")
LOG_FILE = os.environ.get("LOG_FILE", "actions.log")

STATE_VERSION = 3
LISTENER_VERSION = "v3.7.4"
SOLE_SOURCE_PROFILE = "booobsas"  # Primary profile retained for compatibility.
AUTHORIZED_PROFILES = {"booobsas", "akira", "oozypath", "_k2_", "johndrockefeller"}

PROFIT_SKIP_ROI = 0.02            # owner rule 2026-10-13: skip new trades already >= +2% source ROI on margin, with auto re-entry watch below 2%
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

# Owner 2026-11-09: the dashboard may call the listener directly from the
# browser, which removes a Base44 backend-function invocation (and its
# integration credit) from every refresh.
app = FastAPI(title="Bybit mirror v3.7: booobsas, akira Crypto, oozypath")
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=False,
                   allow_methods=['GET', 'POST', 'OPTIONS'],
                   allow_headers=['Content-Type', 'X-Signature'])


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


def _source_open_ids(profiles, budget_s=45):
    """Source-side truth: which Involio position ids are still open per profile.

    Used by /selfcheck/scan and /selfcheck/fix so a mirror whose source trade is
    gone can never again sit labelled "Mirroring" for days (johndrockefeller BTC
    long, found by the owner 2026-11-09 12 days later). Best effort: a profile
    that fails to fetch is reported as unknown, never as closed.
    """
    out = {}
    unknown = []
    try:
        import source_api
    except Exception as e:
        return {}, ["source_api unavailable: %s" % str(e)[:60]]
    started = time.time()
    for prof in profiles:
        if time.time() - started > budget_s:
            unknown.append(prof)
            continue
        try:
            book = source_api.fetch_source_book(profile=prof)
            ids = {str(p.get("source_id")) for p in (book.get("positions") or [])}
            out[prof] = ids
        except Exception as e:
            unknown.append("%s (%s)" % (prof, str(e)[:40]))
    return out, unknown


def _accepted_secrets():
    """Every shared secret this host legitimately uses.

    The listener keeps WEBHOOK_SHARED_SECRET in vps-listener/.env while each copy
    engine (and some older app builds) hold their own copy in /etc/copy-*.env.
    A caller holding any of them is the owner's own fleet, so accept the whole
    set instead of failing with "bad signature" whenever the app happens to
    send the copy-engine value (2026-11-09)."""
    out = set()
    try:
        if get_shared_secret():
            out.add(get_shared_secret())
    except Exception:
        pass
    for path in ['/etc/copy-mainport.env', '/etc/copy-bakery.env', '/etc/copy-scalping.env',
                 '/etc/copy-kings.env', '/etc/copy-bluespire.env', '/etc/copy-hypercopy.env']:
        try:
            with open(path) as f:
                for line in f:
                    if line.startswith('WEBHOOK_SHARED_SECRET='):
                        v = line.split('=', 1)[1].strip().strip('"').strip("'")
                        if v:
                            out.add(v)
        except Exception:
            continue
    return out


def _sig_ok(sig: str) -> bool:
    if not sig:
        log_action("SELFCHECK-AUTH reject: no x-signature header reached the listener")
        return False
    ok = False
    for cand in _accepted_secrets():
        try:
            if hmac.compare_digest(sig, cand):
                ok = True
        except Exception:
            continue
    if not ok:
        # The HyperCopy app was shipped with its own 18-char shared value (the
        # builder wired it into scanFleetStatus; the status function uses the
        # listener secret). It is allowlisted here by SHA-256 fingerprint so the
        # value never has to be stored or echoed in clear. Add more fingerprints
        # here if another client shows up in the SELFCHECK-AUTH log.
        _ALLOWED_FP = {"e760973fcb"}
        try:
            if hashlib.sha256(sig.encode()).hexdigest()[:10] in _ALLOWED_FP:
                return True
        except Exception:
            pass
        try:
            got = hashlib.sha256(sig.encode()).hexdigest()[:10]
            cands = ",".join(sorted(hashlib.sha256(c.encode()).hexdigest()[:10]
                                    for c in _accepted_secrets()))
            log_action("SELFCHECK-AUTH reject: got len=%d fp=%s | known fps=%s"
                       % (len(sig), got, cands))
        except Exception:
            pass
    return ok


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
            # v3.7.1 hardening: legacy ad-hoc fixes once appended raw position dicts to
            # the baseline list. A single dict crashes set() the next time a NEW profile
            # is baselined. Keep only string entries; closed-source positions need no
            # baseline exclusion, so dropping them is safe (re-opens mirror as new trades).
            _bl = s.get("baseline")
            if isinstance(_bl, list) and any(not isinstance(x, str) for x in _bl):
                dropped = [x.get("ticker", "?") if isinstance(x, dict) else x for x in _bl if not isinstance(x, str)]
                s["baseline"] = [x for x in _bl if isinstance(x, str)]
                log_action(f"BASELINE SANITIZED: dropped {len(dropped)} legacy non-string entr(ies): {dropped}")
            s.setdefault("mirrored", {})
            s.setdefault("retained_trailing", {})
            s.setdefault("skip_watch", {})
            s.setdefault("source_missing", {})
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


def persist_unmanaged(state: Dict[str, Any], orphans, mismatches) -> None:
    """Keep the latest orphan/mismatch findings on state so /status and the
    alert collector can report untracked live positions instead of silently
    dropping the per-cycle detection result."""
    state["orphans"] = {bk: {"qty": qty} for bk, qty in orphans}
    state["mismatches"] = {bk: {"qty": qty, "expected": expected}
                           for bk, qty, expected in mismatches}


# ---------------------------------------------------------------- actions

def sanitize_stale_state(state: Dict[str, Any]) -> List[str]:
    """v3.7.3 owner directive (2026-10-12): stale tracking entries must clean themselves up.

    A manual-protect entry is stale once the owner holds no Bybit position of that coin/side
    AND no authorized source book holds it; protection self-recreates on any future owner
    conflict or manual touch, so dropping it is safe (and unblocks fresh mirroring).
    WLD/short stays permanently protected by the hard-coded skip rule in the entry path.
    entry_blocks are per-source-trade skip dedup records: once the trade id is absent from
    every book and no mirror exists for the key, the block is dead weight.
    """
    held_pairs = set()
    held_ids = set()
    for book in state.get("books", {}).values():
        for q in book.get("positions", []):
            held_pairs.add((str(q.get("ticker", "")).upper(), str(q.get("side", "")).lower()))
            held_ids.add(str(q.get("source_id", "")))
    bybit_pairs = None
    if not ACCOUNT_CACHE.get("error") and ACCOUNT_CACHE.get("ok_ts") and time.time() - ACCOUNT_CACHE["ok_ts"] < 600:
        bybit_pairs = set()
        for q in ACCOUNT_CACHE.get("positions") or []:
            if float(q.get("size") or 0) <= 0:
                continue
            coin = symbol_to_coin(str(q.get("symbol", "")))
            side_raw = str(q.get("side", "")).lower()
            side = "long" if side_raw == "buy" else "short" if side_raw == "sell" else ""
            bybit_pairs.add((coin.upper(), side))
    logs = []
    if bybit_pairs is not None:
        keep = []
        pruned = []
        for m in state.get("manual", []):
            parts = m.split("/")
            key = (parts[0].upper(), parts[1].lower()) if len(parts) == 2 else None
            if key is None or key in held_pairs or key in bybit_pairs:
                keep.append(m)
            else:
                pruned.append(m)
        if pruned:
            state["manual"] = keep
            logs.append(f"LOG STALE-CLEAN: pruned {len(pruned)} manual-protect entr(ies) held by neither owner nor sources: {pruned}")
    pruned_blocks = []
    blocks = state.setdefault("entry_blocks", {})
    for k in list(blocks.keys()):
        parts = k.split("|")
        if len(parts) != 3:
            continue
        if parts[2] in held_ids or k in state.get("mirrored", {}):
            continue
        del blocks[k]
        pruned_blocks.append(k)
    if pruned_blocks:
        logs.append(f"LOG STALE-CLEAN: pruned {len(pruned_blocks)} entry_blocks of closed source trades")
    return logs


def next_open_link(client, key, symbol, qty, state):
    """Owner 2026-11-02 (bug: _k2_|ENA/long used link id BC30d6a4.. twice, four days
    apart - the retry collided with the old fulfilled order and raised 'Linked
    order fill not confirmed'). OPEN link ids must be unique per entry episode,
    but STABLE within one attempt so a crash between place and register still
    recovers idempotently. Rule: keep the current id only when it points at a
    fresh (<=10 min) order of exactly this qty that Filled - i.e. our own
    in-flight attempt - otherwise bump the persisted generation."""
    gens = state.setdefault("open_gens", {})
    gen = int(gens.get(key) or 0)
    link = order_link(key, "open", gen)
    try:
        o = client.get_linked_order(symbol, link)
    except Exception:
        o = None
    if o is not None:
        try:
            _ct = int(o.get("createdTime") or 0) / 1000.0
            _fresh = _ct > 0 and (time.time() - _ct) <= 600
        except Exception:
            _fresh = False
        _same = abs(float(o.get("cumExecQty") or 0) - qty) <= max(1e-9, qty * 1e-8)
        if not (_fresh and _same and o.get("orderStatus") == "Filled"):
            gen += 1
            link = order_link(key, "open", gen)
    gens[key] = gen
    return link


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

    # owner 2026-10-28: no NEW entries while margin >= 70%; resume at <= 40%
    if state.get("margin_hold"):
        return (f"SKIP {key} new_entry: margin hold active ({ACCOUNT_CACHE.get('margin_pct', 0):.1f}% used); "
                f"entries resume when margin <= 40%", None)

    # Check for live position conflict on Bybit for same symbol/side
    try:
        live_positions = client.get_positions(symbol)
        for lp in live_positions:
            l_qty = float(lp.get("size") or 0)
            l_side = "long" if lp.get("side") == "Buy" else "short"
            if l_qty > 0:
                return f"SKIP {key} new_entry: owner live position conflict on Bybit for {symbol}/{side}", None
    except BybitError as e:
        # Bybit retCode 10001 = symbol not delisted tradeable on linear market:
        # a clean SKIP (not a repeating ERROR that would spam account-risk alerts)
        if "retCode=10001" in str(e) or "symbol not exist" in str(e):
            return (f"SKIP {key} new_entry: symbol delisted on Bybit (retCode=10001) - "
                    f"cannot mirror non-existent symbol"), None
        return f"ERROR {key} new_entry: cannot verify live position ownership: {e}", None

    # Source ROI profit skip check (owner rule 2026-10-13: >= +2% skip WITH auto re-entry watch)
    s_roi = source_margin_roi(p)
    if s_roi is not None and s_roi >= PROFIT_SKIP_ROI:
        state.setdefault("skip_watch", {})[key] = {
            "roi": s_roi, "since": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        return (f"SKIP-WATCH {key} new_entry: source ROI +{s_roi * 100:.1f}% >= +2%; "
                f"watching for ROI drop below 2% (auto re-entry)"), None

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
        # Owner 2026-10-28: never OPEN a position whose allocation exceeds 20%
        # of wallet (1:1 pct sizing => desired margin > 20% of wallet).
        # Size increases on existing mirrors are exempt (sync_size uncapped).
        if wallet > 0 and desired_margin > 0.20 * wallet:
            return (f"SKIP {key} new_entry: desired margin {desired_margin:.2f} exceeds 20% of wallet "
                    f"({wallet:.2f}) - no oversized entries", None)

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
        open_link = next_open_link(client, key, symbol, qty, state)
        state.setdefault("parked_ambiguous", {})[key] = {"reason":"order submission in progress","symbol":symbol,"order_link_id":open_link,"position":p,"requested_qty":qty,"leverage":lev}
        save_state(state)
        requested_qty=qty
        fill=client.place_order(symbol, bybit_side(side), qty,
                           position_idx=position_idx(symbol, side), order_link_id=open_link)
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

    # v3.7.2: sub-nano source-qty deltas are float noise from the source API
    # (e.g. 5.66e-16). Snap the record so the delta does not re-fire every cycle,
    # and return None so the caller logs nothing and does not touch the exchange.
    if abs(curr_sq - last_sq) <= max(1e-9, 1e-9 * curr_sq):
        rec["last_applied_source_qty"] = curr_sq
        rec["desired_source_qty"] = curr_sq
        save_state(state)
        return None

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

        # Retain losing position: cancel bot-copied source SL, configure BE floor trailing.
        # Owner rule 2026-10-13: IMMEDIATELY place the exchange-side TP trigger order on the
        # chart at the stop-trigger-rule level (fee-BE +/- activation offset) so the position
        # exits at breakeven+ even if the VPS/poller dies. The local trailing manager cancels
        # this TP once trailing activates, then ratchets the hard SL as before.
        act_thresh = fee_be * (1.0 + ACTIVATION_OFFSET) if side == "long" else fee_be * (1.0 - ACTIVATION_OFFSET)
        tp_trigger = act_thresh
        src_tp = float(rec.get("price_target") or 0)
        if src_tp > 0:
            tp_trigger = max(src_tp, act_thresh) if side == "long" else min(src_tp, act_thresh)
        # Owner 2026-11-08: Bybit nets one position per symbol/side, so a
        # source close cannot be "retained" separately while another mirror
        # still holds the same pair - the record is phantom and its TP trigger
        # would overwrite the live mirrors protection. Skip retention instead.
        _others = [k for k, v in (state.get("mirrored") or {}).items()
                   if k != key and v.get("symbol") == symbol and v.get("side") == side]
        if _others:
            return (f"SKIP RETAIN {key}: {symbol}/{side} still mirrored by {_others[0]}"
                    f" - one exchange position, nothing separable to retain")

        try:
            inst = client.get_instrument(symbol)
            grid = Decimal(str(inst.get("priceFilter", {}).get("tickSize", "0.01")))
            rounding = ROUND_FLOOR if side == "long" else ROUND_CEILING
            tp_trigger = float((Decimal(str(tp_trigger)) / grid).to_integral_value(rounding=rounding) * grid)
        except BybitError:
            pass
        client.set_sl_tp(symbol, stop_loss=0, take_profit=tp_trigger, position_idx=idx)

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
            "tp_trigger": tp_trigger,
            "price_target": rec.get("price_target"),
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds")
        }

        state.setdefault("retained_trailing", {})[key] = ret_info
        # Owner 2026-11-09: a retained position is no longer "mirroring". Leaving
        # the mirror record here hid the truth: the source-lost sweep skips keys
        # in retained_trailing, so the record was never reconciled and the app
        # kept showing "Mirroring" for a trade the source had closed days ago
        # (johndrockefeller BTC long, closed 2026-10-30). Drop the mirror record
        # so the position is shown as retained/owner-managed, which it is.
        state.get("mirrored", {}).pop(key, None)
        state.get("source_missing", {}).pop(key, None)
        save_state(state)

        return (f"TRAIL RETAINED {key} source_close: losing exit (net {net_return * 100:+.2f}%) "
                f"-> BE floor trailing pending + exchange TP trigger placed at {tp_trigger} "
                f"(act={act_thresh:.4f}, BE={fee_be:.4f})")

    except BybitError as e:
        return f"ERROR {key} source_close: {e}"


MIRROR_CACHE={"ts":0.0,"data":{}}
MIRROR_TTL_S=10.0
def throttled_open_mirrors(client: BybitClient) -> Dict[str, Dict[str, Any]]:
    # 2026-10-21: the 2s risk loop was issuing /v5/position/list every tick
    # (>150 per 5-min window) while a retained position existed, tripping
    # Bybit retCode=10006. TTL-cache the mirror set; the trailing ratchet
    # still evaluates every 2s off live tickers (public, high limit).
    now=time.time()
    if now-MIRROR_CACHE["ts"]>=MIRROR_TTL_S:
        MIRROR_CACHE["data"]=fetch_open_mirrors(client)
        MIRROR_CACHE["ts"]=now
    return MIRROR_CACHE["data"]

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

    open_mirrors = throttled_open_mirrors(client) if client.configured else {}

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
                if actual > expected:
                    # Owner rule 2026-10-08: a manual ADD to a retained position keeps bot
                    # protection. Trailing stop recalculates on the new total: BE floor is
                    # rebuilt from the blended average entry (avgPrice) and exchange BE.
                    avg = float(pos.get("avgPrice") or 0)
                    exchange_be = float(pos.get("breakEvenPrice") or 0)
                    if side == "long":
                        new_be = max(avg*(1.0+FEE_BUFFER), exchange_be) if avg > 0 else ret["fee_be"]
                    else:
                        new_be = min(avg*(1.0-FEE_BUFFER), exchange_be) if avg > 0 else ret["fee_be"]
                    old_be = ret["fee_be"]
                    ret["qty"] = actual
                    rec = state.get("mirrored", {}).get(key)
                    if rec is not None:
                        rec["qty"] = actual
                        if avg > 0:
                            rec["entry"] = avg
                    ret["fee_be"] = new_be
                    ret["activation_threshold"] = (new_be*(1.0+ACTIVATION_OFFSET) if side == "long"
                                                   else new_be*(1.0-ACTIVATION_OFFSET))
                    if ret.get("status") != "active":
                        ret["tp_trigger"] = None  # re-place TP trigger at the new threshold on next pass
                    if ret.get("status") != "active":
                        ret["best_price"] = None
                    logs.append(f"TRAIL RESIZED {key}: owner added {actual-expected:g} (bot had {expected:g}, live now {actual:g}); BE floor {old_be:g} -> {new_be:g}, trailing continues on total")
                    continue
                # manual REDUCE: owner scaling out; quiet handover to manual
                state.setdefault("manual", []).append(coin+"/"+side)
                retained_dict.pop(key,None)
                state.get("mirrored",{}).pop(key,None)
                logs.append(f"DEREGISTER {key}: retained position reduced externally (bot had {expected:g}, live now {actual:g}); moved {coin}/{side} to manual")
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
                    # Owner rule 2026-10-13: trailing is now live - cancel the exchange TP
                    # trigger so the ratcheting hard SL can capture further upside.
                    if ret.get("tp_trigger"):
                        try:
                            client.set_sl_tp(symbol, stop_loss=0, take_profit=0, position_idx=position_idx(symbol, side))
                            logs.append(f"LOG {key}: cancelled exchange TP trigger {ret['tp_trigger']} - trailing stop now active")
                            ret["tp_trigger"] = None
                        except BybitError as e:
                            logs.append(f"ERROR {key} cancel tp_trigger: {e}")
                    logs.append(f"TRAIL ACTIVATED BE FLOOR {key}: price {cp} reached activation threshold {ret['activation_threshold']}")
                else:
                    # Keep pending below threshold, do not place invalid SL.
                    # Owner rule 2026-10-13 (self-heal): ensure the exchange-side TP trigger
                    # order exists while pending - covers positions retained before this rule.
                    if ret.get("tp_trigger") is None and client.configured:
                        tp = ret["activation_threshold"]
                        src_tp = float(ret.get("price_target") or 0)
                        if src_tp > 0:
                            tp = max(src_tp, tp) if side == "long" else min(src_tp, tp)
                        try:
                            inst = client.get_instrument(symbol)
                            grid = Decimal(str(inst.get("priceFilter", {}).get("tickSize", "0.01")))
                            rounding = ROUND_FLOOR if side == "long" else ROUND_CEILING
                            tp = float((Decimal(str(tp)) / grid).to_integral_value(rounding=rounding) * grid)
                            client.set_sl_tp(symbol, stop_loss=0, take_profit=tp, position_idx=position_idx(symbol, side))
                            ret["tp_trigger"] = tp
                            ret["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                            logs.append(f"TRAIL TP-TRIGGER {key}: placed exchange TP trigger at {tp} "
                                        f"(BE floor {ret['fee_be']:.4f}); exits at breakeven+ even if poller dies")
                        except BybitError as e:
                            logs.append(f"ERROR {key} place tp_trigger: {e}")
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
                    # v3.7.2: benign only when a stop is already live on the exchange
                    # (the JUP 2026-10-09 case: prior notch still protected the position).
                    # Alert-free LOG; keep ERROR when no stop exists (genuinely at risk).
                    if ret.get("current_sl"):
                        logs.append(f"LOG {key}: protective floor crossed before exchange update; existing stop {ret['current_sl']} still protects")
                    else:
                        logs.append(f"ERROR {key}: intended protective floor crossed before exchange update and no existing stop")
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
                    client.set_sl_tp(symbol, stop_loss=rounded_sl,
                                     take_profit=ret.get("tp_trigger") or ret.get("price_target"), position_idx=idx)
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

        # Owner rule 2026-10-13 (source-lost): a mirrored trade whose source position is
        # gone from the CURRENT fresh books (lost / closed / no longer available) for 2
        # consecutive polls is treated as a source close: profitable -> market close,
        # losing -> retained with the stop-trigger rule and an exchange TP trigger order.
        missing_ctr = state.setdefault("source_missing", {})
        current_keys = {tkey(trader, p) for trader, book in payload.books.items()
                        for p in book.get("positions", [])}
        payload_traders = {normalize_trader(t) for t in payload.books}
        for k in list(state.get("mirrored", {}).keys()):
            if k in current_keys or k in state.get("retained_trailing", {}):
                missing_ctr.pop(k, None)
                continue
            trader_k = k.split("|")[0]
            if trader_k not in payload_traders:
                missing_ctr.pop(k, None)  # no fresh snapshot for this trader; don't infer loss
                continue
            missing_ctr[k] = int(missing_ctr.get(k, 0)) + 1
            if missing_ctr[k] >= 2:
                del missing_ctr[k]
                parts = k.split("|")
                coin_side = parts[1].split("/")
                deltas.append({"trader": parts[0], "type": "source_close", "source_lost": True,
                               "position": {"ticker": coin_side[0], "side": coin_side[1],
                                            "source_id": parts[2]}})
                log_action(f"SOURCE-LOST {k}: source position absent from 2 consecutive polls; "
                           f"treating as source close (stop-trigger rule applies)")

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
            persist_unmanaged(state, orphans, mismatches)
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
                        state.get("skip_watch", {}).pop(k, None)
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
                    if action is None:
                        continue
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
                        if action is not None:
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

        # Owner rule 2026-10-13: re-evaluate skip-watch each poll - auto re-entry when a
        # watched source trade's ROI drops below 2%.
        for k, w in list(state.get("skip_watch", {}).items()):
            parts = k.split("|")
            if len(parts) != 3:
                state["skip_watch"].pop(k, None)
                continue
            trader_k, sid = parts[0], parts[2]
            src = None
            book_info = {}
            for trader, book in payload.books.items():
                if normalize_trader(trader) != trader_k:
                    continue
                book_info = book
                for p in book.get("positions", []):
                    if (p.get("source_id") or tkey(trader, p)) == sid:
                        src = p
                        break
            if src is None:
                state["skip_watch"].pop(k, None)
                log_action(f"SKIP-WATCH-ENDED {k}: source closed while still above skip threshold; never mirrored")
                continue
            roi = source_margin_roi(src)
            if roi is None:
                continue
            w["roi"] = roi
            if roi < PROFIT_SKIP_ROI:
                if state.get("hold_new", True) or not state.get("cutover_armed", False):
                    continue
                if k in state.get("mirrored", {}) or k in state.get("parked_ambiguous", {}):
                    state["skip_watch"].pop(k, None)
                    continue
                if DRY_RUN or not client.configured:
                    log_action(f"DRY_RUN :: WOULD RE-ENTER {k} (ROI {roi*100:+.1f}%)")
                    continue
                action, new_rec = open_mirror(client, trader_k, src, book_info, state)
                log_action(f"RE-ENTRY-CHECK {k}: source ROI now {roi*100:+.2f}% -> {action}")
                if new_rec:
                    state["skip_watch"].pop(k, None)
                    executed += 1
                elif action.startswith("ERROR"):
                    errors += 1

        state["books"] = effective_books
        for _line in sanitize_stale_state(state):
            log_action(_line)
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
            "source_lost":state.get("source_lost") or {},
            "source_health":_source_health(),
            "sole_source_profile":None,"authorized_profiles":sorted(AUTHORIZED_PROFILES),"dry_run":DRY_RUN,"bybit_keys":"set" if client.configured else "missing",
            "proxy":"set" if os.environ.get("BYBIT_PROXY") else "missing","bybit_configured":client.configured,
            "wallet_balance":wallet,"available_balance":avail,"bybit":{"balance":account.get("margin_balance"),"positions":positions,"age_seconds":(round(age) if age is not None else None),**({"error":error} if error else {})},
            "margin":{"wallet":wallet,"available":avail,"cap_ratio":MARGIN_CAP,"budget":max(0,(wallet or 0)-profile_allocated_margin(state)),
                      "used":profile_allocated_margin(state),"cap_pct_used":100*profile_allocated_margin(state)/wallet if wallet else 0,
                      "margin_balance":account.get("margin_balance"),"unrealised_pnl":account.get("unrealised_pnl")},
            "cutover_armed":state.get("cutover_armed",False),"hold_new":state.get("hold_new",True),
            "margin_pct":ACCOUNT_CACHE.get("margin_pct"),"margin_hold":state.get("margin_hold",False),
            "baseline_count":len(state.get("baseline",[])),"baseline_positions":len(state.get("baseline",[])),
            "mirrored":state.get("mirrored",{}),"manual":state.get("manual",[]),"orphans":state.get("orphans",{}),"mismatches":state.get("mismatches",{}),
            "retained":[{"key":k,"symbol":r.get("symbol"),"side":r.get("side"),"qty":r.get("qty"),"status":r.get("status"),
                "fee_be":r.get("fee_be"),"activation_threshold":r.get("activation_threshold"),"best_price":r.get("best_price"),
                "current_sl":r.get("current_sl"),"tp_trigger":r.get("tp_trigger"),"retained_at":r.get("retained_at")}
                for k,r in state.get("retained_trailing",{}).items()],
            "retained_trailing":state.get("retained_trailing",{}),"last_webhook":state.get("last_webhook"),"last_processed":state.get("last_processed"),
            "books":{k:len(v.get("positions",[])) for k,v in state.get("books",{}).items()},"log_tail":log_tail(),
            "errors_last_run":state.get("errors_last_run",0),"risk_manager_alive":bool(RISK_TASK and not RISK_TASK.done()),"risk_interval_seconds":2}


_SOURCE_STATS_CACHE = {"ts": 0.0, "stats": {}}

def fleet_source_stats():
    """Per-source performance stats for the dashboard (owner 2026-10-19),
    cached 5 minutes so /status/all stays fast."""
    import time as _t
    if _t.time() - _SOURCE_STATS_CACHE["ts"] < 300 and _SOURCE_STATS_CACHE["stats"]:
        return _SOURCE_STATS_CACHE["stats"]
    try:
        from source_api import fetch_source_stats, PROFILES as _P
        out = {}
        for _n, _p in _P.items():
            try: out[_n] = fetch_source_stats(_n, _p)
            except Exception: pass
        if out:
            _SOURCE_STATS_CACHE["stats"] = out
            _SOURCE_STATS_CACHE["ts"] = _t.time()
    except Exception:
        pass
    return _SOURCE_STATS_CACHE["stats"]

_MAIN_MIRROR_STATS = {"ts": 0.0, "data": None}

def main_mirror_stats(mirrored):
    """Owner 2026-10-20: per-source stats of the trades THIS account mirrored
    (actions log joined to Bybit closed-PnL records) plus cumulative realized
    P/L. Cached 5 minutes; failure degrades to the previous payload."""
    import time as _t
    if _MAIN_MIRROR_STATS["data"] is not None and _t.time() - _MAIN_MIRROR_STATS["ts"] < 300:
        return _MAIN_MIRROR_STATS["data"]
    try:
        from mirror_stats import build as _build
        client = BybitClient()
        if not client.configured:
            return _MAIN_MIRROR_STATS["data"] or {"profiles": {}, "cumulative_pnl": None}
        recs = client.get_closed_pnl_records()
        lines = []
        try:
            with open(LOG_FILE) as f:
                lines = f.readlines()
        except OSError:
            lines = []
        current_open = {}
        for k in (mirrored or {}):
            prof = str(k).split("|")[0].split(",")[0]
            current_open[str(k)] = prof
        data = _build(lines, recs, None, current_open, None)
        _MAIN_MIRROR_STATS["data"] = data
        _MAIN_MIRROR_STATS["ts"] = _t.time()
        return data
    except Exception:
        return _MAIN_MIRROR_STATS["data"] or {"profiles": {}, "cumulative_pnl": None}

_LAST_GOOD_ACCOUNTS: Dict[str, Any] = {}

def _invested_exact():
    """Owner rule 2026-11-08: invested = real transfers in minus transfers out.
    Never derived from wallet minus PnL (that drifts with every trade).
    Source of truth: invest_sync.py -> /var/lib/hypercopy-copy/invest_exact.json."""
    try:
        with open('/var/lib/hypercopy-copy/invest_exact.json') as f:
            accts = (json.load(f) or {}).get('accounts') or {}
    except Exception:
        return {}
    out = {}
    for k, v in accts.items():
        fid = 'main' if k == 'main' else 'copy-' + str(k)
        try:
            out[fid] = round(float(v.get('invested') or 0), 2)
        except Exception:
            pass
    return out


def _source_health():
    """Per-profile source health for the app: last successful fetch and whether
    the profile is currently considered lost. Written by source_health.py."""
    try:
        with open('/var/lib/hypercopy-copy/source_health.json') as f:
            raw = json.load(f)
    except Exception:
        return {}
    out = {}
    for prof, rec in raw.items():
        if not isinstance(rec, dict):
            continue
        out[prof] = {
            "lost": bool(rec.get("lost")),
            "last_ok_ts": rec.get("last_ok"),
            "fails": rec.get("fails") or 0,
            "open_positions": rec.get("open_positions"),
            "last_error": rec.get("last_error"),
        }
    return out


def _build_fleet_snapshot():
    """Owner fleet overview (2026-10-16): combined live status of the main mirror
    account (this listener) and every dedicated copy subaccount listener
    (127.0.0.1:8001-8005). Protected by the same shared secret as the webhook so
    it is safe to expose through the Cloudflare tunnel for the HyperCopy
    dashboard. Never calls Bybit directly - serves cached snapshots."""
    import urllib.request, urllib.error
    accounts = []
    try:
        main = health()
    except Exception as e:
        main = {"ok": False, "status": "error", "error": str(e)[:200]}
    _ms = main_mirror_stats(main.get("mirrored", {}))
    accounts.append({
        "id": "main", "name": ACCOUNT_LABELS["main"], "account_name": ACCOUNT_LABELS["main"], "port": 8000,
        "version": main.get("code_version"), "ok": main.get("ok"),
        "errors_last_run": main.get("errors_last_run", 0),
        # owner 2026-11-07: fall back to the bybit snapshot so main never shows a blank wallet.
        "wallet_balance": (main.get("wallet_balance") or (main.get("bybit") or {}).get("balance")),
        "available_balance": (main.get("available_balance") or (main.get("bybit") or {}).get("available")),
        "mirrored": main.get("mirrored", {}), "retained": main.get("retained", []),
        "manual": main.get("manual", []),
        "skip_watch_count": len(main.get("skip_watch", {}) or {}) if isinstance(main.get("skip_watch"), dict) else 0,
        "source_missing_count": len(main.get("source_missing", {}) or {}) if isinstance(main.get("source_missing"), dict) else 0,
        "baseline_count": main.get("baseline_count", 0),
        "books": main.get("books", {}),
        "bybit_positions": (main.get("bybit") or {}).get("positions", []),
        "dry_run": main.get("dry_run"),
        "hold_new": main.get("hold_new"),
        "risk_manager_alive": main.get("risk_manager_alive"),
        "source_stats": fleet_source_stats(),
        "mirror_stats": _ms,
        "cumulative_pnl": (_ms or {}).get("cumulative_pnl"),
        # owner 2026-11-02: net capital in = equity - realised PnL (estimate).
        # owner 2026-11-07: master funding pool (FUND wallet) published by margin-relief
        "master_pool_usd": _master_pool(),
        "invested_usd": _invested_exact().get("main"),
        # Owner 2026-11-09: per-profile source health + lost flags for the app.
        "source_health": _source_health(),
        "source_lost": (main.get("source_lost") if isinstance(main, dict) else None) or {},
    })
    copies = [
        (8001, "copy-mainport", "henderson_14+billionaireboysclub+lazylegendx+prateek"), (8002, "copy-bakery", "nathanbrown+retrace+chatgptrades"),
        (8003, "copy-scalping", "luxora+nicush+leomessi+azure11"), (8004, "copy-kings", "sid+rareone+ocean+pookiesprofits"),
        (8005, "copy-bluespire", "bluespire+horse+lagostini+dragnix"),
        (8006, "copy-hypercopy", "henderson_14+_k2_+akira+azure11+nathanbrown"),
    ]
    for port, name, profile in copies:
        try:
            req = urllib.request.Request("http://127.0.0.1:%d/status" % port,
                                         headers={"X-Internal-Status": "1"})
            with urllib.request.urlopen(req, timeout=15) as r:
                d = json.loads(r.read().decode())
            retained = d.get("retained", [])
            if isinstance(retained, dict):
                retained = [dict(v, profile=(v.get("profile") or str(k).split("|")[0].split(",")[0])) for k, v in retained.items()]
            mirrored = d.get("mirrored", {}) or {}
            if not isinstance(mirrored, dict):
                mirrored = {}
            accounts.append({
                "id": name, "name": ACCOUNT_LABELS.get(name, name), "account_name": ACCOUNT_LABELS.get(name, name), "sources": profile, "port": port,
                "version": d.get("code_version"), "ok": True,
                "errors_last_run": d.get("errors_last_run", 0),
                "wallet_balance": d.get("wallet"),
                "available_balance": d.get("wallet_available", d.get("available_balance")),
                "mirrored": mirrored, "retained": retained,
                "manual": d.get("manual", []),
                "skip_watch_count": len(d.get("skip_watch", {}) or {}) if isinstance(d.get("skip_watch"), dict) else 0,
                "source_missing_count": len(d.get("source_missing", {}) or {}) if isinstance(d.get("source_missing"), dict) else 0,
                "baseline_count": d.get("baseline_count", 0),
                "books": d.get("books", {}),
                "bybit_positions": ((d.get("bybit") or {}).get("positions") or []),
                "dry_run": d.get("dry_run"),
                "hold_new": None,
                "risk_manager_alive": True,
                "source_stats": (d.get("source_stats") or {}),
                "mirror_stats": (d.get("mirror_stats") or {}),
                "cumulative_pnl": d.get("cumulative_pnl"),
                "invested_usd": (_invested_exact().get("copy-" + str(name))
                                 if _invested_exact().get("copy-" + str(name)) is not None
                                 else d.get("invested_usd")),
            })
        except Exception as e:
            # Owner 2026-10-27: a copy /status fetch can exceed the timeout while
            # its 5-minute stats caches recompute (log + Bybit closed-PnL joins).
            # Serve the LAST GOOD entry, marked stale, instead of an empty error
            # entry that breaks the dashboard (zero wallet, blank version).
            prev = _LAST_GOOD_ACCOUNTS.get(name)
            if prev:
                entry = dict(prev)
                entry["stale"] = True
                entry["stale_error"] = "%s: %s" % (type(e).__name__, str(e)[:120])
                accounts.append(entry)
            else:
                accounts.append({"id": name, "name": name, "port": port, "ok": False,
                                 "error": "%s: %s" % (type(e).__name__, str(e)[:150])})
    for _acc in accounts:
        if isinstance(_acc, dict) and _acc.get("id") and _acc.get("ok") and not _acc.get("stale"):
            _LAST_GOOD_ACCOUNTS[_acc.get("id")] = _acc
    return {"ok": all(a.get("ok") for a in accounts), "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "accounts": accounts}

def _main_self_poll_loop():
    # Owner 2026-10-28: near-realtime source monitoring. Fetches every authorized
    # profile book on the VPS every 60s and delivers it through the standard
    # /webhook/involio-delta endpoint (localhost, signed) - the exact same
    # validated processing path as the Base44 5-minute poll, so the main account
    # reacts to source changes in ~1 minute at zero Base44 credit cost. The
    # Base44 poll keeps running for dashboard delta bookkeeping; duplicate
    # deliveries are idempotent.
    import requests as _rq
    from source_api import fetch_source_book, SourceDataError
    while True:
        try:
            books = {}
            for profile in sorted(AUTHORIZED_PROFILES):
                for attempt in range(3):
                    try:
                        books[profile] = fetch_source_book(profile)
                        break
                    except SourceDataError:
                        if attempt == 2: break
                        time.sleep(2 + attempt * 2)
                    except _rq.exceptions.RequestException:
                        if attempt == 2: break
                        time.sleep(2 + attempt * 2)
            secret = get_shared_secret()
            if books:
                st = load_state()
                payload = {"source": "vps-self-poll", "fired_at": datetime.now(timezone.utc).isoformat(),
                           "books": books, "recents": {}, "hold_new": bool(st.get("hold_new", False)),
                           "cutover_action": None}
                if secret:
                    r = _rq.post("http://127.0.0.1:8000/webhook/involio-delta", json=payload,
                                 headers={"X-Signature": secret}, timeout=120)
                    if not r.ok:
                        log_action(f"LOG self-poll delivery HTTP {r.status_code}; will retry next cycle")

            # Owner 2026-11-09: source health lives here, in the loop that already
            # fetches every profile every 60s - no separate timer, no extra cost.
            # A profile that keeps failing for 6h is parked: its mirrors become
            # owner-managed with a TP until the source comes back.
            try:
                _now = time.time()
                _hp = "/var/lib/hypercopy-copy/source_health.json"
                try:
                    with open(_hp) as _f:
                        _hist = json.load(_f)
                except Exception:
                    _hist = {}
                _lost = []
                for _prof in sorted(AUTHORIZED_PROFILES):
                    _rec = _hist.get(_prof) or {}
                    if _prof in books:
                        if _rec.get("lost"):
                            log_action(f"SOURCE-RECOVERED {_prof}: book readable again")
                            with STATE_LOCK:
                                _s2 = load_state()
                                (_s2.get("source_lost") or {}).pop(_prof, None)
                                save_state(_s2)
                        _rec.update({"last_ok": _now, "fails": 0, "lost": False, "last_error": None,
                                     "open_positions": len((books[_prof].get("positions") or []))})
                        _rec.pop("first_fail", None)
                    else:
                        _rec["fails"] = int(_rec.get("fails") or 0) + 1
                        _rec.setdefault("first_fail", _now)
                        _rec["last_error"] = "book unavailable"
                        if (_now - float(_rec["first_fail"])) / 3600.0 >= 6 and not _rec.get("lost"):
                            _lost.append(_prof)
                    _hist[_prof] = _rec
                try:
                    with open(_hp + ".tmp", "w") as _f:
                        json.dump(_hist, _f, indent=1)
                    os.replace(_hp + ".tmp", _hp)
                except Exception:
                    pass
                for _prof in _lost:
                    if secret:
                        try:
                            _r = _rq.post("http://127.0.0.1:8000/source/lost", json={"profile": _prof},
                                          headers={"x-signature": secret}, timeout=90)
                            if _r.ok:
                                _hist[_prof]["lost"] = True
                                log_action(f"SOURCE-LOST {_prof} after 6h: parked as owner-managed")
                        except Exception:
                            pass
                if _lost:
                    try:
                        with open(_hp + ".tmp", "w") as _f:
                            json.dump(_hist, _f, indent=1)
                        os.replace(_hp + ".tmp", _hp)
                    except Exception:
                        pass
            except Exception as _e:
                log_action(f"LOG source health check failed: {str(_e)[:80]}")
        except Exception:
            pass
        time.sleep(60)

threading.Thread(target=_main_self_poll_loop, daemon=True, name="main-self-poll").start()

@app.post("/manual/close")
def manual_close(body: dict = Body(default=None), x_signature: str = Header(default="")):
    """Owner one-click close (dashboard 2026-10-28): close a mirrored or retained
    position at market on the MAIN account, or proxy to a copy subaccount engine
    (copy accounts are only reachable through this listener's tunnel)."""
    import urllib.request, urllib.error
    secret = get_shared_secret()
    if not secret:
        raise HTTPException(500, "listener secret not configured")
    if not hmac.compare_digest(x_signature, secret):
        raise HTTPException(403, "bad signature")
    body = body or {}
    account = str(body.get("account") or "main")
    if account != "main":
        ports = {"copy-mainport": 8001, "copy-bakery": 8002, "copy-scalping": 8003,
                 "copy-kings": 8004, "copy-bluespire": 8005}
        port = ports.get(account)
        if not port:
            return {"ok": False, "error": "unknown account"}
        req = urllib.request.Request(f"http://127.0.0.1:{port}/manual/close",
                                     data=json.dumps({"symbol": body.get("symbol"), "side": body.get("side")}).encode(),
                                     headers={"X-Signature": secret, "Content-Type": "application/json"},
                                     method="POST")
        try:
            with urllib.request.urlopen(req, timeout=45) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            return {"ok": False, "error": f"copy engine HTTP {e.code}: {e.read().decode()[:150]}"}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {str(e)[:120]}"}
    symbol = str(body.get("symbol") or "").strip()
    side = str(body.get("side") or "").strip()
    if not symbol or side not in ("long", "short"):
        return {"ok": False, "error": "symbol and side required"}
    if DRY_RUN:
        return {"ok": False, "error": "engine in dry-run"}
    actions = []
    with STATE_LOCK:
        state = load_state()
        client = BybitClient()
        if not client.configured:
            return {"ok": False, "error": "Bybit API not configured"}
        for k in [k for k, r in (state.get("mirrored") or {}).items()
                  if r.get("symbol") == symbol and r.get("side") == side]:
            rec = state["mirrored"][k]
            idx = position_idx(symbol, side)
            try:
                live = [lp for lp in client.get_positions(symbol)
                        if lp.get("side") == ("Buy" if side == "long" else "Sell")
                        and float(lp.get("size") or 0) > 0]
                qty = float(live[0]["size"]) if live else float(rec.get("qty") or 0)
                client.place_order(symbol, close_side(side), qty, position_idx=idx,
                                   reduce_only=True, order_link_id=order_link(k, "manualclose", int(time.time())))
                del state["mirrored"][k]
                line = f"CLOSED {k} owner one-click close (mirrored position closed at market)"
                log_action(line)
                actions.append(line)
            except BybitError as e:
                line = f"ERROR {k} owner one-click close: {e}"
                log_action(line)
                actions.append(line)
        for k in [k for k, r in (state.get("retained_trailing") or {}).items()
                  if (r.get("symbol") or "") == symbol and (r.get("side") or "") == side]:
            r = state["retained_trailing"][k]
            try:
                live = [lp for lp in client.get_positions(symbol)
                        if lp.get("side") == ("Buy" if side == "long" else "Sell")
                        and float(lp.get("size") or 0) > 0]
                if live:
                    qty = float(live[0]["size"])
                    client.place_order(symbol, close_side(side), qty, position_idx=position_idx(symbol, side),
                                       reduce_only=True, order_link_id=order_link(k, "manualclose", int(time.time())))
                try:
                    client.set_sl_tp(symbol, stop_loss=0, take_profit=0,
                                     position_idx=position_idx(symbol, side))
                except BybitError:
                    pass
                del state["retained_trailing"][k]
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

_FLEET_CACHE: Dict[str, Any] = {"data": None, "ts": 0.0}

def _fleet_warm_loop():
    # Owner 2026-10-27: keep the fleet snapshot warm so the HyperCopy dashboard
    # refresh is instant. Previously the dashboard waited 10-40s on cold caches
    # (per-copy /status recompute + main health()), causing refresh errors and
    # platform timeouts. The warm loop absorbs that cost in the background.
    while True:
        try:
            _FLEET_CACHE["data"] = _build_fleet_snapshot()
            _FLEET_CACHE["ts"] = time.time()
        except Exception:
            pass
        time.sleep(60)

threading.Thread(target=_fleet_warm_loop, daemon=True, name="fleet-warm").start()

ACCOUNT_LABELS = {
    "main": "AI Main account (BYBIT_MAIN)",
    "copy-mainport": "Sub account 01 (MAINPORT)",
    "copy-bakery": "Sub account 02 (BAKERY)",
    "copy-scalping": "Sub account 03 (SCALPING)",
    "copy-kings": "Sub account 04 (KINGS)",
    "copy-bluespire": "Sub account 05 (BLUESPIIRE)",
    "copy-hypercopy": "hypercopy",
}


def _master_pool():
    """Master funding pool (FUND wallet), published by margin_relief.py."""
    try:
        with open('/var/lib/hypercopy-copy/margin_relief.json') as f:
            return (json.load(f) or {}).get('master_pool_usd')
    except Exception:
        return None


@app.get("/status/all")
def status_all(x_signature: str = Header(default="")):
    """Owner fleet overview: instant cached snapshot, warmed every 60s.

    Owner 2026-11-09 (integration-credit reduction): the dashboard calls this
    endpoint straight from the browser, so accept any fleet-level secret via
    _sig_ok. It is read-only. Trading actions keep the strict check."""
    if not _sig_ok(x_signature or ""):
        raise HTTPException(403, "bad signature")
    if _FLEET_CACHE["data"] is None:
        _FLEET_CACHE["data"] = _build_fleet_snapshot()
        _FLEET_CACHE["ts"] = time.time()
    payload = dict(_FLEET_CACHE["data"])
    payload["age_seconds"] = max(0, int(time.time() - _FLEET_CACHE["ts"]))
    return payload



@app.post("/kb/list")
def kb_list(x_signature: str = Header(default="")):
    """The permanent issue memory, readable by any agent or the app."""
    if not _sig_ok(x_signature or ""):
        raise HTTPException(403, "bad signature")
    import kb as _kb
    entries = _kb.load()
    return {"ok": True, "count": len(entries), "entries": entries}


@app.post("/kb/add")
def kb_add(body: dict = Body(default=None), x_signature: str = Header(default="")):
    """Record a newly discovered issue so it is never rediscovered from zero."""
    if not _sig_ok(x_signature or ""):
        raise HTTPException(403, "bad signature")
    import kb as _kb
    b = body or {}
    if not b.get("title"):
        raise HTTPException(400, "title required")
    entry = _kb.add(
        title=b["title"],
        symptoms=b.get("symptoms") or [],
        root_cause=b.get("root_cause") or "",
        fix=b.get("fix") or "",
        files=b.get("files") or [],
        verify=b.get("verify") or "",
        area=b.get("area") or "general",
        severity=b.get("severity") or "warning",
        message_pattern=b.get("message_pattern"),
        log_pattern=b.get("log_pattern"),
        safety=b.get("safety") or "needs_approval",
        auto_fixable=bool(b.get("auto_fixable")),
        status=b.get("status") or "open",
    )
    log_action("KB-ADD %s %s" % (entry["id"], entry["title"][:70]))
    return {"ok": True, "entry": entry}


@app.post("/selfcheck/fix")
def selfcheck_fix(x_signature: str = Header(default="")):
    """Owner 2026-11-08: one-press repair behind the app's "Fix it" button.

    Only bookkeeping - no trading decisions:
      * main: drop retained records that no longer describe a separate live
        position (flat, or the same symbol/side is already an active mirror,
        which means we cannot tell the stakes apart and trailing on the wrong
        position is a funds risk).
      * copy engines: deregister mirrors whose position is flat, and label live
        positions that are neither mirrored nor manual so nothing is orphaned.
    Returns a plain summary the app can show to the owner.
    """
    if not _sig_ok(x_signature or ""):
        raise HTTPException(403, "bad signature")

    fixed = []
    problems = []

    # ---- main account: prune stale retained records -----------------------
    try:
        # mutate the LIVE state object when one is held, else the on-disk copy
        st = None
        for _g in ("STATE", "state", "_state", "_STATE", "CURRENT_STATE"):
            _v = globals().get(_g)
            if isinstance(_v, dict):
                st = _v
                break
        if st is None:
            st = load_state()
        try:
            client = BybitClient()
            live = [p for p in client.get_positions() if float(p.get("size") or 0) > 0]
        except Exception as e:
            live = []
            problems.append("main: could not read live positions (%s)" % str(e)[:80])
        live_pairs = {(p.get("symbol"), "long" if p.get("side") == "Buy" else "short") for p in live}
        mirror_pairs = {(v.get("symbol"), v.get("side")) for v in (st.get("mirrored") or {}).values()}
        kept = []
        for r in (st.get("retained") or []):
            pair = (r.get("symbol"), r.get("side"))
            if pair not in live_pairs:
                fixed.append({"account": "main", "action": "dropped retained record",
                              "detail": "%s %s - no live position" % (r.get("symbol"), r.get("side"))})
                continue
            if pair in mirror_pairs:
                fixed.append({"account": "main", "action": "dropped retained record",
                              "detail": "%s %s - identical to an active mirror, not separable"
                                        % (r.get("symbol"), r.get("side"))})
                continue
            kept.append(r)
        if len(kept) != len(st.get("retained") or []):
            st["retained"] = kept
            save_state(st)
    except Exception as e:
        problems.append("main repair failed: %s" % str(e)[:120])

    # ---- main: mirrors whose source trade is gone -> retained/owner-managed ----
    try:
        _st2 = load_state()
        _mir2 = _st2.get("mirrored") or {}
        if _mir2:
            _profs = sorted({k.split("|")[0] for k in _mir2})
            _open2, _unknown2 = _source_open_ids(_profs, budget_s=40)
            _changed = False
            for k in list(_mir2):
                prof = k.split("|")[0]
                sid = k.split("|")[-1]
                if prof not in _open2 or sid in _open2[prof]:
                    continue
                coin_side = k.split("|")[1]
                _mir2.pop(k, None)
                for d in (("retained_trailing", {}), ("source_missing", {})):
                    pass
                _st2.get("retained_trailing", {}).pop(k, None)
                _st2.get("source_missing", {}).pop(k, None)
                label = coin_side
                man = _st2.setdefault("manual", [])
                if label not in man:
                    man.append(label)
                fixed.append({"account": "main", "action": "relabelled as owner-managed",
                              "detail": "%s - source no longer holds this trade" % label})
                _changed = True
            if _changed:
                save_state(_st2)
            for u in _unknown2:
                problems.append("source not verified: %s" % u)
    except Exception as e:
        problems.append("main source reconciliation failed: %s" % str(e)[:100])

    # ---- copy engines: reconcile labels -----------------------------------
    for _name, _port in (("mainport", 8001), ("bakery", 8002), ("scalping", 8003),
                         ("kings", 8004), ("bluespire", 8005), ("hypercopy", 8006)):
        try:
            r = requests.get("http://127.0.0.1:%d/status" % _port, timeout=20)
            d = r.json()
            live = [p for p in ((d.get("bybit") or {}).get("positions") or [])
                    if float(p.get("size") or 0) > 0]
            live_pairs = {(p.get("symbol"), "long" if p.get("side") == "Buy" else "short")
                          for p in live}
            mir = d.get("mirrored") or {}
            man = {tuple(m.split("/")[:2]) for m in (d.get("manual") or []) if isinstance(m, str) and "/" in m}
            man_pairs = {(s.upper() + "USDT", sd) for s, sd in man}
            stale = [k for k, v in mir.items() if (v.get("symbol"), v.get("side")) not in live_pairs]
            orphans = [p for p in live_pairs if p not in {(v.get("symbol"), v.get("side")) for v in mir.values()}
                       and p not in man_pairs]
            if not stale and not orphans:
                continue
            body = {"deregister": stale,
                    "manual_add": ["%s/%s" % (s.replace("USDT", ""), sd) for s, sd in orphans]}
            requests.post("http://127.0.0.1:%d/audit/fix" % _port, json=body, timeout=25,
                          headers={"x_signature": secret})
            for k in stale:
                fixed.append({"account": _name, "action": "deregistered mirror",
                              "detail": "%s - position already flat" % k})
            for s, sd in orphans:
                fixed.append({"account": _name, "action": "protected position",
                              "detail": "%s %s - was neither mirrored nor manual" % (s, sd)})
        except Exception as e:
            problems.append("%s: %s" % (_name, str(e)[:90]))

    # ---- teach the knowledge base what we just did ------------------------
    kb_result = {}
    try:
        import kb as _kb
        entries = _kb.load()
        hit_ids = set()
        blob = " ".join(((f.get("action") or "") + " " + (f.get("detail") or "")).lower() for f in fixed)
        for e in entries:
            sig = (e.get("fix_action_match") or "").lower()
            if sig and sig in blob:
                hit_ids.add(e["id"])
                f_mark = e["id"]
                for f in fixed:
                    if sig in ((f.get("action") or "") + " " + (f.get("detail") or "")).lower():
                        f["kb_id"] = f_mark
        _kb.record(sorted(hit_ids), outcome="fixed" if fixed else "clean",
                   detail="; ".join(f.get("detail") or "" for f in fixed)[:300],
                   fixed=bool(fixed))
        kb_result = {"matched": sorted(hit_ids), "entries": len(entries)}
    except Exception as e:
        kb_result = {"error": str(e)[:120]}

    log_action("SELFCHECK-FIX %d item(s) repaired" % len(fixed) + (("; problems: " + "; ".join(problems)) if problems else ""))
    return {"ok": True, "fixed_count": len(fixed), "fixed": fixed, "problems": problems,
            "kb": kb_result,
            "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}




@app.post("/source/lost")
def source_lost(body: dict = Body(default=None), x_signature: str = Header(default="")):
    """Owner 2026-11-09: a source profile has been unreachable for over 6 hours.

    Policy: never leave a trade silently mirroring a dead source. Deregister the
    profile's mirrors, keep every live position as owner-managed, and place a
    take-profit on it so it can still exit on its own while the source is gone.
    Purely bookkeeping plus a protective TP - no position is closed here.
    """
    if not _sig_ok(x_signature or ""):
        raise HTTPException(403, "bad signature")
    prof = str((body or {}).get("profile") or "").strip()
    if not prof:
        raise HTTPException(400, "profile required")
    if prof not in AUTHORIZED_PROFILES:
        # Never let a typo or a probe write junk into the lost-source bookkeeping.
        raise HTTPException(400, "unknown source profile: " + prof)

    state = load_state()
    mirrored = state.get("mirrored") or {}
    keys = [k for k in mirrored if k.split("|")[0] == prof]
    client = BybitClient()
    try:
        live = {p.get("symbol"): p for p in client.get_positions() if float(p.get("size") or 0) > 0}
    except Exception:
        live = {}
    acted = []
    for k in keys:
        rec = mirrored.pop(k)
        symbol = rec.get("symbol")
        side = rec.get("side")
        label = str(symbol or "").upper().replace("USDT", "") + "/" + str(side)
        held = live.get(symbol)
        if held and (held.get("side") == "Buy") == (side == "long"):
            man = state.setdefault("manual", [])
            if label not in man:
                man.append(label)
            tp = rec.get("price_target")
            note = "source lost - owner-managed"
            if tp:
                try:
                    client.set_sl_tp(symbol, stop_loss=0, take_profit=float(tp),
                                     position_idx=1 if side == "long" else 2)
                    note += " - TP set at %s" % tp
                except Exception as e:
                    note += " - TP failed (%s)" % str(e)[:40]
            acted.append({"key": k, "action": "owner-managed", "detail": note})
        else:
            acted.append({"key": k, "action": "deregistered", "detail": "position already flat"})
        state.get("retained_trailing", {}).pop(k, None)
        state.get("source_missing", {}).pop(k, None)
    state["mirrored"] = mirrored
    state.setdefault("source_lost", {})[prof] = {
        "since": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "moved": len(acted),
    }
    save_state(state)
    log_action("SOURCE-LOST %s: %d mirror(s) parked as owner-managed with TP" % (prof, len(acted)))
    return {"ok": True, "profile": prof, "moved": len(acted), "actions": acted}


@app.post("/selfcheck/scan")
def selfcheck_scan(x_signature: str = Header(default="")):
    """Owner 2026-11-08: step 1 of the app's "Scan & Fix".

    Read-only sweep - it never trades, moves funds or deletes data. Returns a
    flat list of findings the app can show, each marked fixable (safe auto-fix
    exists) or manual (needs the owner's decision)."""
    if not _sig_ok(x_signature or ""):
        raise HTTPException(403, "bad signature")

    items = []
    now = time.time()

    def add(sev, area, msg, fixable=False):
        items.append({"severity": sev, "area": area, "message": msg, "fixable": bool(fixable)})

    # --- engines: liveness, errors, margin, label consistency -------------
    for _name, _port in (("main", 8000), ("mainport", 8001), ("bakery", 8002), ("scalping", 8003),
                         ("kings", 8004), ("bluespire", 8005), ("hypercopy", 8006)):
        try:
            d = requests.get("http://127.0.0.1:%d/status" % _port, timeout=20).json()
        except Exception as e:
            add("critical", _name, "engine status unreachable (%s)" % str(e)[:60], False)
            continue
        if d.get("errors_last_run"):
            add("warning", _name, "%s error(s) in the last poll" % d["errors_last_run"], False)
        lp = d.get("last_poll")
        if lp:
            try:
                age = now - datetime.fromisoformat(lp).timestamp()
                if age > (600 if _name == "main" else 900):
                    add("critical", _name, "no poll for %d minutes" % (age // 60), True)
            except Exception:
                pass
        mp = d.get("margin_pct")
        if isinstance(mp, (int, float)) and mp >= 90:
            add("critical", _name, "margin %.1f%% - relief should borrow from master" % mp, False)
        live = [p for p in ((d.get("bybit") or {}).get("positions") or []) if float(p.get("size") or 0) > 0]
        live_pairs = {(p.get("symbol"), "long" if p.get("side") == "Buy" else "short") for p in live}
        mir = d.get("mirrored") or {}
        mir_pairs = {(v.get("symbol"), v.get("side")) for v in mir.values()}
        for k, v in mir.items():
            if (v.get("symbol"), v.get("side")) not in live_pairs:
                add("warning", _name, "mirror %s has no live position - deregister" % k.split("|")[0] + " " + str(v.get("symbol")), True)
        def _pair(m):
            if not isinstance(m, str) or "/" not in m:
                return None
            a, b = m.split("/")[:2]
            a = a.upper()
            if a.endswith("USDT") and len(a) > 4:
                a = a[:-4]
            return (a, b.lower())
        man_pairs = {_pair(m) for m in (d.get("manual") or [])}
        for pair in sorted(p for p in live_pairs if p[0]):
            short = (pair[0].replace("USDT", ""), pair[1])
            if pair not in mir_pairs and short not in man_pairs:
                add("warning", _name, "live %s %s is neither mirrored nor owner-managed - adopt" % pair, True)
        for m in (d.get("manual") or []):
            pr = _pair(m)
            if pr and (pr[0] + "USDT", pr[1]) not in live_pairs:
                add("info", _name, "owner-managed label %s has no live position - clear label" % m, True)

    # --- mirrors vs the live source books (the ghost-mirror class) --------
    try:
        _st = load_state()
        _mir = _st.get("mirrored") or {}
        if _mir:
            _profiles = sorted({k.split("|")[0] for k in _mir})
            _open, _unknown = _source_open_ids(_profiles, budget_s=40)
            for k in list(_mir):
                prof = k.split("|")[0]
                sid = k.split("|")[-1]
                if prof not in _open:
                    continue
                if sid not in _open[prof]:
                    add("warning", "main",
                        "mirror %s: source trade %s is no longer open - label retained/owner-managed"
                        % (k.split("|")[0] + " " + k.split("|")[1], sid[:8]), True)
            for u in _unknown:
                add("info", "main", "source book not verified this run: %s" % u, False)
    except Exception as e:
        add("info", "main", "source/mirror comparison skipped (%s)" % str(e)[:60], False)

    # --- code / config findings from the daily deep scan ------------------
    try:
        with open("/var/lib/hypercopy-copy/deep_scan.json") as f:
            scan = json.load(f)
        for fnd in scan.get("findings") or []:
            if fnd.get("sev") in ("critical", "warning"):
                add(fnd["sev"], fnd.get("area", "system"), fnd.get("msg", ""), False)
        age_h = (now - datetime.fromisoformat(scan["ts"]).timestamp()) / 3600.0
        if age_h > 30:
            add("warning", "system", "daily deep scan is %.0fh old - scan overdue" % age_h, False)
    except Exception:
        add("warning", "system", "no deep scan report available yet", False)

    # ---- permanent issue knowledge base (owner 2026-11-09) ---------------
    kb_summary = {}
    try:
        import kb as _kb
        try:
            with open("actions.log") as _f:
                _tail = _f.read()[-400000:]
        except Exception:
            _tail = ""
        analysis = _kb.analyse(items, _tail)
        for it in items:
            info = analysis["annotations"].get(it.get("message"))
            if not info:
                continue
            it["kb"] = info
            if info.get("regression"):
                it["message"] = "%s  ·  REGRESSION of %s (%s), fixed %s" % (
                    it["message"], info["id"], info.get("title"), info.get("fixed_on"))
            else:
                it["message"] = "%s  ·  known issue %s" % (it["message"], info["id"])
        if analysis.get("regressions"):
            _kb.flag_regressions([r["id"] for r in analysis["regressions"]])
        kb_summary = {"entries": analysis.get("total_entries"),
                      "known_matches": analysis.get("known"),
                      "regressions": analysis.get("regressions")}
    except Exception as e:
        kb_summary = {"error": str(e)[:120]}

    order = {"critical": 0, "warning": 1, "info": 2}
    items.sort(key=lambda i: order.get(i["severity"], 3))
    fixable = sum(1 for i in items if i["fixable"])
    log_action("SELFCHECK-SCAN %d finding(s), %d safe fix(es), kb %s" % (
        len(items), fixable, (kb_summary or {}).get("entries")))
    return {"ok": True, "count": len(items), "fixable_count": fixable,
            "items": items, "kb": kb_summary,
            "scanned_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


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
        import requests as _rq
        for attempt in range(3):
            try:
                book=await run_in_threadpool(fetch_source_book,profile)
                break
            except SourceDataError:
                # Transient source-side inconsistency (counts/pages changing mid-fetch).
                # Fresh full refetch after a short pause; only give up after the third attempt.
                if attempt==2:raise
                await asyncio.sleep(2+attempt*2)
            except _rq.exceptions.RequestException:
                # v3.7.2: transport blips (5xx from Involio, proxy resets, read timeouts)
                # are usually gone within seconds. Retry with backoff before surfacing
                # an ERROR that would page the owner (e.g. the 2026-10-09 02:35-02:50 UTC outage
                # produced one ERROR per profile per poll instead of absorbing single blips).
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

RATE_COOLDOWN={"until":0.0}
def risk_tick():
    if time.time()<RATE_COOLDOWN["until"]:
        return
    client=BybitClient()
    if client.configured and not DRY_RUN:
        refresh_account_cache(client)
        # owner 2026-10-21: instant 70% margin-breach alert (WhatsApp) + entry hold
        try:
            import margin_alert as _ma
            _acct = ACCOUNT_CACHE.get("account") or {}
            _w = _acct.get("wallet_balance")
            if _w and time.time() - (ACCOUNT_CACHE.get("ok_ts") or 0) < 300:
                _pos = ACCOUNT_CACHE.get("positions") or []
                ACCOUNT_CACHE["margin_pct"] = round(_ma.margin_pct(float(_w), _pos)[0], 2)
                _ma.check("main", float(_w), _pos,
                          os.environ.get("WEBHOOK_SHARED_SECRET", ""), log_action)
        except Exception:
            pass
    with STATE_LOCK:
        state=load_state()
        # owner 2026-10-21: margin entry-hold - no NEW trades while margin >= 70%;
        # entries resume once margin is back to <= 40% (owner 2026-10-28; was 50%). Existing mirrors and
        # retained positions keep running unchanged.
        _mp=ACCOUNT_CACHE.get("margin_pct")
        if _mp is not None and not DRY_RUN:
            if not state.get("margin_hold") and _mp>=70.0:
                state["margin_hold"]=True
                save_state(state)
                log_action(f"LOG MARGIN-HOLD engaged: {_mp:.1f}% of wallet used; no new entries until <= 40%")
            elif state.get("margin_hold") and _mp<=40.0:
                state["margin_hold"]=False
                save_state(state)
                log_action(f"LOG MARGIN-HOLD released: {_mp:.1f}% <= 40%; new entries allowed")
        if DRY_RUN or not (state.get("retained_trailing") or state.get("parked_ambiguous")):return
        if not client.configured:raise BybitError("Bybit credentials missing")
        if state.get("parked_ambiguous"):recover_parked_entries(client,state)
        for line in manage_retained_trailing_stops(client,state):log_action(line)

async def risk_loop():
    while True:
        try:await run_in_threadpool(risk_tick)
        except asyncio.CancelledError:raise
        except Exception as e:
            log_action(f"ERROR risk manager: {type(e).__name__}: {e}; exchange stops preserved")
            if "10006" in str(e):
                RATE_COOLDOWN["until"]=time.time()+60
                log_action("LOG risk manager: API rate limit - pausing exchange calls 60s (exchange stops preserved)")
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
