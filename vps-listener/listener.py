# Involio -> Bybit HyperCopy VPS listener v3 (FULL MIRROR mode)
#
# Receives webhook POSTs from the Base44 `pollInvolioDeltas` function (fired
# whenever Involio trader positions show activity). Diffs the books and applies
# the owner rules (directive 2026-09-26, amended 2026-09-27):
#
# RULES:
#   1. FULL MIRRORING of NEW trades from all 3 Involio profiles. FRESH START:
#      every position already open on Involio at the first run after this
#      update is recorded as baseline and NEVER mirrored or managed.
#      A baseline symbol/side the source later CLOSED and RE-OPENED is a NEW
#      trade: it is un-baselined automatically and becomes mirror-eligible.
#   2. Mirrored trades are followed religiously: source size adds/reduces
#      are copied to Bybit, SL/TP changes are synced to Bybit. BUT at most
#      MAX_ADDS_PER_TRADE size increases are copied per trade; further adds
#      are skipped (owner cap, 2026-09-27).
#   3. A new trade already >= 3% profit (sim ratio last_sim/entry_sim) at
#      detection is SKIPPED as too late. Negative or < 3%: mirrored.
#   4. Trade ratio is ALWAYS 1:1: our notional (entry_sim x leverage USDT)
#      matches the source trade's notional; our leverage matches theirs,
#      capped at MAX_LEVERAGE (owner cap, 2026-09-27).
#   5. NO-LOSS RULE kept: when a source position closes, our share closes
#      only if unrealised PnL is comfortably positive (>= 0.5% of value);
#      otherwise a Bybit trailing stop is set (activation avgPrice x1.015
#      long / x0.985 short, 1% distance).
#   6. MARGIN CAP (owner rule, 2026-09-27): total committed margin must stay
#      <= MARGIN_CAP (70%) of the wallet. New trades and size adds are
#      scaled down (or skipped) to respect the remaining budget.
#   7. SYMBOL UNIQUENESS (owner rule, 2026-09-27): if a symbol is already
#      mirrored by one Involio profile, a new trade on that symbol from any
#      OTHER profile is skipped.
#   8. ORPHAN DEFENSE (owner rule, 2026-09-27): state is saved after EVERY
#      executed order; a mirror is only deregistered after its Bybit position
#      is missing in TWO consecutive successful polls; live Bybit positions
#      that no mirror record tracks are reported as ORPHANS, and positions
#      whose live qty exceeds the tracked mirrors are reported as MISMATCHES
#      (alerts only - the listener NEVER auto-touches them). Pre-existing
#      untracked positions are adopted once as owner-managed (manual) and
#      never auto-traded.
#   9. /status endpoint feeds the Base44 daily health check (9am Dubai) and
#      the 15-min activity alert collector.
#  10. Durable state file survives restarts -> 24/7 operation.
#
# Sizing facts (verified against live Involio data 2026-09-26):
#   - Involio sims are MARGIN in USD; notional = sim x leverage.
#   - qty(source) = (last_sim x leverage) / current_price (constant through
#     pure PnL; changes only on real adds/reduces).
#
# DRY_RUN=true: no orders are placed; everything is logged. Set DRY_RUN=false
# in .env to go live. Bybit keys live only in the local .env on the VPS.
#
# Run:
#     uvicorn listener:app --host 127.0.0.1 --port 8000

import hmac
import json
import os
from datetime import datetime, timezone

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from bybit_api import (BybitClient, BybitError, bybit_side, close_side,
                       coin_to_symbol, position_idx, round_qty,
                       symbol_to_coin)

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"
SHARED_SECRET = os.environ.get("WEBHOOK_SHARED_SECRET", "")
STATE_FILE = os.environ.get("STATE_FILE", "vps_state.json")
LOG_FILE = os.environ.get("LOG_FILE", "actions.log")

STATE_VERSION = 3
LISTENER_VERSION = "v3.0.0"   # bump on every deployed change; exposed in /status
PNL_CLOSE_MIN_RATIO = 0.005      # no-loss: realize only if >= 0.5% of value
PROFIT_SKIP_RATIO = 1.03         # rule 3: skip new trades already +3%
SIZE_NOISE_BAND = (0.85, 1.15)   # sim/price wobble that is pure PnL
MIN_NOTIONAL = 5.0               # Bybit linear minimum order notional (USDT)
MAX_LEVERAGE = 20                # owner cap 2026-09-27
MARGIN_CAP = 0.70                # owner cap 2026-09-27: max share of wallet committed
MAX_ADDS_PER_TRADE = 2           # owner cap 2026-09-27: copied size increases per trade
MISMATCH_MIN_USDT = 2.0          # orphan/mismatch reporting sensitivity
MISMATCH_QTY_FRAC = 0.05         # excess qty tolerance (5% of live qty)
LOG_TAIL_LINES = 60

app = FastAPI(title="Involio HyperCopy Listener v3")


class WebhookPayload(BaseModel):
    source: str
    fired_at: str
    recents: dict = {}
    books: dict


# ---------------------------------------------------------------- helpers

def log_action(line: str) -> None:
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    entry = f"[{stamp}] {line}"
    print(entry, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(entry + "\n")
    except OSError:
        pass


def log_tail(n: int = LOG_TAIL_LINES) -> list:
    try:
        with open(LOG_FILE) as f:
            return f.read().splitlines()[-n:]
    except OSError:
        return []


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                s = json.load(f)
            if s.get("version") == STATE_VERSION:
                return s
            if s.get("version") == 2:
                # v2 -> v3 migration: keep everything, add new registries.
                s["version"] = STATE_VERSION
                s.setdefault("manual", [])
                s.setdefault("manual_adopted", False)
                s.setdefault("orphans", {})
                s.setdefault("mismatches", {})
                s.setdefault("dereg_pending", [])
                for rec in s.get("mirrored", {}).values():
                    rec.setdefault("adds", 0)
                return s
        except (json.JSONDecodeError, OSError):
            pass
    return {"version": STATE_VERSION, "baseline": [], "mirrored": {},
            "books": {}, "last_webhook": None, "fresh_start_at": None,
            "last_processed": None, "deltas_last_run": 0, "errors_last_run": 0,
            "manual": [], "manual_adopted": False, "orphans": {},
            "mismatches": {}, "dereg_pending": []}


def save_state(state: dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_FILE)


def tkey(trader: str, p: dict) -> str:
    return f"{trader}|{p['ticker']}/{p['side']}"


def mirror_bybit_key(k: str) -> str:
    """'trader|COIN/side' -> 'COIN/side'."""
    return k.split("|", 1)[1]


def source_notional(p: dict) -> float:
    """Involio notional in USDT = margin (sim) x leverage."""
    return float(p.get("entry_sim") or 0) * float(p.get("leverage") or 1)


def profit_ratio(p: dict) -> float:
    es = float(p.get("entry_sim") or 0)
    ls = float(p.get("last_sim") or 0)
    if es <= 0:
        return 1.0
    return (ls or es) / es


def parse_ts(ts):
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def is_reopened_position(p: dict, fresh_start_at) -> bool:
    """True if the source position was CREATED after our fresh start,
    i.e. a baseline key whose trade closed and re-opened (a NEW trade)."""
    created = parse_ts(p.get("created_at"))
    fresh = parse_ts(fresh_start_at)
    return bool(created and fresh and created > fresh)


def symbol_mirrored_by_others(mirrored: dict, trader: str, coin: str) -> list:
    """Profiles (other than `trader`) that already mirror `coin` (rule 7)."""
    owners = set()
    for k in mirrored:
        owner, bk = k.split("|", 1)[0], mirror_bybit_key(k)
        if owner != trader and bk.split("/")[0] == coin:
            owners.add(owner)
    return sorted(owners)


def margin_budget(client: BybitClient) -> float:
    """Extra USDT margin we may commit right now under the 70% wallet cap.

    budget = 0.70 x wallet - already-committed margin (wallet - available).
    Negative budget means the cap is exhausted: nothing new may be opened.
    """
    wallet = float(client.get_balance() or 0)
    avail = float(client.get_available_balance() or 0)
    used = max(0.0, wallet - avail)
    return MARGIN_CAP * wallet - used


def compute_deltas(prev_books: dict, books: dict) -> list:
    """Diff two book snapshots per trader -> list of delta dicts."""
    deltas = []
    for trader, book in books.items():
        prev = {p["ticker"] + "/" + p["side"]: p
                for p in prev_books.get(trader, {}).get("positions", [])}
        curr_keys = set()
        for p in book.get("positions", []):
            key = p["ticker"] + "/" + p["side"]
            curr_keys.add(key)
            old = prev.get(key)
            if old is None:
                deltas.append({"trader": trader, "type": "new_entry", "position": p})
            elif old.get("last_sim") != p.get("last_sim"):
                kind = "size_add" if (p.get("last_sim") or 0) > (old.get("last_sim") or 0) else "size_reduce"
                deltas.append({"trader": trader, "type": kind, "position": p})
            elif (old.get("stop_loss") != p.get("stop_loss")
                  or old.get("price_target") != p.get("price_target")):
                deltas.append({"trader": trader, "type": "sltp_change", "position": p})
        for key, old in prev.items():
            if key not in curr_keys:
                deltas.append({"trader": trader, "type": "source_close", "position": old})
    return deltas


def find_prev(prev_books: dict, trader: str, p: dict):
    for q in prev_books.get(trader, {}).get("positions", []):
        if q["ticker"] == p["ticker"] and q["side"] == p["side"]:
            return q
    return None


def fetch_open_mirrors(client: BybitClient) -> dict:
    """Live Bybit positions -> {'COIN/side': position_dict}."""
    mirrors = {}
    for pos in client.get_positions():
        if pos.get("size") in ("0", 0, 0.0, None, ""):
            continue
        side = "long" if pos.get("side") == "Buy" else "short"
        mirrors[symbol_to_coin(pos["symbol"]) + "/" + side] = pos
    return mirrors


def detect_unmanaged(state: dict, open_mirrors: dict):
    """Rule 8: live positions no mirror record tracks (orphans) and live
    positions exceeding their tracked mirror qty (mismatches)."""
    tracked: dict = {}
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

def open_mirror(client: BybitClient, trader: str, p: dict):
    """Open a 1:1 mirror for a new Involio entry. Returns (action_text, record|None)."""
    key = tkey(trader, p)
    coin, side = p["ticker"], p["side"]
    symbol = coin_to_symbol(coin)
    pr = profit_ratio(p)
    if pr >= PROFIT_SKIP_RATIO:
        return (f"SKIP {key} new_entry: already +{(pr - 1) * 100:.1f}% profit "
                f"(>= 3%) - too late to mirror"), None
    notional = source_notional(p)
    if notional < MIN_NOTIONAL:
        return (f"SKIP {key} new_entry: notional {notional:.2f} USDT "
                f"< min {MIN_NOTIONAL:.0f} USDT"), None
    try:
        ticker = client.get_ticker(symbol)
        price = float(ticker["lastPrice"])
        inst = client.get_instrument(symbol)
        lev = int(float(p.get("leverage") or 1)) or 1
        lev = max(1, min(lev, MAX_LEVERAGE))
        avail = client.get_available_balance()
        budget = margin_budget(client)
        if budget <= 0:
            return (f"SKIP {key} new_entry: {MARGIN_CAP:.0%} margin cap reached "
                    f"(available {avail:.2f} USDT)"), None
        margin_cap = min(avail * 0.98, budget)   # respect both caps
        max_notional = margin_cap * lev
        scaled = ""
        if notional > max_notional:
            scaled = (f" (scaled down from {source_notional(p):.2f}: "
                      f"budget {budget:.2f} USDT x lev {lev})")
            notional = max_notional
            if notional < MIN_NOTIONAL:
                return (f"SKIP {key} new_entry: insufficient margin "
                        f"(available {avail:.2f} USDT)"), None
        their_qty = notional / price
        qty = round_qty(their_qty, inst)
        client.set_leverage(symbol, lev)
        client.place_order(symbol, bybit_side(side), qty,
                           position_idx=position_idx(symbol, side))
        rec = {"symbol": symbol, "side": side, "qty": qty,
               "their_qty": their_qty, "notional": qty * price,
               "leverage": lev, "adds": 0,
               "opened_at": datetime.now(timezone.utc)
               .isoformat(timespec="seconds"),
               "entry_price": price}
        return (f"OPENED {key} 1:1 mirror: {qty} {coin} "
                f"(~{qty * price:.2f} USDT notional, lev {lev}){scaled}"), rec
    except BybitError as e:
        return f"ERROR {key} open_mirror: {e}", None


def sync_size(client: BybitClient, trader: str, p: dict, rec: dict, prev_p):
    """Rule 2: follow source size adds/reduces 1:1 (PnL noise filtered out),
    with the owner's add cap and margin cap applied to increases."""
    key = tkey(trader, p)
    symbol, side = rec["symbol"], rec["side"]
    coin = symbol[:-4]
    if prev_p is None:
        return f"LOG {key} size: no previous snapshot to compare"
    ls_n, ls_p = float(p.get("last_sim") or 0), float(prev_p.get("last_sim") or 0)
    pr_n, pr_p = float(p.get("current_price") or 0), float(prev_p.get("current_price") or 0)
    if min(ls_n, ls_p, pr_n, pr_p) <= 0:
        return f"LOG {key} size: incomplete sim/price data, skipped sync"
    ratio = (ls_n / ls_p) * (pr_p / pr_n)   # source qty change estimate
    rec["their_qty"] = rec.get("their_qty", rec.get("qty", 0.0)) * ratio
    lo, hi = SIZE_NOISE_BAND
    if lo <= ratio <= hi:
        return (f"LOG {key} size: sim wobble x{ratio:.4f} within noise band "
                f"(pure PnL, no trader size change)")
    try:
        price = float(client.get_ticker(symbol)["lastPrice"])
        delta_qty = rec["their_qty"] - rec.get("qty", 0.0)
        if abs(delta_qty) * price < MIN_NOTIONAL:
            return (f"LOG {key} size: real change x{ratio:.3f} but delta "
                    f"{delta_qty:.6f} {coin} (~{abs(delta_qty) * price:.2f} USDT) "
                    f"below min notional - deferred")
        inst = client.get_instrument(symbol)
        dqty = round_qty(abs(delta_qty), inst)
        idx = position_idx(symbol, side)
        if delta_qty > 0:
            if rec.get("adds", 0) >= MAX_ADDS_PER_TRADE:
                return (f"SKIP {key} size_add: add cap reached "
                        f"({MAX_ADDS_PER_TRADE} increases already copied, "
                        f"source x{ratio:.3f})")
            lev = int(rec.get("leverage") or 1) or 1
            budget = margin_budget(client)
            need_margin = dqty * price / lev
            if budget <= 0:
                return (f"SKIP {key} size_add: {MARGIN_CAP:.0%} margin cap "
                        f"reached, increase not copied")
            if need_margin > budget:
                dqty = round_qty(budget * lev / price, inst)
                if dqty <= 0 or dqty * price < MIN_NOTIONAL:
                    return (f"SKIP {key} size_add: increase larger than "
                            f"remaining margin budget {budget:.2f} USDT")
            client.place_order(symbol, bybit_side(side), dqty, position_idx=idx)
            rec["qty"] = rec.get("qty", 0.0) + dqty
            rec["adds"] = rec.get("adds", 0) + 1
            return (f"ADDED {key} 1:1 follow: +{dqty} {coin} "
                    f"(now {rec['qty']}, source x{ratio:.3f}, "
                    f"add {rec['adds']}/{MAX_ADDS_PER_TRADE})")
        client.place_order(symbol, close_side(side), dqty,
                           position_idx=idx, reduce_only=True)
        rec["qty"] = max(0.0, rec.get("qty", 0.0) - dqty)
        return (f"REDUCED {key} 1:1 follow: -{dqty} {coin} "
                f"(now {rec['qty']}, source x{ratio:.3f})")
    except BybitError as e:
        return f"ERROR {key} sync_size: {e}"


def sync_sltp(client: BybitClient, trader: str, p: dict, rec: dict) -> str:
    """Rule 2: follow source SL/TP changes religiously."""
    key = tkey(trader, p)
    sl, tp = p.get("stop_loss"), p.get("price_target")
    if not sl and not tp:
        return f"LOG {key} sltp: source cleared stops; leaving ours in place"
    try:
        client.set_sl_tp(rec["symbol"], sl, tp,
                        position_idx=position_idx(rec["symbol"], rec["side"]))
        return f"SYNC {key} sltp: SL={sl} TP={tp}"
    except BybitError as e:
        return f"ERROR {key} sync_sltp: {e}"


def execute_source_close(client: BybitClient, delta: dict, rec: dict,
                         open_mirrors: dict) -> str:
    """Rule 5: no-loss close of our share when the source drops a position."""
    p = delta["position"]
    key = tkey(delta["trader"], p)
    symbol, side = rec["symbol"], rec["side"]
    coin = symbol[:-4]
    pos = open_mirrors.get(p["ticker"] + "/" + p["side"])
    if pos is None:
        return f"LOG {key} source_close: no live Bybit position (already gone)"
    idx = position_idx(symbol, side)
    try:
        pnl = float(pos.get("unrealisedPnl") or 0)
        size = float(pos.get("size") or 0)
        avg = float(pos.get("avgPrice") or 0)
        value = size * avg
        if pnl > 0 and value > 0 and (pnl / value) >= PNL_CLOSE_MIN_RATIO:
            qty = min(rec.get("qty", 0.0) or size, size)
            inst = client.get_instrument(symbol)
            dqty = round_qty(qty, inst)
            client.place_order(symbol, close_side(side), dqty,
                               position_idx=idx, reduce_only=True)
            return (f"CLOSED {key} source_close: pnl={pnl:.4f} "
                    f"({pnl / value * 100:.2f}% of {value:.2f}) - market close "
                    f"{dqty} {coin}")
        act = avg * (1.015 if side == "long" else 0.985)
        client.set_trailing_stop(symbol, act, 1.0, position_idx=idx)
        return (f"TRAIL {key} source_close: pnl={pnl:.4f} not comfortably "
                f"positive -> trailing stop act={act:.2f} dist=1%")
    except BybitError as e:
        return f"ERROR {key} source_close: {e}"


# ---------------------------------------------------------------- webhook

@app.post("/webhook/involio-delta")
async def involio_delta(payload: WebhookPayload, x_signature: str = Header(default="")):
    if not SHARED_SECRET:
        log_action("REJECT: WEBHOOK_SHARED_SECRET not set on VPS")
        raise HTTPException(500, "listener secret not configured")
    if not hmac.compare_digest(x_signature, SHARED_SECRET):
        log_action("REJECT: bad signature")
        raise HTTPException(403, "bad signature")

    state = load_state()

    # Fresh start (rule 1): record baseline on the first run after this update.
    if state.get("fresh_start_at") is None:
        baseline = [tkey(t, p) for t, book in payload.books.items()
                    for p in book.get("positions", [])]
        state["baseline"] = baseline
        state["fresh_start_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        state["books"] = payload.books
        state["last_webhook"] = payload.fired_at
        state["last_processed"] = state["fresh_start_at"]
        save_state(state)
        log_action(f"FRESH START: baseline recorded, {len(baseline)} existing "
                   f"Involio position(s) will NOT be mirrored: {baseline}")
        return {"ok": True, "fresh_start": True, "baseline": len(baseline)}

    prev_books = state.get("books", {})
    deltas = compute_deltas(prev_books, payload.books)

    client = BybitClient()
    open_mirrors: dict = {}
    bybit_ok = False
    errors = 0
    if not DRY_RUN and client.configured:
        try:
            open_mirrors = fetch_open_mirrors(client)
            bybit_ok = True
            log_action(f"LIVE: {len(open_mirrors)} open Bybit position(s): "
                       f"{sorted(open_mirrors)}")
        except BybitError as e:
            errors += 1
            log_action(f"BYBIT POLL FAILED (skipping reconcile): {e}")

    # Rule 8: deregister a mirror only when its Bybit position is missing in
    # TWO consecutive successful polls, so a transient API glitch or a
    # pagination hiccup never orphans a live position.
    if bybit_ok:
        missing_now = {mirror_bybit_key(k) for k in state["mirrored"]
                       if mirror_bybit_key(k) not in open_mirrors}
        pending = set(state.get("dereg_pending", []))
        for k in list(state["mirrored"].keys()):
            if mirror_bybit_key(k) in missing_now and mirror_bybit_key(k) in pending:
                log_action(f"DEREGISTER {k}: Bybit position gone in two "
                            f"consecutive polls (closed manually?) - leaving unmanaged")
                del state["mirrored"][k]
        state["dereg_pending"] = sorted(missing_now)

    # Rule 8: orphan / mismatch detection (alerts only, never auto-trades).
    if bybit_ok:
        orphans, mismatches = detect_unmanaged(state, open_mirrors)
        if not state.get("manual_adopted"):
            for bk, qty in orphans:
                state["manual"].append(bk)
                log_action(f"ADOPT-MANUAL {bk}: pre-existing untracked Bybit "
                           f"position (qty {qty}) registered as owner-managed, "
                           f"never auto-traded")
            state["manual_adopted"] = True
        else:
            now = datetime.now(timezone.utc).isoformat(timespec="seconds")
            for bk, qty in orphans:
                prev = state["orphans"].get(bk)
                if prev is None or qty > prev.get("last_alert_qty", 0) * 1.10:
                    log_action(f"ALERT ORPHAN {bk}: live Bybit position qty "
                               f"{qty} tracked by NO mirror - investigate")
                state["orphans"][bk] = {
                    "first_seen": (prev or {}).get("first_seen", now),
                    "qty": qty,
                    "last_alert_qty": qty if prev is None
                    else max(qty, prev.get("last_alert_qty", 0))}
            for bk, qty, expected in mismatches:
                prev = state["mismatches"].get(bk)
                if prev is None or qty > prev.get("last_alert_qty", 0) * 1.10:
                    log_action(f"ALERT MISMATCH {bk}: live qty {qty} exceeds "
                               f"tracked mirrors {expected:.4f} - investigate")
                state["mismatches"][bk] = {
                    "first_seen": (prev or {}).get("first_seen", now),
                    "qty": qty, "expected": expected,
                    "last_alert_qty": qty if prev is None
                    else max(qty, prev.get("last_alert_qty", 0))}

    executed = 0
    for d in deltas:
        p = d["position"]
        k = tkey(d["trader"], p)
        t = d["type"]

        # A source-close of a baseline trade frees the key: a later re-open
        # on the same symbol/side must be treated as a brand-new trade.
        if t == "source_close" and k in state.get("baseline", []):
            state["baseline"].remove(k)
            log_action(f"BASELINE-CLOSED {k}: removed from baseline "
                       f"(re-opens will be mirrored as new trades)")
            continue

        # A baseline key whose position was CREATED after the fresh start is
        # a closed-and-reopened trade (e.g. the WLD/STRK bug, 2026-09-26):
        # un-baseline it so it can be mirrored like any new trade.
        if k in state.get("baseline", []) and is_reopened_position(p, state.get("fresh_start_at")):
            state["baseline"].remove(k)
            log_action(f"UN-BASELINE {k}: source re-opened a NEW trade on this "
                       f"key (created {p.get('created_at')}) - eligible for mirroring")

        if k in state.get("baseline", []):
            log_action(f"SKIP {k} {t}: baseline position (fresh start, manual)")
            continue

        rec = state["mirrored"].get(k)

        # Close+reopen within one poll gap looks like a size change; if the
        # position is brand new (created after fresh start) treat it as new.
        if t in ("size_add", "size_reduce") and not rec \
                and is_reopened_position(p, state.get("fresh_start_at")):
            log_action(f"REOPEN {k}: source closed+re-opened within one poll "
                       f"gap - treating as new_entry")
            t = "new_entry"

        if t == "new_entry":
            if rec:
                log_action(f"LOG {k} new_entry: mirror already exists")
                continue
            # Rule 7: one symbol, one profile.
            others = symbol_mirrored_by_others(state["mirrored"], d["trader"], p["ticker"])
            if others:
                log_action(f"SKIP {k} new_entry: symbol {p['ticker']} already "
                            f"mirrored by profile(s) {','.join(others)}")
                continue
            if DRY_RUN or not client.configured:
                log_action(f"DRY_RUN :: WOULD OPEN {k} "
                           f"(notional {source_notional(p):.2f} USDT, "
                           f"profit {(profit_ratio(p) - 1) * 100:+.1f}%)")
            else:
                action, new_rec = open_mirror(client, d["trader"], p)
                if new_rec:
                    state["mirrored"][k] = new_rec
                    executed += 1
                    save_state(state)   # rule 8: persist immediately
                elif action.startswith("ERROR"):
                    errors += 1
                log_action(action)

        elif t in ("size_add", "size_reduce"):
            if not rec:
                log_action(f"SKIP {k} {t}: no mirror of this position")
                continue
            if DRY_RUN or not client.configured:
                log_action(f"DRY_RUN :: WOULD SYNC size {k}")
            else:
                action = sync_size(client, d["trader"], p, rec,
                                   find_prev(prev_books, d["trader"], p))
                if action.startswith(("ADDED", "REDUCED")):
                    executed += 1
                    save_state(state)   # rule 8: persist immediately
                elif action.startswith("ERROR"):
                    errors += 1
                log_action(action)

        elif t == "sltp_change":
            if not rec:
                log_action(f"SKIP {k} sltp_change: no mirror of this position")
                continue
            if DRY_RUN or not client.configured:
                log_action(f"DRY_RUN :: WOULD SYNC sltp {k} "
                           f"SL={p.get('stop_loss')} TP={p.get('price_target')}")
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
                log_action(f"DRY_RUN :: WOULD NO-LOSS CLOSE {k}")
            else:
                action = execute_source_close(client, d, rec, open_mirrors)
                if action.startswith(("CLOSED", "TRAIL")):
                    executed += 1
                    if action.startswith("CLOSED"):
                        del state["mirrored"][k]
                        save_state(state)   # rule 8: persist immediately
                elif action.startswith("ERROR"):
                    errors += 1
                log_action(action)

    state["books"] = payload.books
    state["last_webhook"] = payload.fired_at
    state["last_processed"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state["deltas_last_run"] = len(deltas)
    state["errors_last_run"] = errors
    save_state(state)
    return {"ok": True, "deltas": len(deltas), "executed": executed,
            "dry_run": DRY_RUN, "errors": errors}


# ---------------------------------------------------------------- endpoints

@app.get("/health")
async def health():
    state = load_state()
    return {
        "ok": True,
        "mode": "full_mirror",
        "dry_run": DRY_RUN,
        "bybit_keys": "set" if BybitClient().configured else "missing",
        "proxy": "set" if os.environ.get("BYBIT_PROXY") else "missing",
        "traders": list(state.get("books", {}).keys()),
        "last_webhook": state.get("last_webhook"),
        "positions": {t: len(b.get("positions", []))
                      for t, b in state.get("books", {}).items()},
    }


@app.get("/status")
async def status():
    """Rich snapshot for the Base44 daily health check and 15-min alerts."""
    state = load_state()
    out = {
        "ok": True,
        "mode": "full_mirror",
        "code_version": LISTENER_VERSION,
        "dry_run": DRY_RUN,
        "fresh_start_at": state.get("fresh_start_at"),
        "baseline_count": len(state.get("baseline", [])),
        "mirrored": state.get("mirrored", {}),
        "orphans": state.get("orphans", {}),
        "mismatches": state.get("mismatches", {}),
        "manual": state.get("manual", []),
        "books": {t: len(b.get("positions", []))
                  for t, b in state.get("books", {}).items()},
        "last_webhook": state.get("last_webhook"),
        "last_processed": state.get("last_processed"),
        "deltas_last_run": state.get("deltas_last_run"),
        "errors_last_run": state.get("errors_last_run"),
        "log_tail": log_tail(),
    }
    client = BybitClient()
    out["bybit_keys"] = "set" if client.configured else "missing"
    out["proxy"] = "set" if os.environ.get("BYBIT_PROXY") else "missing"
    if not DRY_RUN and client.configured:
        try:
            out["bybit"] = {
                "balance": client.get_balance(),
                "positions": [
                    {"symbol": p.get("symbol"), "side": p.get("side"),
                     "size": p.get("size"), "avgPrice": p.get("avgPrice"),
                     "unrealisedPnl": p.get("unrealisedPnl"),
                     "leverage": p.get("leverage")}
                    for p in client.get_positions()
                    if p.get("size") not in ("0", 0, 0.0, None, "")
                ],
            }
            wallet = out["bybit"]["balance"]
            avail = client.get_available_balance()
            used = max(0.0, wallet - avail)
            out["margin"] = {"wallet": wallet, "available": avail,
                             "used": used, "cap_ratio": MARGIN_CAP,
                             "budget": MARGIN_CAP * wallet - used,
                             "cap_pct_used": (used / wallet * 100) if wallet else None}
        except BybitError as e:
            out["bybit_error"] = str(e)
    return out
