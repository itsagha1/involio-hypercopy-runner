"""
Involio -> Bybit HyperCopy VPS listener (REAL DATA, DRY_RUN by default)

Receives webhook POSTs from the Base44 `pollInvolioDeltas` function whenever an
Involio trader position shows activity. Keeps the last-known books, computes
exact deltas, and applies the manage-only copybot rules.

MANAGE-ONLY RULES (owner directive 2026-09-22):
  - NEW INVOLIO ENTRIES ARE PAUSED. No new symbols, no side flips.
  - Only manage positions that are ALREADY OPEN on the Bybit sub-account
    at run start. Absent symbol/side -> skip, log new_entries_paused.
  - NEVER realize a loss: when the source drops a position, close only if
    unrealisedPnl is comfortably positive; otherwise set a Bybit trailing
    stop (activation avgPrice x1.015 long / x0.985 short, distance 1%).
  - Sizing base = full sub-account equity per trader, max_trade 50 USDT,
    min 2 USDT.

DRY_RUN=True (default): no Bybit orders are placed; every planned action is
logged to actions.log and to the console. Bybit keys live only in the local
.env on the VPS - never in Base44. Bybit calls require the Webshare Spain
proxy (BYBIT_PROXY) because Bybit geo-blocks UAE IPs.

Run:
    uvicorn listener:app --host 127.0.0.1 --port 8000
"""

import hmac
import json
import os
from datetime import datetime, timezone

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from bybit_api import (BybitClient, BybitError, close_side, coin_to_symbol,
                       position_idx, symbol_to_coin)

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"
SHARED_SECRET = os.environ.get("WEBHOOK_SHARED_SECRET", "")
STATE_FILE = os.environ.get("STATE_FILE", "vps_state.json")
LOG_FILE = os.environ.get("LOG_FILE", "actions.log")

# No-loss close threshold: source dropped the position; we only realize
# if unrealised PnL is comfortably positive (>= 0.5% of position value).
PNL_CLOSE_MIN_RATIO = 0.005

app = FastAPI(title="Involio HyperCopy Listener")


class WebhookPayload(BaseModel):
    source: str
    fired_at: str
    recents: dict = {}
    books: dict


def log_action(line: str) -> None:
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    entry = f"[{stamp}] {line}"
    print(entry, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(entry + "\n")


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"books": {}, "last_webhook": None}


def save_state(state: dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_FILE)


def compute_deltas(prev_books: dict, books: dict) -> list:
    """Diff two book snapshots per trader -> list of delta dicts."""
    deltas = []
    for trader, book in books.items():
        prev = {p["ticker"] + "/" + p["side"]: p for p in prev_books.get(trader, {}).get("positions", [])}
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
            elif old.get("stop_loss") != p.get("stop_loss") or old.get("price_target") != p.get("price_target"):
                deltas.append({"trader": trader, "type": "sltp_change", "position": p})
        for key, old in prev.items():
            if key not in curr_keys:
                deltas.append({"trader": trader, "type": "source_close", "position": old})
    return deltas


def fetch_open_mirrors(client: BybitClient) -> dict:
    """Live Bybit positions -> {'COIN/side': position_dict}."""
    mirrors = {}
    for pos in client.get_positions():
        if pos.get("size") in ("0", 0, 0.0, None, ""):
            continue
        side = "long" if pos.get("side") == "Buy" else "short"
        mirrors[symbol_to_coin(pos["symbol"]) + "/" + side] = pos
    return mirrors


def execute_source_close(client: BybitClient, delta: dict, mirror: dict) -> str:
    """No-loss rule: close only if comfortably positive, else trail."""
    coin = delta["position"]["ticker"]
    side = delta["position"]["side"]
    symbol = coin_to_symbol(coin)
    idx = int(mirror.get("positionIdx") or 0) or position_idx(symbol, side)
    try:
        pnl = float(mirror.get("unrealisedPnl") or 0)
        size = float(mirror.get("size") or 0)
        avg = float(mirror.get("avgPrice") or 0)
        value = size * avg
        if pnl > 0 and value > 0 and (pnl / value) >= PNL_CLOSE_MIN_RATIO:
            client.place_order(symbol, close_side(side), size,
                               position_idx=idx, reduce_only=True)
            return (f"CLOSED {coin}/{side} source_close: pnl={pnl:.4f} "
                    f"({pnl / value * 100:.2f}% of {value:.2f}) - market close")
        act = avg * (1.015 if side == "long" else 0.985)
        client.set_trailing_stop(symbol, act, 1.0, position_idx=idx)
        return (f"TRAIL {coin}/{side} source_close: pnl={pnl:.4f} not comfortably positive "
                f"-> trailing stop act={act:.2f} dist=1%")
    except BybitError as e:
        return f"ERROR {coin}/{side} source_close: {e}"


def evaluate(delta: dict, open_mirrors: dict):
    """Apply the manage-only rules. open_mirrors = {'COIN/side': bybit_position}.
    Returns (action_text, mirror_key_or_None)."""
    p = delta["position"]
    coin, side, trader = p["ticker"], p["side"], delta["trader"]
    key = f"{coin}/{side}"
    t = delta["type"]
    if t in ("new_entry", "size_add") and key not in open_mirrors:
        return f"SKIP {trader}/{key} {t}: new_entries_paused (no open mirror)", None
    if t in ("size_add", "size_reduce", "sltp_change"):
        if key in open_mirrors:
            return (f"LOG {trader}/{key} {t}: mirror exists, sync deferred "
                    f"(sizing: full equity/trader, max 50, min 2 USDT)"), None
        return f"SKIP {trader}/{key} {t}: no open mirror", None
    if t == "source_close":
        if key in open_mirrors:
            return f"NO_LOSS_RULE {trader}/{key} source_close (mirror live)", key
        return f"SKIP {trader}/{key} source_close: no open mirror", None
    return f"UNKNOWN {trader}/{key} {t}", None


@app.post("/webhook/involio-delta")
async def involio_delta(payload: WebhookPayload, x_signature: str = Header(default="")):
    if not SHARED_SECRET:
        log_action("REJECT: WEBHOOK_SHARED_SECRET not set on VPS")
        raise HTTPException(500, "listener secret not configured")
    if not hmac.compare_digest(x_signature, SHARED_SECRET):
        log_action("REJECT: bad signature")
        raise HTTPException(403, "bad signature")

    state = load_state()
    deltas = compute_deltas(state.get("books", {}), payload.books)

    client = BybitClient()
    open_mirrors: dict = {}
    if not DRY_RUN and client.configured:
        try:
            open_mirrors = fetch_open_mirrors(client)
            log_action(f"LIVE: {len(open_mirrors)} open Bybit mirror(s): {sorted(open_mirrors)}")
        except BybitError as e:
            log_action(f"BYBIT POLL FAILED (treating as no mirrors): {e}")

    executed = 0
    for d in deltas:
        action, mirror_key = evaluate(d, open_mirrors)
        if not DRY_RUN and mirror_key is not None:
            action = execute_source_close(client, d, open_mirrors[mirror_key])
            executed += 1
        log_action(f"DRY_RUN={DRY_RUN} :: {action}" if DRY_RUN else action)

    state["books"] = payload.books
    state["last_webhook"] = payload.fired_at
    state["last_processed"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state["deltas_last_run"] = len(deltas)
    state["live_mode"] = not DRY_RUN
    save_state(state)
    return {"ok": True, "deltas": len(deltas), "executed": executed, "dry_run": DRY_RUN}


@app.get("/health")
async def health():
    state = load_state()
    return {
        "ok": True,
        "dry_run": DRY_RUN,
        "bybit_keys": "set" if BybitClient().configured else "missing",
        "proxy": "set" if os.environ.get("BYBIT_PROXY") else "missing",
        "traders": list(state.get("books", {}).keys()),
        "last_webhook": state.get("last_webhook"),
        "positions": {t: len(b.get("positions", [])) for t, b in state.get("books", {}).items()},
    }
