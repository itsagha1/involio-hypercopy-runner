"""
Involio -> Bybit HyperCopy VPS listener (REAL DATA, DRY_RUN by default)

Receives webhook POSTs from the Base44 `pollInvolioDeltas` function whenever an
Involio trader position shows activity. Keeps the last-known books, computes
exact deltas, and applies the manage-only copybot rules.

MANAGE-ONLY RULES (owner directive 2026-09-22):
  - NEW INVOLIO ENTRIES ARE PAUSED. No new symbols, no side flips.
  - Only manage positions that are ALREADY OPEN on Bybit sub-account
    AIsub587820763 at run start. Absent symbol/side -> skip, log
    new_entries_paused.
  - NEVER realize a loss: when the source drops a position, close only if
    unrealisedPnl is comfortably positive; otherwise set a Bybit trailing
    stop (activation avgPrice x1.015 long / x0.985 short, distance 1%).
  - Sizing base = full sub-account equity per trader, max_trade 50 USDT,
    min 2 USDT.

DRY_RUN=True (default): no Bybit orders are placed; every planned action is
logged to actions.log and to the console. Bybit keys live only in the local
.env on the VPS - never in Base44.

Run:
    uvicorn listener:app --host 127.0.0.1 --port 8000
"""

import hmac
import json
import os
from datetime import datetime, timezone

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"
SHARED_SECRET = os.environ.get("WEBHOOK_SHARED_SECRET", "")
STATE_FILE = os.environ.get("STATE_FILE", "vps_state.json")
LOG_FILE = os.environ.get("LOG_FILE", "actions.log")

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


def evaluate(delta: dict, open_mirrors: set) -> str:
    """Apply the manage-only rules. open_mirrors = set of 'TRADER/COIN/SIDE'
    currently open on Bybit (filled by the Bybit poller once live)."""
    p = delta["position"]
    trader = delta["trader"]
    key = f"{trader}/{p['ticker']}/{p['side']}"
    t = delta["type"]
    if t in ("new_entry", "size_add") and key not in open_mirrors:
        return f"SKIP {key} {t}: new_entries_paused (no open mirror)"
    if t in ("size_add", "size_reduce", "sltp_change"):
        if key in open_mirrors:
            return f"TODO {key} {t}: mirror size/SLTP sync (sizing: full equity/trader, max 50, min 2 USDT)"
        return f"SKIP {key} {t}: no open mirror"
    if t == "source_close":
        if key in open_mirrors:
            return (f"NO-LOSS RULE {key} source_close: close only if unrealisedPnl comfortably > 0; "
                    "else set Bybit trailing stop (act = avgPrice x1.015 long / x0.985 short, dist 1%)")
        return f"SKIP {key} source_close: no open mirror"
    return f"UNKNOWN {key} {t}"


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

    # Until Bybit polling is wired in, open_mirrors is empty: everything new
    # is skipped under the new-entries-paused rule. When live, this set is
    # loaded from the Bybit position list at the start of each run.
    open_mirrors: set = set()

    for d in deltas:
        action = evaluate(d, open_mirrors)
        log_action(f"DRY_RUN={DRY_RUN} :: {action}" if DRY_RUN else action)

    state["books"] = payload.books
    state["last_webhook"] = payload.fired_at
    state["last_processed"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state["deltas_last_run"] = len(deltas)
    save_state(state)
    return {"ok": True, "deltas": len(deltas), "dry_run": DRY_RUN}


@app.get("/health")
async def health():
    state = load_state()
    return {
        "ok": True,
        "dry_run": DRY_RUN,
        "traders": list(state.get("books", {}).keys()),
        "last_webhook": state.get("last_webhook"),
        "positions": {t: len(b.get("positions", [])) for t, b in state.get("books", {}).items()},
    }
