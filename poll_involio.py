"""
Involio delta poller (REAL API) - standalone backup for the Base44 poll.

Auth: reusable refresh token -> GET /v1_0/auth/refresh_token -> access token.
Books: POST /v1_0/investments/get_investments (+ _sims) per portfolio.

Usage:
    INVOLIO_REFRESH_TOKEN=<token> python3 poll_involio.py

Optional env:
    WEBHOOK_URL, WEBHOOK_SHARED_SECRET  - fire the VPS webhook (same payload
                                          format as the Base44 function)
    BASE44_APP_ID, BASE44_API_KEY       - record deltas to the InvolioDelta entity

State is kept in last_state.json so exact deltas (new/close/size/sltp) are
computed locally.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

BASE = "https://api.involio.com"
STATE_FILE = "last_state.json"

REFRESH_TOKEN = os.environ.get("INVOLIO_REFRESH_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
WEBHOOK_SHARED_SECRET = os.environ.get("WEBHOOK_SHARED_SECRET", "")
BASE44_APP_ID = os.environ.get("BASE44_APP_ID", "")
BASE44_API_KEY = os.environ.get("BASE44_API_KEY", "")

TRADERS = {
    "limpan96": "0e123c39-1b77-4163-867e-f6e153da4946",      # Scalping
    "nathanbrown": "e402a68a-0fdc-4372-b402-5ead36277146",   # The Bakery
    "akira": "72813ea9-35db-4ffe-b0b1-2d71cbbac837",         # Crypto
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}


def get_access_token() -> str:
    r = requests.get(
        f"{BASE}/v1_0/auth/refresh_token",
        headers={**HEADERS, "Authorization": f"Bearer {REFRESH_TOKEN}"},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["accessToken"]


def poll_book(access: str, portfolio_id: str) -> dict:
    auth = {"Authorization": f"Bearer {access}"}
    inv = requests.post(
        f"{BASE}/v1_0/investments/get_investments",
        headers={**HEADERS, **auth},
        json={"portfolioId": portfolio_id, "isOpen": True, "params": {"page": 1, "size": 50}},
        timeout=15,
    )
    inv.raise_for_status()
    sims = requests.post(
        f"{BASE}/v1_0/investments/get_investments_sims",
        headers={**HEADERS, **auth},
        json={"portfolioId": portfolio_id},
        timeout=15,
    )
    sims.raise_for_status()
    sizes = {s["ticker"]: s for s in sims.json().get("investments", [])}
    positions = []
    for p in inv.json().get("investmentsTicker", []):
        s = sizes.get(p["ticker"], {})
        positions.append({
            "ticker": p["ticker"],
            "side": "long" if p.get("directionLong") else "short",
            "leverage": p.get("leverage"),
            "entry_price": p.get("entryPrice"),
            "current_price": p.get("currentPrice"),
            "price_target": p.get("priceTarget"),
            "stop_loss": p.get("stopLoss"),
            "created_at": p.get("createdAt"),
            "updated_at": p.get("updatedAt"),
            "entry_sim": s.get("entrySim"),
            "last_sim": s.get("lastSim"),
        })
    return {"positions": positions, "remaining_sim": sims.json().get("portfolioRemainingSim")}


def compute_deltas(prev_books: dict, books: dict) -> list:
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


def record_in_base44(trader: str, delta: dict) -> None:
    if not (BASE44_APP_ID and BASE44_API_KEY):
        return
    url = f"https://api.base44.com/apps/{BASE44_APP_ID}/entities/InvolioDelta"
    p = delta["position"]
    requests.post(
        url,
        headers={"api_key": BASE44_API_KEY},
        json={
            "profile": trader,
            "coin": p.get("ticker"),
            "side": p.get("side"),
            "delta_type": delta["type"],
            "price": p.get("current_price"),
            "value_usd": (p.get("entry_sim") or 0) * (p.get("leverage") or 0) or None,
            "is_demo": False,
            "payload": json.dumps(delta),
            "processed": False,
            "webhook_sent": False,
            "action_taken": "",
        },
        timeout=15,
    ).raise_for_status()


def fire_webhook(recents: dict, books: dict) -> None:
    if not WEBHOOK_URL:
        return
    headers = {"Content-Type": "application/json"}
    if WEBHOOK_SHARED_SECRET:
        headers["X-Signature"] = WEBHOOK_SHARED_SECRET
    requests.post(
        WEBHOOK_URL,
        headers=headers,
        json={
            "source": "standalone-poll",
            "fired_at": datetime.now(timezone.utc).isoformat(),
            "recents": recents,
            "books": books,
        },
        timeout=15,
    ).raise_for_status()


def main() -> int:
    if not REFRESH_TOKEN:
        print("INVOLIO_REFRESH_TOKEN env var required", file=sys.stderr)
        return 1

    state = json.load(open(STATE_FILE)) if os.path.exists(STATE_FILE) else {"books": {}}

    access = get_access_token()
    books = {trader: poll_book(access, pid) for trader, pid in TRADERS.items()}
    deltas = compute_deltas(state["books"], books)

    for d in deltas:
        print(f"{d['trader']}: {d['type']} {d['position']['ticker']}/{d['position']['side']}")
        record_in_base44(d["trader"], d)

    recents = {t: [p["ticker"] for p in b["positions"]] for t, b in books.items()} if deltas else {}
    if deltas and WEBHOOK_URL:
        try:
            fire_webhook(recents, books)
            print("webhook fired")
        except Exception as exc:
            print(f"webhook failed: {exc}", file=sys.stderr)

    state["books"] = books
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    json.dump(state, open(STATE_FILE, "w"), indent=2)
    print(f"done: {len(deltas)} deltas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
