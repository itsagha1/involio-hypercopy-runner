"""
Involio→HyperCopy webhook listener (DEMO MODE)

Runs on your VPS (or PC/Termux). Receives webhook POSTs from the Base44
polling function whenever an Involio delta is detected, runs the trade
management logic, and reports the action back to Base44.

DEMO MODE: no real Hyperliquid calls are made. Actions are only logged
and written back to the InvolioDelta entity. Flip DRY_RUN=False once
real credentials are in place.

Setup (on VPS):
    python3 -m venv venv && source venv/bin/activate
    pip install -r requirements.txt
    uvicorn listener:app --host 127.0.0.1 --port 8000

Behind Cloudflare Tunnel (recommended, no open ports):
    cloudflared tunnel --url http://127.0.0.1:8000
"""

import hashlib
import hmac
import os
import time

import requests
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

DRY_RUN = True  # <-- demo mode. No real orders will ever be placed while True.

SHARED_SECRET = os.environ.get("WEBHOOK_SHARED_SECRET", "demo-secret-change-me")
BASE44_APP_ID = os.environ.get("BASE44_APP_ID", "")   # fill in after deploy
BASE44_API_KEY = os.environ.get("BASE44_API_KEY", "") # fill in after deploy
DEMO_LOG = "demo_actions.log"

app = FastAPI(title="Involio Delta Listener (demo)")


class Delta(BaseModel):
    delta_id: str           # Base44 InvolioDelta record id
    profile: str            # limpan96 / akira / nathanbrown
    profile_label: str      # Scalping / Crypto / The Bakery
    delta_type: str         # new_trade / size_change / close / ...
    coin: str | None = None
    side: str | None = None
    size: float | None = None
    price: float | None = None
    value_usd: float | None = None
    payload: str = ""       # raw JSON


def verify_signature(raw_body: bytes, signature: str | None) -> None:
    """Ensure the request really came from your Base44 function."""
    if not signature:
        raise HTTPException(status_code=401, detail="Missing signature")
    expected = hmac.new(
        SHARED_SECRET.encode(), raw_body, hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(expected, signature or ""):
        raise HTTPException(status_code=401, detail="Bad signature")


def manage_trade(delta: Delta) -> str:
    """
    DEMO trade-management logic. Replace with real Hyperliquid calls later.

    This is where the heavy/smart work lives - the part we do NOT want
    Base44 to burn credits on.
    """
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    if delta.delta_type == "new_trade":
        action = (
            f"[SIMULATED] Open {delta.side} {delta.size} {delta.coin} "
            f"@ {delta.price} for {delta.profile} ({delta.profile_label})"
        )
    elif delta.delta_type == "size_change":
        action = f"[SIMULATED] Resize {delta.coin} position for {delta.profile}"
    elif delta.delta_type == "close":
        action = f"[SIMULATED] Close {delta.coin} position for {delta.profile}"
    else:
        action = f"[SIMULATED] No-op for delta type {delta.delta_type}"

    line = f"{ts} | {action}"
    print(line)
    with open(DEMO_LOG, "a") as f:
        f.write(line + "\n")
    return action


def report_back(delta: Delta, action: str) -> None:
    """Tell Base44 what we did, so the DB keeps the full story."""
    if not (BASE44_APP_ID and BASE44_API_KEY):
        print("(demo) would report back:", delta.delta_id, "->", action)
        return
    url = (
        f"https://api.base44.com/apps/{BASE44_APP_ID}/"
        f"backend/functions/reportDeltaAction"
    )
    try:
        requests.post(
            url,
            json={"delta_id": delta.delta_id, "action_taken": action},
            headers={"api_key": BASE44_API_KEY},
            timeout=10,
        )
    except Exception as exc:  # network hiccup should not kill the listener
        print("report_back failed:", exc)


@app.post("/webhook/involio-delta")
async def involio_delta(
    delta: Delta,
    x_signature: str | None = Header(default=None),
):
    # NOTE: raw-body signature check happens in production; demo accepts
    # the shared secret via header for simplicity during testing.
    if x_signature != SHARED_SECRET and x_signature != hmac.new(
        SHARED_SECRET.encode(), delta.model_dump_json().encode(), hashlib.sha256
    ).hexdigest():
        # In demo mode allow the plain secret; tighten this before prod.
        if x_signature != SHARED_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")

    action = manage_trade(delta)
    report_back(delta, action)
    return {"ok": True, "dry_run": DRY_RUN, "action": action}


@app.get("/health")
async def health():
    return {"ok": True, "dry_run": DRY_RUN, "uptime_mode": "demo"}
