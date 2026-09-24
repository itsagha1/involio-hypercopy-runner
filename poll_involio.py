"""
Involio delta poller (DEMO MODE) - runs on GitHub Actions / PC / Termux.

Polls the three Involio profiles, detects deltas since the last run,
records them in the Base44 InvolioDelta entity, and fires the VPS webhook.

DEMO MODE: Involio endpoints are placeholders (INDEM). We fill these in
after inspecting the app in the live browser. State is kept in
last_state.json (committed back to the repo in GitHub Actions).
"""

import hashlib
import json
import os
import sys
import time

import requests

DRY_RUN = True
STATE_FILE = "last_state.json"
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL", "290"))

# --- Base44 config (set as GitHub secrets) ---
BASE44_APP_ID = os.environ.get("BASE44_APP_ID", "")
BASE44_API_KEY = os.environ.get("BASE44_API_KEY", "")

# --- VPS webhook config (set as GitHub secrets) ---
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
WEBHOOK_SHARED_SECRET = os.environ.get("WEBHOOK_SHARED_SECRET", "demo-secret-change-me")

PROFILES = [
    {"name": "limpan96", "label": "Scalping", "url": "https://app.invoapp.com/limpan96"},
    {"name": "akira", "label": "Crypto", "url": "https://app.invoapp.com/akira"},
    {"name": "nathanbrown", "label": "The Bakery", "url": "https://app.invoapp.com/nathanbrown"},
]

# TODO: fill in with the real endpoints we discover from the live browser session.
INDELIO_ENDPOINT_TEMPLATE = "https://app.invoapp.com/api/{profile}/trades"


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"profiles": {}}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def fetch_profile(profile: dict) -> list:
    """Fetch current positions/trades for a profile. Placeholder for now."""
    # DEMO: return fake data until real endpoints are wired in.
    return [
        {"coin": "BTC", "side": "long", "size": 0.5, "price": 65000.0,
         "type": "position", "ts": int(time.time())},
    ]


def compute_deltas(previous: list, current: list) -> list:
    """Diff two snapshots of a profile's trades -> list of delta dicts."""
    def key(t):
        return (t.get("coin"), t.get("side"))

    prev_map = {key(t): t for t in previous or []}
    curr_map = {key(t): t for t in current or []}

    deltas = []
    for k, t in curr_map.items():
        p = prev_map.get(k)
        if p is None:
            deltas.append({"delta_type": "new_trade", **t})
        elif p.get("size") != t.get("size"):
            deltas.append({"delta_type": "size_change", **t})
    for k, p in prev_map.items():
        if k not in curr_map:
            deltas.append({"delta_type": "close", **p})
    return deltas


def record_in_base44(profile: dict, delta: dict) -> str | None:
    """Write the delta to the Base44 InvolioDelta entity. Returns record id."""
    if not (BASE44_APP_ID and BASE44_API_KEY):
        print("(demo) would record in Base44:", profile["name"], delta)
        return None
    url = f"https://api.base44.com/apps/{BASE44_APP_ID}/entities/InvolioDelta"
    body = {
        "profile": profile["name"],
        "profile_label": profile["label"],
        "delta_type": delta["delta_type"],
        "coin": delta.get("coin"),
        "side": delta.get("side"),
        "size": delta.get("size"),
        "price": delta.get("price"),
        "is_demo": True,
        "payload": json.dumps(delta),
        "delta_hash": hashlib.sha256(json.dumps(delta, sort_keys=True).encode()).hexdigest(),
        "webhook_sent": False,
        "processed": False,
    }
    resp = requests.post(url, json=body, headers={"api_key": BASE44_API_KEY}, timeout=15)
    resp.raise_for_status()
    return resp.json().get("id")


def fire_webhook(delta_id: str | None, profile: dict, delta: dict) -> None:
    if not WEBHOOK_URL:
        print("(demo) no WEBHOOK_URL set; skipping webhook")
        return
    payload = {
        "delta_id": delta_id,
        "profile": profile["name"],
        "profile_label": profile["label"],
        **delta,
    }
    resp = requests.post(
        WEBHOOK_URL,
        json=payload,
        headers={"X-Signature": WEBHOOK_SHARED_SECRET},  # tightened for prod
        timeout=15,
    )
    print("webhook ->", resp.status_code)


def main() -> int:
    state = load_state()
    any_delta = False

    for profile in PROFILES:
        try:
            current = fetch_profile(profile)
        except Exception as exc:
            print(f"poll failed for {profile['name']}: {exc}")
            continue

        previous = state["profiles"].get(profile["name"], [])
        deltas = compute_deltas(previous, current)
        state["profiles"][profile["name"]] = current

        for delta in deltas:
            any_delta = True
            delta_id = record_in_base44(profile, delta)
            fire_webhook(delta_id, profile, delta)

    save_state(state)
    print("poll done, deltas:", "yes" if any_delta else "none")
    return 0


if __name__ == "__main__":
    sys.exit(main())
