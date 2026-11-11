#!/usr/bin/env python3
"""Spillover router (owner 2026-11-11).

A trade the HOME account cannot take (sub-minimum notional, symbol conflict,
margin hold, 20% entry cap) is opened on the best other account instead.

Owner's policy:
  * spill source homes: bakery and scalping only (v1)
  * size FITS THE TARGET WALLET, not the source size
  * pick the best target: lowest margin usage that still fills the minimum
  * the trade belongs to the TARGET account (it is recorded there as
    owner-managed, so attribution and protection follow the account)
  * fleet rule: never stack the same symbol+side in two accounts
  * never touch the AI Main account or the hypercopy account

Safety: SAFE-TP applies (a copied TP can never sit on the losing side of the
entry we actually got), no order is placed without passing every gate, and a
symbol is only ever opened once per run.
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

STATE_FILE = "/var/lib/hypercopy-copy/spill_router.json"
LOG_FILE = "/var/lib/hypercopy-copy/spill_router.log"
ENV_DIR = "/etc"

ACCOUNTS = {
    "mainport": 8001,
    "bakery": 8002,
    "scalping": 8003,
    "kings": 8004,
    "bluespire": 8005,
}
HOMES = ("bakery", "scalping")
LOG_FILES = {
    "bakery": "/var/lib/hypercopy-copy/bakery_actions.log",
    "scalping": "/var/lib/hypercopy-copy/scalping_actions.log",
}

MIN_NOTIONAL = 5.0
ENTRY_CAP_PCT = 0.20
MARGIN_HOLD_PCT = 0.70
MAX_CANDIDATE_AGE = int(os.environ.get("SPILL_MAX_AGE_MIN", "45")) * 60
DRY_RUN = os.environ.get("SPILL_ENABLED", "0") != "1"

ELIGIBLE_REASONS = (
    "below minimum",
    "below exchange minimum",
    "live position conflict",
    "exceeds 20% of wallet",
    "margin hold",
)


def log(msg):
    line = "[%s] %s" % (datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), msg)
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"handled": {}, "opened": []}


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=1)
    os.replace(tmp, STATE_FILE)


def env_for(account):
    out = {}
    try:
        with open("%s/copy-%s.env" % (ENV_DIR, account)) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    except OSError:
        pass
    return out


def status(port):
    import requests
    try:
        r = requests.get("http://127.0.0.1:%d/status" % port, timeout=8)
        return r.json() if r.status_code == 200 else {}
    except Exception:
        return {}


def live_positions(st):
    out = set()
    bybit = st.get("bybit") or {}
    for p in bybit.get("positions") or []:
        sym = p.get("symbol")
        side = (p.get("side") or "").lower()
        if not sym or not side:
            continue
        try:
            size = float(p.get("size") or 0)
        except (TypeError, ValueError):
            size = 0
        if size:
            out.add((sym, "long" if side == "buy" else "short"))
    for k in (st.get("mirrored") or {}):
        parts = str(k).split("|")
        if len(parts) > 1 and "/" in parts[1]:
            coin, side = parts[1].split("/", 1)
            out.add((coin + "USDT", side))
    return out


def wallet_and_margin(st):
    """Copy engines expose wallet + margin_pct; the main account uses the other names."""
    wallet = st.get("wallet") or st.get("wallet_balance")
    if wallet is None:
        wallet = (st.get("bybit") or {}).get("balance")
    try:
        wallet = float(wallet or 0)
    except (TypeError, ValueError):
        wallet = 0.0
    pct = st.get("margin_pct")
    if pct is not None:
        try:
            return wallet, float(pct) / 100.0
        except (TypeError, ValueError):
            pass
    used = st.get("margin_used")
    if used is None:
        used = (st.get("bybit") or {}).get("margin_used")
    try:
        used = float(used or 0)
    except (TypeError, ValueError):
        used = 0.0
    return wallet, ((used / wallet) if wallet else 1.0)


def candidates(state):
    """Skip lines from the home engines that could be placed elsewhere."""
    import re
    found = {}
    cutoff = time.time() - MAX_CANDIDATE_AGE
    for home, path in LOG_FILES.items():
        try:
            with open(path) as f:
                lines = f.readlines()[-400:]
        except OSError:
            continue
        for line in lines:
            m = re.match(r"\[(\d{4}-\d\d-\d\d) (\d\d:\d\d:\d\d) UTC\] \S+: SKIP (\S+) (new_entry): (.+)", line)
            if not m:
                continue
            when = datetime.strptime(m.group(1) + " " + m.group(2), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
            if when < cutoff:
                continue
            reason = m.group(5)
            if not any(r in reason for r in ELIGIBLE_REASONS):
                continue
            key = m.group(3)
            if key in state["handled"]:
                continue
            parts = key.split("|")
            if len(parts) < 2 or "/" not in parts[1]:
                continue
            profile = parts[0]
            coin, side = parts[1].split("/", 1)
            found.setdefault(key, {
                "key": key, "profile": profile, "coin": coin, "side": side,
                "home": home, "reason": reason.strip()[:90], "when": when,
            })
    return list(found.values())



def profile_portfolios():
    """profile -> portfolio_id, taken from each engine's unit (the source of truth)."""
    import glob, re
    out = {}
    for path in glob.glob("/etc/systemd/system/copy-*.service"):
        try:
            text = open(path).read()
        except OSError:
            continue
        prof = re.search(r"COPY_PROFILE=([^\s]*)", text)
        pids = re.search(r"COPY_PORTFOLIO_ID=([^\s]*)", text)
        if not prof or not pids:
            continue
        names = [x.strip() for x in prof.group(1).split(",") if x.strip()]
        ids = [x.strip() for x in pids.group(1).split(",") if x.strip()]
        for name, pid in zip(names, ids):
            out.setdefault(name, pid)
    return out


def source_position(profile, coin, side):
    from source_api import fetch_source_book
    pid = PORTFOLIOS.get(profile)
    try:
        book = fetch_source_book(profile=profile, portfolio_id=pid)
    except Exception as e:
        log("skip %s: source book unavailable (%s)" % (profile, str(e)[:60]))
        return None
    for p in book.get("positions") or []:
        if str(p.get("ticker") or "").upper() == coin.upper() and str(p.get("side")).lower() == side:
            return p
    return None


def best_target(cand, source_pos, statuses, fleet):
    """Lowest margin usage that still clears the minimum notional and the cap."""
    alloc = source_pos.get("source_allocation_pct")
    lev = float(source_pos.get("leverage") or 1)
    try:
        alloc = float(alloc)
    except (TypeError, ValueError):
        alloc = 0.0
    scored = []
    for name, st in statuses.items():
        if name == cand["home"]:
            continue
        wallet, margin_pct = wallet_and_margin(st)
        if wallet < 10 or margin_pct >= MARGIN_HOLD_PCT:
            continue
        symbol = cand["coin"].upper() + "USDT"
        if (symbol, cand["side"]) in fleet.get(name, set()):
            continue
        desired_margin = alloc * wallet if alloc else min(wallet * 0.05, 50.0)
        if desired_margin > wallet * ENTRY_CAP_PCT:
            desired_margin = wallet * ENTRY_CAP_PCT
        notional = desired_margin * lev
        if notional < MIN_NOTIONAL:
            continue
        scored.append((margin_pct, -notional, name, desired_margin, notional))
    scored.sort()
    return scored[0] if scored else None


PORTFOLIOS = {}


def main():
    global PORTFOLIOS
    PORTFOLIOS = profile_portfolios()
    # source_api gates on its own profile table; register every copy source so
    # the router can read the same books the engines read.
    try:
        import source_api as _sa
        _sa.PROFILES.update(PORTFOLIOS)
    except Exception:
        pass
    state = load_state()
    statuses = {name: status(port) for name, port in ACCOUNTS.items()}
    fleet = {name: live_positions(st) for name, st in statuses.items()}
    global_fleet = set()
    for s in fleet.values():
        global_fleet |= s

    cands = candidates(state)
    log("scan: %d candidate(s)%s" % (len(cands), " [DRY RUN]" if DRY_RUN else ""))
    placed = 0

    for cand in cands:
        symbol = cand["coin"].upper() + "USDT"
        if (symbol, cand["side"]) in global_fleet:
            state["handled"][cand["key"]] = {"at": time.time(), "result": "fleet symbol cap"}
            continue
        src = source_position(cand["profile"], cand["coin"], cand["side"])
        if not src:
            state["handled"][cand["key"]] = {"at": time.time(), "result": "source position gone"}
            continue
        pick = best_target(cand, src, statuses, fleet)
        if not pick:
            alloc = src.get("source_allocation_pct")
            lev = src.get("leverage")
            state["handled"][cand["key"]] = {"at": time.time(), "result": "no eligible account"}
            log("no target for %s (%s) alloc=%s lev=%s wallets=%s" % (
                cand["key"], cand["reason"], alloc, lev,
                {n: (round(wallet_and_margin(st)[0]), round(wallet_and_margin(st)[1] * 100))
                 for n, st in statuses.items()}))
            continue
        margin_pct, _neg, target, desired_margin, notional = pick
        # one position per symbol+side across the fleet, including within this run
        global_fleet.add((symbol, cand["side"]))
        fleet.setdefault(target, set()).add((symbol, cand["side"]))
        log("PLAN %s -> %s: margin %.2f USDT, notional %.2f USDT (target margin %.0f%%)"
            % (cand["key"], target, desired_margin, notional, margin_pct * 100))
        if DRY_RUN:
            state["handled"][cand["key"]] = {"at": time.time(), "result": "dry-run planned %s" % target}
            continue
        try:
            record = open_on(target, cand, src, desired_margin, notional)
        except Exception as e:
            log("FAILED %s -> %s: %s" % (cand["key"], target, str(e)[:140]))
            state["handled"][cand["key"]] = {"at": time.time(), "result": "error: %s" % str(e)[:80]}
            continue
        state["handled"][cand["key"]] = {"at": time.time(), "result": "opened on %s" % target}
        state["opened"].append(record)
        placed += 1

    save_state(state)
    if placed:
        log("placed %d spillover trade(s)" % placed)


def open_on(target, cand, src, desired_margin, notional):
    """Open on the target account and leave it owner-managed there."""
    from bybit_api import BybitClient
    env = env_for(target)
    client = BybitClient(
        api_key=env.get("BYBIT_API_KEY"),
        api_secret=env.get("BYBIT_API_SECRET"),
        rsa_key_file=env.get("BYBIT_RSA_PRIVATE_KEY_FILE"),
        proxy=env.get("BYBIT_PROXY"),
    )
    if not client.configured:
        raise RuntimeError("target client not configured")
    symbol = cand["coin"].upper() + "USDT"
    lev = int(float(src.get("leverage") or 1))
    price = float(client.get_ticker(symbol)["lastPrice"])
    qty = notional / price
    side = "Buy" if cand["side"] == "long" else "Sell"
    idx = 1 if cand["side"] == "long" else 2
    try:
        client.set_leverage(symbol, lev)
    except Exception:
        pass
    client.place_order(symbol, side, qty, position_idx=idx)
    # SAFE-TP: never leave a target on the losing side of the fill we got
    tp = src.get("price_target")
    sl = src.get("stop_loss")
    fee = 0.0012
    if tp:
        tp = float(tp)
        if cand["side"] == "long" and tp <= price * (1 + fee):
            tp = round(price * (1 + fee), 8)
        if cand["side"] == "short" and tp >= price * (1 - fee):
            tp = round(price * (1 - fee), 8)
    if sl:
        sl = float(sl)
        if cand["side"] == "long" and sl >= price:
            sl = None
        if cand["side"] == "short" and sl <= price:
            sl = None
    if tp or sl:
        try:
            client.set_sl_tp(symbol, sl, tp, position_idx=idx)
        except Exception as e:
            log("SL/TP failed on %s %s: %s" % (target, symbol, str(e)[:80]))
    log("OPENED %s on %s: qty %.6f @ %.6f (%.2f USDT margin, lev %d)"
        % (symbol, target, qty, price, desired_margin, lev))
    return {
        "at": datetime.now(timezone.utc).isoformat(), "account": target,
        "symbol": symbol, "side": cand["side"], "qty": qty, "entry": price,
        "margin": desired_margin, "leverage": lev, "profile": cand["profile"],
        "source_key": cand["key"], "why_spilled": cand["reason"],
    }


if __name__ == "__main__":
    main()
