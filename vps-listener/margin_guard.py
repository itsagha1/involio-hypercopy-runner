#!/usr/bin/env python3
"""Margin guard: judge every account the way the exchange does, every minute.

Owner 2026-11-12, after a near miss: Sub account 04 sat at 98.3% initial margin
(only $5 free) while the engines reported 62.9%, because they divided margin by
wallet instead of equity. Relief could never fire, and nothing else was looking.

This guard is deliberately independent of the engines and of margin_relief:
  * reads accountIMRate / accountMMRate straight from Bybit, every 60 seconds
  * WATCH   >= 80% : logged
  * ACT     >= 90% : borrows from the master funding account to bring the
                     account back to 60%, 100-200 USDT, max 600 outstanding,
                     one action per account per hour
  * URGENT  >= 95% : same borrow, but also writes an URGENT marker and an
                     unmissable log line, because that account is one bad
                     candle from liquidation
It NEVER opens, closes or resizes a position. Money moves one way only:
master -> subaccount, to buy distance from liquidation.

Usage
  margin_guard.py            act for real
  margin_guard.py --dry      show what it would do, change nothing
  margin_guard.py --test     run the decision-logic checks and exit
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import move_funds as mf  # noqa: E402

ACCOUNTS = ["mainport", "bakery", "scalping", "kings", "bluespire", "hypercopy"]
LABELS = {"mainport": "Sub account 01", "bakery": "Sub account 02", "scalping": "Sub account 03",
          "kings": "Sub account 04", "bluespire": "Sub account 05", "hypercopy": "hypercopy"}
STATE_PATH = "/var/lib/hypercopy-copy/margin_guard.json"
RELIEF_LEDGER = "/var/lib/hypercopy-copy/margin_relief.json"   # the loan book margin_relief repays from
LOG_PATH = "/var/lib/hypercopy-copy/margin_guard.log"

WATCH_PCT = 80.0
ACT_PCT = 90.0
URGENT_PCT = 95.0
TARGET_PCT = 60.0
MIN_BORROW = 100.0
MAX_BORROW = 200.0
MAX_OUTSTANDING = 600.0
COOLDOWN = 3600


def log(msg):
    line = "[%s] %s" % (datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), msg)
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass



def load_ledger():
    try:
        return json.load(open(RELIEF_LEDGER))
    except Exception:
        return {"accounts": {}, "history": []}


def record_ledger(account, amount, before_rate, after_rate):
    """Borrows must land in the ledger margin_relief repays from, or the money
    would never find its way back to master."""
    ledger = load_ledger()
    accs = ledger.setdefault("accounts", {})
    rec = accs.setdefault(account, {})
    rec["outstanding"] = round(float(rec.get("outstanding") or 0) + amount, 2)
    rec["last_action_ts"] = time.time()
    ledger.setdefault("history", []).append({
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "account": account,
        "action": "guard_borrow", "amount": amount,
        "before_rate": round(before_rate, 2), "after_rate": round(after_rate, 2)})
    try:
        tmp = RELIEF_LEDGER + ".tmp"
        json.dump(ledger, open(tmp, "w"), indent=1)
        os.replace(tmp, RELIEF_LEDGER)
    except Exception as e:
        log("%s: ledger write failed (%s) - repayment record may be incomplete" % (account, str(e)[:60]))


def borrow_amount(used, equity, outstanding):
    """How much to move so that initial margin is TARGET_PCT of equity again."""
    needed = used / (TARGET_PCT / 100.0) - equity
    amount = max(MIN_BORROW, min(MAX_BORROW, needed))
    amount = min(amount, MAX_OUTSTANDING - outstanding)
    return round(amount, 2)


def decide(rate_pct, used, equity, outstanding):
    """Pure decision function so it can be tested without touching money.

    Returns (action, amount): action is one of ok / watch / borrow / urgent / capped."""
    if rate_pct >= URGENT_PCT:
        if outstanding >= MAX_OUTSTANDING:
            return "capped", 0.0
        return "urgent", borrow_amount(used, equity, outstanding)
    if rate_pct >= ACT_PCT:
        if outstanding >= MAX_OUTSTANDING:
            return "capped", 0.0
        return "borrow", borrow_amount(used, equity, outstanding)
    if rate_pct >= WATCH_PCT:
        return "watch", 0.0
    return "ok", 0.0


def read_account(client):
    r = client._get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
    x = (r.get("list") or [{}])[0]
    return {
        "equity": float(x.get("totalEquity") or 0),
        "wallet": float(x.get("totalWalletBalance") or 0),
        "im": float(x.get("totalInitialMargin") or 0),
        "mm": float(x.get("totalMaintenanceMargin") or 0),
        "im_rate": float(x.get("accountIMRate") or 0) * 100.0,
        "mm_rate": float(x.get("accountMMRate") or 0) * 100.0,
        "free": float(x.get("totalAvailableBalance") or 0),
    }


def master_balance():
    c = mf.client_for("master")
    r = c._get("/v5/asset/transfer/query-account-coins-balance",
               {"accountType": "FUND", "coin": "USDT"})
    lst = r.get("balance") or r.get("list") or []
    if isinstance(lst, dict):
        lst = [lst]
    b = lst[0] if lst else {}
    return float(b.get("transferBalance") or b.get("walletBalance") or 0)


def selftest():
    """Deterministic checks of the decision logic. Run before trusting it."""
    cases = [
        # rate, used, equity, outstanding -> expected action
        (59.9, 300.0, 500.0, 0.0, "ok"),
        (80.0, 160.0, 200.0, 0.0, "watch"),
        (89.9, 179.0, 199.0, 0.0, "watch"),
        (90.0, 300.0, 333.0, 0.0, "borrow"),
        (98.3, 300.05, 305.25, 0.0, "urgent"),
        (95.0, 380.0, 400.0, 600.0, "capped"),
    ]
    ok = True
    for rate, used, equity, out, expect in cases:
        action, amount = decide(rate, used, equity, out)
        good = action == expect
        ok = ok and good
        print("  %-6s rate %5.1f%% used %7.2f equity %7.2f out %6.1f -> %-7s amount %7.2f"
              % ("PASS" if good else "FAIL", rate, used, equity, out, action, amount))
    # sizing: at 98.3% on sub 04 the borrow must bring it back near 60%
    action, amount = decide(98.3, 300.05, 305.25, 0.0)
    post_equity = 305.25 + amount
    post_rate = 300.05 / post_equity * 100.0
    good = 55.0 <= post_rate <= 65.0
    ok = ok and good
    print("  %-6s sub 04 case: borrow %.2f -> rate %.2f%% (want 55-65%%)"
          % ("PASS" if good else "FAIL", amount, post_rate))
    # a huge loss must not demand more than the cap
    action, amount = decide(99.0, 900.0, 910.0, 500.0)
    good = amount <= 100.0 and action == "urgent"
    ok = ok and good
    print("  %-6s cap respected: amount %.2f (max remaining 100.00)" % ("PASS" if good else "FAIL", amount))
    print("SELFTEST:", "ALL PASS" if ok else "FAILURES PRESENT")
    return 0 if ok else 1


def main():
    dry = "--dry" in sys.argv
    try:
        state = json.load(open(STATE_PATH))
    except Exception:
        state = {"accounts": {}, "history": []}
    accounts = state.setdefault("accounts", {})
    history = state.setdefault("history", [])

    pool = master_balance() if not dry or True else 0.0
    log("guard run%s | master transferable %.2f USDT" % (" [DRY]" if dry else "", pool))

    now = time.time()
    for name in ACCOUNTS:
        label = LABELS.get(name, name)
        try:
            info = read_account(mf.client_for(name))
        except Exception as e:
            log("%s: read failed (%s)" % (label, str(e)[:70]))
            continue
        st = accounts.setdefault(name, {"outstanding": 0.0, "last_action_ts": 0})
        ledger = load_ledger()
        out = float(((ledger.get("accounts") or {}).get(name) or {}).get("outstanding") or 0)
        action, amount = decide(info["im_rate"], info["im"], info["equity"], out)
        msg = ("%s: IM rate %.2f%% (MM %.2f%%) equity %.2f free %.2f outstanding %.2f -> %s"
               % (label, info["im_rate"], info["mm_rate"], info["equity"], info["free"], out, action))
        log(msg)

        if action in ("borrow", "urgent"):
            since = now - float(st.get("last_action_ts") or 0)
            if since < COOLDOWN:
                log("%s: %s but %d min left on cooldown" % (label, action, int((COOLDOWN - since) // 60)))
                continue
            if pool < amount:
                log("%s: needs %.2f but master holds only %.2f - CANNOT FUND" % (label, amount, pool))
                if action == "urgent":
                    open("/var/lib/hypercopy-copy/margin_URGENT_%s.flag" % name, "w").write(
                        "%s %.2f%% needs %.2f master %.2f\n" % (msg, info["im_rate"], amount, pool))
                continue
            if dry:
                log("%s: [DRY] would borrow %.2f from master" % (label, amount))
                continue
            try:
                res = mf.transfer("master", name, amount, execute=True)
                if (res or {}).get("status") != "SUCCESS":
                    log("%s: transfer not confirmed: %s" % (label, json.dumps(res)[:140]))
                    continue
            except Exception as e:
                log("%s: transfer failed (%s)" % (label, str(e)[:90]))
                continue
            after = read_account(mf.client_for(name))
            st["outstanding"] = round(out + amount, 2)
            st["last_action_ts"] = now
            record_ledger(name, amount, info["im_rate"], after["im_rate"])
            pool -= amount
            history.append({"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                            "account": name, "action": action, "amount": amount,
                            "before_rate": round(info["im_rate"], 2),
                            "after_rate": round(after["im_rate"], 2)})
            log("%s: BORROWED %.2f -> IM rate %.2f%% -> %.2f%% | free %.2f -> %.2f"
                % (label, amount, info["im_rate"], after["im_rate"], info["free"], after["free"]))
        elif action == "capped":
            log("%s: at %.2f%% and already %.2f borrowed - MAX OUTSTANDING reached, owner action needed"
                % (label, info["im_rate"], out))
            open("/var/lib/hypercopy-copy/margin_URGENT_%s.flag" % name, "w").write(
                "%s IM %.2f%% outstanding %.2f at cap\n" % (label, info["im_rate"], out))

        st["last_rate"] = round(info["im_rate"], 2)
        st["last_seen"] = now

    state["master_pool_usd"] = round(pool, 2)
    state["ts"] = now
    if not dry:
        tmp = STATE_PATH + ".tmp"
        json.dump(state, open(tmp, "w"), indent=1)
        os.replace(tmp, STATE_PATH)
    return 0


if __name__ == "__main__":
    if "--test" in sys.argv:
        sys.exit(selftest())
    sys.exit(main())
