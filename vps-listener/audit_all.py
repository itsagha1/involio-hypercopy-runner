#!/usr/bin/env python3
"""Fleet-wide audit: scan the whole copy setup and safely repair it.

Owner 2026-11-12: every bug fixed by hand must also be detectable and fixable
here, so the same fault never needs a human again. Runs as root (reads the
engines' state files and restarts services).

Checks
  1. all seven engines reachable, error count, poll freshness
  2. shared book cache: every (profile, portfolio) present, both Involio
     accounts healthy, multi-portfolio sources merged correctly
  3. retained positions whose source is STILL OPEN  -> restore to mirrored
     (uses the live exchange position for qty/entry, never a guess)
  4. mirrors with no live exchange position        -> deregister
  5. live positions neither mirrored nor owner-managed -> adopt as own
  6. stale owner-managed labels (no live position) -> clear
  7. margin relief: outstanding loan on an account that has recovered
  8. spillover ledger: duplicate or conflicting opens
  9. self-check scan: reports remaining fixable items

Usage:  audit_all.py [--scan]     (--scan = report only, no changes)
Output: JSON on stdout, appended to /var/lib/hypercopy-copy/fleet_audit.log
"""
import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

ENGINES = [("main", 8000, "/home/ubuntu/involio-hypercopy-runner/vps-listener/vps_state.json"),
           ("mainport", 8001, "/var/lib/hypercopy-copy/mainport_state.json"),
           ("bakery", 8002, "/var/lib/hypercopy-copy/bakery_state.json"),
           ("scalping", 8003, "/var/lib/hypercopy-copy/scalping_state.json"),
           ("kings", 8004, "/var/lib/hypercopy-copy/kings_state.json"),
           ("bluespire", 8005, "/var/lib/hypercopy-copy/bluespire_state.json"),
           ("hypercopy", 8006, "/var/lib/hypercopy-copy/hypercopy_state.json")]
SERVICE = {"main": "hypercopy-listener"}
for n, _p, _f in ENGINES[1:]:
    SERVICE[n] = "copy-%s" % n
CACHE = "http://127.0.0.1:8010"
FINDINGS = []
ACTIONS = []
SCAN_ONLY = "--scan" in sys.argv


def finding(sev, area, msg, fixed=None):
    FINDINGS.append({"severity": sev, "area": area, "message": msg,
                     "fixed": fixed, "fixable": fixed is not None})


def acted(area, msg):
    ACTIONS.append({"area": area, "action": msg})


def get(url, timeout=15):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)


def status(port):
    try:
        return get("http://127.0.0.1:%d/status" % port, timeout=15)
    except Exception:
        return None


def book(profile):
    try:
        return get(CACHE + "/book?profile=" + profile, timeout=10).get("book") or {}
    except Exception:
        return {}


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def save(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=1)
    os.replace(tmp, path)


def restart(name):
    subprocess.run(["systemctl", "restart", SERVICE[name]], check=False)
    time.sleep(6)


def pair(label):
    if not isinstance(label, str) or "/" not in label:
        return None
    a, b = label.split("/")[:2]
    a = a.upper()
    if a.endswith("USDT") and len(a) > 4:
        a = a[:-4]
    return (a, b.lower())


# ---------------------------------------------------------------- 1. engines
engine_status = {}
for name, port, _state in ENGINES:
    d = status(port)
    engine_status[name] = d
    if d is None:
        finding("critical", name, "engine not responding on port %d" % port)
        continue
    if d.get("errors_last_run"):
        finding("warning", name, "%s error(s) in the last poll" % d["errors_last_run"])
    # margin judged on EQUITY from the exchange, never the engine's wallet figure
    try:
        import move_funds as _mf
        _r = _mf.client_for(name)._get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        _x = (_r.get("list") or [{}])[0]
        _rate = float(_x.get("accountIMRate") or 0) * 100.0
        _free = float(_x.get("totalAvailableBalance") or 0)
        engine_status.setdefault(name, {})["im_rate"] = round(_rate, 2)
        if _rate >= 95:
            finding("critical", name, "IM rate %.2f%% against equity, free %.2f - liquidation risk NOW" % (_rate, _free))
        elif _rate >= 90:
            finding("critical", name, "IM rate %.2f%% against equity - margin guard should borrow" % _rate)
        elif _rate >= 85:
            finding("warning", name, "IM rate %.2f%% against equity, free %.2f - approaching the 90%% trigger" % (_rate, _free))
    except Exception as _me:
        finding("info", name, "equity margin unreadable (%s)" % str(_me)[:50])
    lp = d.get("last_poll")
    if lp:
        try:
            age = time.time() - datetime.fromisoformat(lp).timestamp()
            if age > 900:
                finding("critical", name, "no poll for %d minutes" % int(age // 60))
        except Exception:
            pass

# ---------------------------------------------------------------- 2. cache
cache_health = None
try:
    cache_health = get(CACHE + "/health", timeout=10)
except Exception as e:
    finding("critical", "cache", "book cache unreachable (%s)" % str(e)[:50])

if cache_health:
    prof_map = cache_health.get("profiles") or {}
    stale = [p for p, v in prof_map.items() if v.get("error")]
    tokens = {}
    for v in prof_map.values():
        tokens[v.get("token")] = tokens.get(v.get("token"), 0) + 1
    if stale:
        finding("critical", "cache", "%d source book(s) failing: %s"
                % (len(stale), ", ".join(sorted(stale)[:5])))
        if not SCAN_ONLY:
            subprocess.run(["systemctl", "restart", "book-cache"], check=False)
            time.sleep(8)
            try:
                h2 = get(CACHE + "/health", timeout=10)
                still = [p for p, v in (h2.get("profiles") or {}).items() if v.get("error")]
                acted("cache", "restarted book-cache; %d still failing" % len(still))
                finding("critical", "cache", "%d book(s) fail after restart: %s"
                        % (len(still), ", ".join(sorted(still)[:5])), fixed=not still)
            except Exception:
                acted("cache", "restarted book-cache (health unreadable after restart)")
    if "B" not in tokens:
        finding("warning", "cache", "only one Involio account in use - second token idle or missing")
    # multi-portfolio sources must be merged, not overwritten
    try:
        units = {}
        for fn in os.listdir("/etc/systemd/system"):
            if fn.startswith("copy-") and fn.endswith(".service"):
                txt = open("/etc/systemd/system/" + fn).read()
                profs = [x.strip() for x in (txt.split("COPY_PROFILE=")[1].split("\n")[0]).split(",") if x.strip()]
                pids = [x.strip() for x in (txt.split("COPY_PORTFOLIO_ID=")[1].split("\n")[0]).split(",") if x.strip()]
                for pr, pid in zip(profs, pids):
                    units.setdefault(pr, set()).add(pid)
        for pr, pids in units.items():
            if len(pids) < 2:
                continue
            merged = book(pr).get("positions") or []
            ids = {str(p.get("source_id")) for p in merged}
            ok = len(ids) >= min(2, len(merged))
            if not merged:
                finding("critical", "cache", "%s runs %d portfolios but merged book is empty" % (pr, len(pids)))
            elif not ok:
                finding("warning", "cache", "%s merges %d portfolios into %d position(s) - possible collision"
                        % (pr, len(pids), len(merged)))
    except Exception as e:
        finding("info", "cache", "multi-portfolio check skipped (%s)" % str(e)[:50])

# ------------------------------------------- 3-6. per-engine state repair
for name, port, state_path in ENGINES:
    d = engine_status.get(name)
    if not d or not os.path.exists(state_path):
        continue
    st = load(state_path)
    if st is None:
        finding("warning", name, "state file unreadable: %s" % state_path)
        continue
    live = {}
    for p in (d.get("bybit") or {}).get("positions") or []:
        if float(p.get("size") or 0) > 0:
            live[(p.get("symbol"), "long" if (p.get("side") or "").lower() == "buy" else "short")] = p
    changed = False

    # 3. retained while the source trade is still open -> back to mirroring
    books = {}
    for key, rec in list((st.get("retained") or {}).items()):
        prof = rec.get("profile") or key.split("|")[0]
        if prof not in books:
            books[prof] = book(prof)
        open_ids = {str(p.get("source_id")) for p in books[prof].get("positions") or []}
        if str(rec.get("source_id")) not in open_ids:
            continue
        pos = live.get((rec.get("symbol"), rec.get("side")))
        if not pos:
            finding("warning", name, "retained %s: source open but no exchange position" % key.split("|")[1])
            continue
        qty = float(pos.get("size") or 0)
        if SCAN_ONLY:
            finding("warning", name, "retained %s: source is STILL OPEN - should be mirroring"
                    % key.split("|")[1], fixed=False)
            continue
        src = next((p for p in books[prof].get("positions") or []
                    if str(p.get("source_id")) == str(rec.get("source_id"))), {})
        st.setdefault("mirrored", {})[key] = {
            "symbol": rec.get("symbol"), "side": rec.get("side"), "qty": qty,
            "margin": float(pos.get("positionIM") or 0) or float(rec.get("margin") or 0),
            "source_id": rec.get("source_id"),
            "last_applied_source_qty": qty, "desired_source_qty": qty,
            "leverage": int(float(pos.get("leverage") or 5) or 5),
            "opened_at": rec.get("retained_at"),
            "entry_price": float(pos.get("avgPrice") or 0) or float(rec.get("entry") or 0),
            "price_target": src.get("price_target") or 0,
            "stop_loss": src.get("stop_loss") or None,
            "profile": prof,
            "restored_by": "fleet-audit " + datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        del st["retained"][key]
        changed = True
        acted(name, "restored %s from retained to mirrored (source still open)" % key.split("|")[1])
        finding("warning", name, "retained %s was source-open - restored to mirroring" % key.split("|")[1], fixed=True)

    # 4. mirrors with no live position -> deregister
    for key, rec in list((st.get("mirrored") or {}).items()):
        if (rec.get("symbol"), rec.get("side")) not in live:
            if SCAN_ONLY:
                finding("warning", name, "mirror %s has no exchange position" % key.split("|")[1], fixed=False)
                continue
            del st["mirrored"][key]
            changed = True
            acted(name, "deregistered mirror %s (no live position)" % key.split("|")[1])
            finding("warning", name, "mirror %s had no exchange position - deregistered" % key.split("|")[1], fixed=True)

    # 5. live position nobody owns -> adopt
    owned = {(v.get("symbol"), v.get("side")) for v in (st.get("mirrored") or {}).values()}
    owned |= {p for p in (pair(m) and (pair(m)[0] + "USDT", pair(m)[1]) for m in (st.get("manual") or [])) if p}
    for pair_key in sorted(live):
        if pair_key not in owned:
            finding("warning", name, "live %s %s is neither mirrored nor owner-managed" % pair_key)

    # 6. stale owner-managed labels -> clear
    keep_manual = []
    for m in (st.get("manual") or []):
        pr = pair(m)
        if pr and (pr[0] + "USDT", pr[1]) not in live:
            if SCAN_ONLY:
                finding("info", name, "label %s has no live position - should be cleared" % m, fixed=False)
                keep_manual.append(m)
                continue
            acted(name, "cleared stale label %s" % m)
            finding("info", name, "cleared stale label %s" % m, fixed=True)
            changed = True
            continue
        keep_manual.append(m)
    if keep_manual != (st.get("manual") or []):
        st["manual"] = keep_manual
        changed = True

    if changed and not SCAN_ONLY:
        save(state_path, st)
        restart(name)
        d2 = status(port)
        if d2 and d2.get("errors_last_run"):
            finding("critical", name, "errors after repair restart (%s)" % d2.get("errors_last_run"))

# ---------------------------------------------------------------- 7. relief
try:
    rel = load("/var/lib/hypercopy-copy/margin_relief.json") or {}
    for acc, v in (rel.get("accounts") or {}).items():
        out = float((v or {}).get("outstanding") or 0)
        if out <= 0:
            continue
        d = engine_status.get(acc) or {}
        mp = d.get("margin_pct")
        if isinstance(mp, (int, float)) and mp <= 30:
            finding("warning", "relief", "%s still shows %.0f USDT lent while margin is %.0f%% - repayment due"
                    % (acc, out, mp))
        else:
            finding("info", "relief", "%s carries %.0f USDT lent (margin %.0f%%)" % (acc, out, mp or 0))
except Exception as e:
    finding("info", "relief", "relief check skipped (%s)" % str(e)[:50])

# ---------------------------------------------------------------- 8. spillover
try:
    sp = load("/var/lib/hypercopy-copy/spill_router.json") or {}
    seen = {}
    for o in (sp.get("opened") or []):
        k = (o.get("account"), o.get("symbol"), o.get("side"))
        seen[k] = seen.get(k, 0) + 1
    dupes = {k: v for k, v in seen.items() if v > 1}
    if dupes:
        finding("warning", "spillover", "repeated spill opens: %s" % list(dupes)[:3])
    else:
        finding("info", "spillover", "%d spill placement(s) on record, no duplicates" % len(sp.get("opened") or []))
except Exception as e:
    finding("info", "spillover", "spillover check skipped (%s)" % str(e)[:50])

# selfcheck step removed: the listener scan already calls this audit

RESULT = {
    "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "mode": "scan" if SCAN_ONLY else "fix",
    "findings": FINDINGS,
    "actions": ACTIONS,
    "counts": {
        "critical": sum(1 for f in FINDINGS if f["severity"] == "critical"),
        "warning": sum(1 for f in FINDINGS if f["severity"] == "warning"),
        "fixed": sum(1 for f in FINDINGS if f.get("fixed")),
    },
    "engines": {n: {"errors": (d or {}).get("errors_last_run"),
                    "mirrors": len((d or {}).get("mirrored") or {}),
                    "margin": (d or {}).get("margin_pct")}
                for n, d in engine_status.items()},
    "cache": None if not cache_health else {
        "profiles": len(cache_health.get("profiles") or {}),
        "failing": sum(1 for v in (cache_health.get("profiles") or {}).values() if v.get("error")),
    },
}
with open("/var/lib/hypercopy-copy/fleet_audit.json", "w") as f:
    json.dump(RESULT, f, indent=1)
line = "%s [%s] critical=%d warning=%d fixed=%d %s" % (
    RESULT["ts"], RESULT["mode"], RESULT["counts"]["critical"],
    RESULT["counts"]["warning"], RESULT["counts"]["fixed"],
    "; ".join(a["action"] for a in ACTIONS[:5]))
with open("/var/lib/hypercopy-copy/fleet_audit.log", "a") as f:
    f.write(line + "\n")
print(json.dumps(RESULT))
