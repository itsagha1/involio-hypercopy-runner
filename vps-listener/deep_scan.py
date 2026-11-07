#!/usr/bin/env python3
"""Daily deep scan for the HyperCopy fleet (owner 2026-11-08).

Runs entirely on the VPS: no Base44 credits, no LLM. It checks the things that
have actually broken before (dead engines, state drift, order-link collisions,
manual-entry corruption, credential/clock problems) plus code integrity, git
drift, mirror consistency against the live exchange, log error mining, ledger
freshness and disk hygiene.

Safe, idempotent auto-fixes are applied. Anything needing judgement is reported
with severity so the agent step can look only when there is something to see.

Output: /var/lib/hypercopy-copy/deep_scan.json  (+ appended deep_scan.log)
Exit code: 0 healthy, 1 warnings, 2 critical.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

BASE = "/home/ubuntu/involio-hypercopy-runner"
LISTENER = BASE + "/vps-listener"
STATE_DIR = "/var/lib/hypercopy-copy"
OUT_JSON = STATE_DIR + "/deep_scan.json"
OUT_LOG = STATE_DIR + "/deep_scan.log"
REPORT_DAYS = 1.0

ENGINES = [
    ("main", 8000, "hypercopy-listener"),
    ("mainport", 8001, "copy-mainport"),
    ("bakery", 8002, "copy-bakery"),
    ("scalping", 8003, "copy-scalping"),
    ("kings", 8004, "copy-kings"),
    ("bluespire", 8005, "copy-bluespire"),
    ("hypercopy", 8006, "copy-hypercopy"),
]
TIMERS = ["hypercopy-audit.timer", "hypercopy-autoupdate.timer", "margin-relief.timer"]
LOGS = [
    (name, LISTENER + "/actions.log" if name == "main" else f"{STATE_DIR}/{name}_actions.log")
    for name, _p, _u in ENGINES
]

findings: List[Dict[str, Any]] = []
fixes: List[str] = []


def add(sev: str, area: str, msg: str, data: Any = None) -> None:
    findings.append({"sev": sev, "area": area, "msg": msg, "data": data})


def sh(cmd: str, timeout: int = 30) -> str:
    try:
        return subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        ).stdout.strip()
    except Exception as e:  # noqa: BLE001
        return f"__error__{e}"


def status(port: int) -> Dict[str, Any] | None:
    out = sh(f"curl -s -m 8 http://127.0.0.1:{port}/status", timeout=15)
    try:
        return json.loads(out)
    except Exception:  # noqa: BLE001
        return None


# --------------------------------------------------------------------------- #
# 1. code integrity
# --------------------------------------------------------------------------- #
def check_code() -> None:
    bad = []
    for f in sorted(os.listdir(LISTENER)):
        if not f.endswith(".py"):
            continue
        r = subprocess.run(
            [sys.executable, "-m", "py_compile", os.path.join(LISTENER, f)],
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            bad.append({"file": f, "err": r.stderr.strip().splitlines()[-1:]})
    if bad:
        add("critical", "code", f"{len(bad)} file(s) fail to compile", bad)
    else:
        add("ok", "code", "all listener modules compile")


# --------------------------------------------------------------------------- #
# 2. git drift
# --------------------------------------------------------------------------- #
def check_git() -> None:
    head = sh(f"cd {BASE} && git rev-parse HEAD")
    dirty = sh(f"cd {BASE} && git diff --name-only HEAD -- vps-listener | head -5")
    fetch = sh(f"cd {BASE} && timeout 25 git fetch -q origin main && git rev-parse origin/main")
    if dirty:
        add("warning", "git", "working tree has uncommitted code changes", dirty.splitlines())
    if head and fetch and not fetch.startswith("__error__") and head != fetch:
        add("warning", "git", "VPS code differs from GitHub main (autoupdate blocked)",
            {"local": head[:10], "remote": fetch[:10]})
    else:
        add("ok", "git", "code matches origin/main")


# --------------------------------------------------------------------------- #
# 3. services + timers
# --------------------------------------------------------------------------- #
def check_units() -> None:
    for name, port, unit in ENGINES:
        st = sh(f"systemctl is-active {unit}")
        if st != "active":
            add("critical", "service", f"{unit} is {st or 'unknown'}", {"port": port})
            if sh(f"sudo -n true && echo ok") == "ok":
                sh(f"sudo -n systemctl restart {unit}")
                time.sleep(3)
                if sh(f"systemctl is-active {unit}") == "active":
                    fixes.append(f"restarted {unit}")
    for t in TIMERS:
        st = sh(f"systemctl is-active {t}")
        if st != "active":
            add("warning", "timer", f"{t} is {st or 'unknown'}")


# --------------------------------------------------------------------------- #
# 4. engine health from /status
# --------------------------------------------------------------------------- #
def check_engines() -> Dict[str, Dict[str, Any]]:
    snaps: Dict[str, Dict[str, Any]] = {}
    now = time.time()
    for name, port, unit in ENGINES:
        d = status(port)
        if not d:
            add("critical", "engine", f"{name}: status endpoint not responding", {"port": port})
            continue
        snaps[name] = d
        if d.get("errors_last_run"):
            add("warning", "engine", f"{name}: {d['errors_last_run']} error(s) last poll",
                {"errors": d["errors_last_run"]})
        if d.get("dry_run"):
            add("warning", "engine", f"{name}: still in DRY RUN (not trading)")
        lp = d.get("last_poll")
        if lp:
            try:
                age = now - datetime.fromisoformat(lp).timestamp()
                limit = 600 if name == "main" else 900
                if age > limit:
                    add("critical", "engine", f"{name}: last poll {int(age)}s ago (stalled)")
            except Exception:  # noqa: BLE001
                pass
        mp = d.get("margin_pct")
        if isinstance(mp, (int, float)) and mp >= 70:
            add("warning", "risk", f"{name}: margin {mp}% (entry hold active)")
        for k in ("orphans", "mismatches"):
            v = d.get(k)
            if isinstance(v, list) and v:
                add("warning", "engine", f"{name}: {len(v)} {k}", v[:5])
    return snaps


# --------------------------------------------------------------------------- #
# 5. state hygiene + mirror consistency vs the live exchange
# --------------------------------------------------------------------------- #
def state_path(name: str) -> str:
    return LISTENER + "/vps_state.json" if name == "main" else f"{STATE_DIR}/{name}_state.json"


def check_state(snaps: Dict[str, Dict[str, Any]]) -> None:
    for name, _port, _unit in ENGINES:
        p = state_path(name)
        if not os.path.exists(p):
            add("warning", "state", f"{name}: state file missing", {"path": p})
            continue
        try:
            with open(p) as f:
                st = json.load(f)
        except Exception as e:  # noqa: BLE001
            add("critical", "state", f"{name}: state unreadable ({e})", {"path": p})
            continue

        man = st.get("manual") or []
        bad = [m for m in man if isinstance(m, dict)]
        if bad:
            st["manual"] = [
                f"{str(m.get('symbol') or m.get('ticker') or '').upper()}/"
                f"{str(m.get('side') or '').lower()}" for m in man if isinstance(m, dict)
            ] + [m for m in man if not isinstance(m, dict)]
            try:
                with open(p, "w") as f:
                    json.dump(st, f)
                fixes.append(f"{name}: normalised {len(bad)} dict manual entr(ies)")
            except Exception as e:  # noqa: BLE001
                add("critical", "state", f"{name}: manual entries are dicts and not writable ({e})")

        blocks = st.get("entry_blocks") or {}
        if len(blocks) > 250:
            add("warning", "state", f"{name}: {len(blocks)} entry_blocks accumulating")

        size = os.path.getsize(p)
        if size > 3_000_000:
            add("warning", "state", f"{name}: state file {size // 1000}kB (growth)")

        # mirror consistency vs live exchange (only for copy engines, they hold keys)
        if name == "main":
            continue
        d = snaps.get(name) or {}
        live = None
        try:
            live = (d.get("bybit") or {}).get("positions")
        except Exception:  # noqa: BLE001
            live = None
        if live is None:
            continue
        mirrored = st.get("mirrored") or {}
        mir_pairs = {(v.get("symbol"), v.get("side")) for v in mirrored.values()}
        for lp in live or []:
            if float(lp.get("size") or 0) <= 0:
                continue
            side = "long" if lp.get("side") == "Buy" else "short"
            pair = (lp.get("symbol"), side)
            if pair not in mir_pairs:
                man_pairs = {(m.split("/")[0] + "USDT", m.split("/")[1])
                             for m in (st.get("manual") or []) if isinstance(m, str) and "/" in m}
                if pair not in man_pairs:
                    add("warning", "mirror",
                        f"{name}: live {pair[0]} {pair[1]} held but neither mirrored nor manual",
                        {"size": lp.get("size")})


# --------------------------------------------------------------------------- #
# 6. log error mining
# --------------------------------------------------------------------------- #
SIG = re.compile(r"(ERROR|CRITICAL|Traceback|retCode|10006|10001|10002|collision)", re.I)


def check_logs() -> None:
    cutoff = time.time() - REPORT_DAYS * 86400
    for name, path in LOGS:
        if not os.path.exists(path):
            continue
        sigs: Dict[str, int] = {}
        last_ts = 0.0
        try:
            with open(path, errors="replace") as f:
                for line in f:
                    if not SIG.search(line):
                        continue
                    m = re.match(r"\[(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})", line)
                    if not m:
                        continue
                    ts = datetime.strptime(m.group(1) + " " + m.group(2), "%Y-%m-%d %H:%M:%S")
                    if ts.replace(tzinfo=timezone.utc).timestamp() < cutoff:
                        continue
                    key = re.sub(r"[0-9a-f]{8,}|\d+\.\d+|\d+", "N", line[30:])[:110]
                    sigs[key] = sigs.get(key, 0) + 1
                    last_ts = max(last_ts, ts.replace(tzinfo=timezone.utc).timestamp())
        except Exception:  # noqa: BLE001
            continue
        top = sorted(sigs.items(), key=lambda kv: -kv[1])[:3]
        if top:
            fresh = (time.time() - last_ts) < 3 * 3600
            add("warning" if (sum(sigs.values()) > 5 and fresh) else "ok", "logs",
                f"{name}: {sum(sigs.values())} error-ish lines in 24h, last {int((time.time()-last_ts)/60)}min ago"
                + ("" if fresh else " (resolved)"), top)


# --------------------------------------------------------------------------- #
# 7. credentials, clock, ledger, disk
# --------------------------------------------------------------------------- #
def check_platform() -> None:
    skew = sh("sudo -n chronyc tracking 2>/dev/null | awk '/System time/{print $4}'")
    try:
        if skew and abs(float(skew)) > 1.0:
            add("warning", "clock", f"system clock skew {skew}s (Bybit 10002 risk)")
    except Exception:  # noqa: BLE001
        pass

    led = STATE_DIR + "/transfer_ledger.jsonl"
    inv = STATE_DIR + "/invest_exact.json"
    if os.path.exists(led):
        led_age = (time.time() - os.path.getmtime(led)) / 86400
        if os.path.exists(inv):
            inv_age = (time.time() - os.path.getmtime(inv)) / 86400
            if inv_age > 2:
                sh(f"cd {LISTENER} && venv/bin/python invest_sync.py >/dev/null 2>&1")
                fixes.append("re-ran invest_sync (ledger figures were stale)")
            try:
                with open(inv) as f:
                    tot = sum(float(v.get("invested") or 0) for v in (json.load(f).get("accounts") or {}).values())
                add("ok", "ledger", f"invested total {tot:.2f} USDT", {"ledger_age_days": round(led_age, 2)})
            except Exception:  # noqa: BLE001
                add("warning", "ledger", "invest_exact.json unreadable")

    du = sh(f"du -sm {STATE_DIR} /var/lib/hypercopy-backups 2>/dev/null | awk '{{print $1\" \"$2}}'")
    for line in du.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0].isdigit() and int(parts[0]) > 500:
            add("warning", "disk", f"{parts[1]} uses {parts[0]}MB")
    old = sh("find /var/lib/hypercopy-backups -name '*.tgz' -mtime +14 2>/dev/null | wc -l")
    if old.isdigit() and int(old) > 0:
        sh("find /var/lib/hypercopy-backups -name '*.tgz' -mtime +14 -delete")
        fixes.append(f"pruned {old} backup archive(s) older than 14 days")


# --------------------------------------------------------------------------- #
def main() -> int:
    t0 = time.time()
    check_code()
    check_git()
    check_units()
    snaps = check_engines()
    check_state(snaps)
    check_logs()
    check_platform()

    sev_rank = {"ok": 0, "warning": 1, "critical": 2}
    worst = max((sev_rank.get(f["sev"], 0) for f in findings), default=0)
    summary = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "status": ["ok", "warning", "critical"][worst],
        "counts": {
            "critical": sum(1 for f in findings if f["sev"] == "critical"),
            "warning": sum(1 for f in findings if f["sev"] == "warning"),
            "ok": sum(1 for f in findings if f["sev"] == "ok"),
        },
        "fixes_applied": fixes,
        "findings": findings,
        "duration_s": round(time.time() - t0, 1),
        "engines": {
            n: {
                "version": d.get("code_version") or d.get("version"),
                "mirrored": len(d.get("mirrored") or {}),
                "errors": d.get("errors_last_run"),
                "last_poll": d.get("last_poll"),
                "margin_pct": d.get("margin_pct"),
            }
            for n, d in snaps.items()
        },
    }
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        with open(OUT_JSON, "w") as f:
            json.dump(summary, f, indent=1)
        with open(OUT_LOG, "a") as f:
            f.write(json.dumps({k: summary[k] for k in ("ts", "status", "counts", "fixes_applied")}) + "\n")
    except Exception as e:  # noqa: BLE001
        print(f"write failed: {e}", file=sys.stderr)

    print(json.dumps(summary, indent=1)[:4000])
    return 0 if worst == 0 else (1 if worst == 1 else 2)


if __name__ == "__main__":
    sys.exit(main())
