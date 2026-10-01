#!/bin/bash
# State-preserving updater. Exchange credentials remain untouched on the VPS.
set -euo pipefail
REPO=${HYPERCOPY_REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
cd "$REPO"
LOG=/tmp/hypercopy-autoupdate.log
exec >>"$LOG" 2>&1
exec 9>/run/hypercopy-autoupdate.lock
flock -n 9 || exit 0
GIT=(git -c safe.directory="$REPO")
PY="$REPO/vps-listener/venv/bin/python"
[ -x "$PY" ] || { echo 'FAIL: bot virtual environment missing'; exit 1; }
OLD=$("${GIT[@]}" rev-parse HEAD)
"${GIT[@]}" fetch origin main
NEW=$("${GIT[@]}" rev-parse origin/main)
[ "$OLD" = "$NEW" ] && { echo "Already current: $OLD"; exit 0; }
# Never discard local code changes. Runtime state is handled separately.
DIRTY=$("${GIT[@]}" diff --name-only HEAD -- . ':!vps-listener/vps_state.json')
[ -z "$DIRTY" ] || { echo "FAIL: locally modified code; leaving $OLD intact"; exit 1; }
mkdir -p /var/lib/hypercopy-backups
chmod 700 /var/lib/hypercopy-backups
BACKUP=$(mktemp -d /var/lib/hypercopy-backups/update.XXXXXX)
STATE="$REPO/vps-listener/vps_state.json"
[ ! -f "$STATE" ] || cp -p "$STATE" "$BACKUP/state.json"
restore_state() { [ ! -f "$BACKUP/state.json" ] || cp -p "$BACKUP/state.json" "$STATE"; }
rollback() {
  echo "Rolling back code to $OLD; preserving runtime state"
  "${GIT[@]}" reset --hard "$OLD"
  restore_state
  systemctl restart hypercopy-listener.service
  exit 1
}
echo "Updating $OLD -> $NEW"
"${GIT[@]}" merge --ff-only origin/main || { restore_state; exit 1; }
restore_state
cd vps-listener
"$PY" tests/test_rules.py || rollback
if [ -f tests/test_cutover.py ]; then
  if ! "$PY" - <<'PY'
import runpy
suite=runpy.run_path('tests/test_cutover.py')
functions=[(name,fn) for name,fn in suite.items() if name.startswith('test_') and callable(fn)]
assert functions, 'No cutover tests discovered'
for name,fn in functions:
    fn()
print('Cutover test functions passed:', len(functions))
PY
  then rollback; fi
fi
EXPECTED=$("$PY" -c 'import re; print(re.search(r"LISTENER_VERSION\s*=\s*\"([^\"]+)\"",open("listener.py").read()).group(1))')
systemctl restart hypercopy-listener.service || rollback
for attempt in $(seq 1 20); do
  if "$PY" - "$EXPECTED" <<'PY'
import sys,json,urllib.request
try:
    with urllib.request.urlopen('http://127.0.0.1:8000/status',timeout=4) as r:
        value=json.load(r)
    assert value.get('ok') is True
    assert value.get('code_version') == sys.argv[1]
except Exception:
    raise SystemExit(1)
PY
  then echo "Deployment verified: $EXPECTED ($NEW)"; exit 0; fi
  sleep 3
done
rollback
