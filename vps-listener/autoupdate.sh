#!/usr/bin/env bash
# Auto-update for the HyperCopy listener (2026-09-27).
#
# Runs as a systemd timer (see install_autoupdate.sh). Every cycle:
#   1. git fetch origin main
#   2. if a new commit exists: git pull --ff-only
#   3. run the offline test suite (tests/test_rules.py, no network)
#   4. tests pass  -> restart hypercopy-listener on the new code
#      tests fail  -> git reset back to the previous commit and restart
#                     that (never leaves the bot on broken code)
#
# The owner authorized the agent to fix bugs autonomously (2026-09-27);
# this script is the deploy channel for fixes pushed to GitHub. Set
# SKIP_RESTART=1 to test the script without touching the service.
set -u

REPO="$(cd "$(dirname "$0")/.." && pwd)"
LOG="/tmp/hypercopy-autoupdate.log"
SERVICE="hypercopy-listener"
RUN_USER="ubuntu"

log() { echo "[$(date -u +%FT%TZ)] $*" >> "$LOG"; }

cd "$REPO" || { log "FATAL: repo dir missing: $REPO"; exit 1; }
log "run start"

git -c safe.directory="$REPO" fetch origin main --quiet 2>>"$LOG" \
  || { log "fetch failed (network?) - keeping current code"; exit 0; }

LOCAL="$(git rev-parse HEAD 2>/dev/null || echo none)"
REMOTE="$(git rev-parse origin/main 2>/dev/null || echo none)"
if [ "$LOCAL" = "$REMOTE" ]; then
  log "up to date ($LOCAL)"
  exit 0
fi

log "new commit $REMOTE (was $LOCAL), pulling"
if ! git -c safe.directory="$REPO" pull --ff-only origin main --quiet >>"$LOG" 2>&1; then
  log "pull failed - keeping current code $LOCAL"
  git -c safe.directory="$REPO" reset --hard "$LOCAL" >>"$LOG" 2>&1
  exit 0
fi

cd vps-listener || { log "FATAL: vps-listener missing"; exit 1; }
if python3 tests/test_rules.py >>"$LOG" 2>&1; then
  log "tests passed on $REMOTE"
  if [ "${SKIP_RESTART:-0}" != "1" ] && command -v systemctl >/dev/null 2>&1; then
    systemctl restart "$SERVICE"
    log "listener restarted on $REMOTE"
  else
    log "restart skipped (SKIP_RESTART or no systemctl)"
  fi
else
  log "tests FAILED on $REMOTE - rolling back to $LOCAL"
  cd "$REPO"
  git -c safe.directory="$REPO" reset --hard "$LOCAL" >>"$LOG" 2>&1
  if [ "${SKIP_RESTART:-0}" != "1" ] && command -v systemctl >/dev/null 2>&1; then
    systemctl restart "$SERVICE"
    log "listener restarted on rollback $LOCAL"
  fi
fi

# keep repo ownership consistent (script may run as root)
[ "$(id -u)" = "0" ] && chown -R "$RUN_USER":"$RUN_USER" "$REPO" >/dev/null 2>&1
log "run done"
