#!/bin/bash
# tunnel-heartbeat: reports the CURRENT quick-tunnel URL to Base44 every
# 5 min so the pipeline re-targets itself after any VM reboot (self-heal).
# Runs as root via systemd (the .env is root-owned).
set -u
ENV_FILE="/home/ubuntu/involio-hypercopy-runner/vps-listener/.env"
ENDPOINT="https://superagent-85fc14c0.base44.app/functions/reportTunnelUrl"

URL=$(journalctl -u cloudflared-tunnel --no-pager -n 500 2>/dev/null \
      | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | tail -1)
[ -z "$URL" ] && exit 0

SECRET=$(grep '^WEBHOOK_SHARED_SECRET=' "$ENV_FILE" 2>/dev/null | cut -d= -f2-)
[ -z "$SECRET" ] && exit 0

curl -s -m 15 -X POST "$ENDPOINT" \
     -H 'Content-Type: application/json' \
     -d "{\"url\":\"$URL\",\"secret\":\"$SECRET\"}" >/dev/null 2>&1
exit 0
