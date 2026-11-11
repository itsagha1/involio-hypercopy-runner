#!/bin/bash
# tunnel-heartbeat: keeps Base44 pointed at the current quick-tunnel URL.
#
# Owner 2026-11-10 (integration-credit cut): this used to POST to the
# reportTunnelUrl backend function every 5 minutes - 288 billed function
# invocations a day for a value that changes only when the tunnel restarts.
# Now it reports only when the URL actually changed, with one confirmation a
# day so a lost Base44 record still heals itself.
set -u
ENV_FILE="/home/ubuntu/involio-hypercopy-runner/vps-listener/.env"
ENDPOINT="https://superagent-85fc14c0.base44.app/functions/reportTunnelUrl"
STATE_DIR="/var/lib/hypercopy-copy"
STATE="$STATE_DIR/last_tunnel_report"

URL=$(journalctl -u cloudflared-tunnel --no-pager 2>/dev/null \
      | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | tail -1)
[ -z "$URL" ] && exit 0

mkdir -p "$STATE_DIR" 2>/dev/null
LAST=""
AGE=999999
if [ -f "$STATE" ]; then
  LAST=$(cat "$STATE" 2>/dev/null)
  NOW=$(date +%s)
  MOD=$(stat -c %Y "$STATE" 2>/dev/null || echo 0)
  AGE=$(( NOW - MOD ))
fi

# unchanged URL and already confirmed today -> nothing to bill
if [ "$URL" = "$LAST" ] && [ "$AGE" -lt 86400 ]; then
  exit 0
fi

SECRET=$(grep '^WEBHOOK_SHARED_SECRET=' "$ENV_FILE" 2>/dev/null | cut -d= -f2-)
[ -z "$SECRET" ] && exit 0

curl -s -m 15 -X POST "$ENDPOINT" \
     -H 'Content-Type: application/json' \
     -d "{\"url\":\"$URL\",\"secret\":\"$SECRET\"}" >/dev/null 2>&1

echo "$URL" > "$STATE" 2>/dev/null
exit 0
