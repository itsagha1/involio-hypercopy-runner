#!/bin/bash
# Install ONE 100%-copy instance (copy_listener.py) for a dedicated Bybit subaccount.
# Usage: sudo bash install_copy_account.sh <name> <involio_profile> <portfolio_id> <port>
# Env file /etc/copy-<name>.env must already exist (root, 600) with:
#   BYBIT_API_KEY=...            (subaccount key; RSA file or BYBIT_API_SECRET)
#   BYBIT_RSA_PRIVATE_KEY_FILE=  (path, like main account) or BYBIT_API_SECRET=
#   BYBIT_PROXY=http://user:pass@64.137.96.74:6641   (Webshare Spain proxy, IP-whitelisted)
#   INVOLIO_REFRESH_TOKEN=...     (shared reusable token)
#   DRY_RUN=false                 (only when going live)
set -euo pipefail
NAME="$1"; PROFILE="$2"; PORTFOLIO="$3"; PORT="$4"
[ -z "$NAME" ] || [ -z "$PROFILE" ] || [ -z "$PORTFOLIO" ] || [ -z "$PORT" ] && { echo "usage: $0 <name> <profile> <portfolio_id> <port>"; exit 1; }
ENVF="/etc/copy-${NAME}.env"
[ -f "$ENVF" ] || { echo "missing $ENVF"; exit 1; }
RUN=/var/lib/hypercopy-copy
mkdir -p "$RUN"

cat > /etc/systemd/system/copy-${NAME}.service <<EOF
[Unit]
Description=100%% Involio copy instance ${NAME} (${PROFILE})
After=network-online.target

[Service]
EnvironmentFile=${ENVF}
Environment=COPY_NAME=${NAME}
Environment=COPY_PROFILE=${PROFILE}
Environment=COPY_PORTFOLIO_ID=${PORTFOLIO}
Environment=COPY_PORT=${PORT}
Environment=COPY_STATE_FILE=${RUN}/${NAME}_state.json
Environment=COPY_LOG_FILE=${RUN}/${NAME}_actions.log
WorkingDirectory=/home/ubuntu/involio-hypercopy-runner/vps-listener
ExecStart=/home/ubuntu/involio-hypercopy-runner/vps-listener/venv/bin/uvicorn copy_listener:app --host 127.0.0.1 --port ${PORT}
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now copy-${NAME}.service
sleep 3
systemctl is-active copy-${NAME}.service
curl -s "http://127.0.0.1:${PORT}/status" | head -c 400; echo
echo "installed copy-${NAME} on port ${PORT}"
