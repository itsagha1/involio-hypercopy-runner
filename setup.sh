#!/usr/bin/env bash
# One-shot VPS setup for the Involio HyperCopy runner (Ubuntu 22.04/24.04).
# Installs python venv and the FastAPI listener as a systemd service.
# (Cloudflared is installed separately - see README "Cloudflare Tunnel".)
# Run as root or with sudo:   sudo bash setup.sh
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)/vps-listener"
RUN_USER="${SUDO_USER:-$(logname 2>/dev/null || echo "${USER:-ubuntu}")}"

echo "==> Installing python3-venv"
apt-get update -qq && apt-get install -y python3-venv python3-pip >/dev/null

echo "==> Creating venv + installing deps"
python3 -m venv "$REPO_DIR/venv"
"$REPO_DIR/venv/bin/pip" -q install -r "$REPO_DIR/requirements.txt"

if [ ! -f "$REPO_DIR/.env" ]; then
  echo "==> Creating $REPO_DIR/.env (EDIT ME)"
  cat > "$REPO_DIR/.env" <<'ENVEOF'
WEBHOOK_SHARED_SECRET=change-me-to-the-same-secret-as-base44
DRY_RUN=true
ENVEOF
  chmod 600 "$REPO_DIR/.env"
else
  echo "==> $REPO_DIR/.env already exists, keeping it"
fi

echo "==> Installing systemd service (hypercopy-listener)"
cat > /etc/systemd/system/hypercopy-listener.service <<SVCEOF
[Unit]
Description=Involio HyperCopy webhook listener
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$RUN_USER
WorkingDirectory=$REPO_DIR
EnvironmentFile=$REPO_DIR/.env
ExecStart=$REPO_DIR/venv/bin/uvicorn listener:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SVCEOF
systemctl daemon-reload
systemctl enable --now hypercopy-listener
sleep 2
systemctl --no-pager status hypercopy-listener | head -5 || true

echo ""
echo "SETUP COMPLETE. Next steps:"
echo "1. Edit the secret in $REPO_DIR/.env (same value as Base44 WEBHOOK_SHARED_SECRET)"
echo "   then: sudo systemctl restart hypercopy-listener"
echo "2. Expose port 8000 with a Cloudflare Tunnel (see README)."
echo "3. Put the tunnel URL + /webhook/involio-delta into Base44 secret VPS_WEBHOOK_URL."
echo "4. Test locally: curl http://127.0.0.1:8000/health"
