#!/usr/bin/env bash
# One-time installer for the HyperCopy auto-update timer (2026-09-27).
# Run ONCE on the VM with sudo:
#   sudo bash install_autoupdate.sh
# After this, fixes pushed to GitHub deploy themselves every 15 minutes
# (pull -> offline tests -> restart, auto-rollback if tests fail).
set -eu

DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$(dirname "$DIR")"
echo "Installing autoupdate units for repo at $REPO"

cat > /etc/systemd/system/hypercopy-autoupdate.service <<EOF
[Unit]
Description=HyperCopy listener auto-update (pull + test + restart)

[Service]
Type=oneshot
ExecStart=$DIR/autoupdate.sh
EOF

cat > /etc/systemd/system/hypercopy-autoupdate.timer <<EOF
[Unit]
Description=Check for HyperCopy updates every 15 minutes

[Timer]
OnCalendar=*:0/15
Persistent=true

[Install]
WantedBy=timers.target
EOF

chmod +x "$DIR/autoupdate.sh"
systemctl daemon-reload
systemctl enable --now hypercopy-autoupdate.timer

echo "Installed. Status:"
systemctl status hypercopy-autoupdate.timer --no-pager | head -5
echo "Log: /tmp/hypercopy-autoupdate.log"
