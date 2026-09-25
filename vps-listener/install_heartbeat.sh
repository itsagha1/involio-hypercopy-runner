#!/bin/bash
# One-shot installer for the tunnel heartbeat systemd timer (run with sudo).
set -e
DIR=/home/ubuntu/involio-hypercopy-runner/vps-listener

cat > /etc/systemd/system/tunnel-heartbeat.service <<UNIT
[Unit]
Description=Report current tunnel URL to Base44

[Service]
Type=oneshot
ExecStart=$DIR/heartbeat.sh
UNIT

cat > /etc/systemd/system/tunnel-heartbeat.timer <<TIMER
[Unit]
Description=Run tunnel heartbeat every 5 minutes

[Timer]
OnBootSec=2min
OnUnitActiveSec=5min
AccuracySec=10s

[Install]
WantedBy=timers.target
TIMER

systemctl daemon-reload
systemctl enable --now tunnel-heartbeat.timer
echo "HEARTBEAT INSTALLED: timer active every 5 min"
systemctl list-timers tunnel-heartbeat* --no-pager | head -3
