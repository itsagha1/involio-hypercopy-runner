#!/usr/bin/env bash
# Install the copy-fleet self-audit timer (run once, then it survives reboots).
set -euo pipefail
cd "$(dirname "$0")"
sudo cp hypercopy-audit.service hypercopy-audit.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hypercopy-audit.timer
systemctl list-timers hypercopy-audit.timer --no-pager | head -3
