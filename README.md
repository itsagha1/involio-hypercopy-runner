# Involio → HyperCopy Runner

Demo pipeline that polls 3 Involio profiles, records deltas in a Base44
entity (`InvolioDelta`), and fires a webhook to a VPS listener that runs
the trade-management logic off Base44.

```
Involio profiles ──(5-min poll)──> poll_involio.py
                                     │
                                     ├─ record ──> Base44 InvolioDelta entity
                                     └─ webhook ─> VPS listener (DRY_RUN mode)
                                                      └─ reports action back
```

**Profiles watched:** limpan96 (Scalping), akira (Crypto), nathanbrown (The Bakery)

Everything here is **DEMO / DRY_RUN** — no real orders, no real credentials.
Secrets are provided via environment variables / GitHub Secrets.

## Layout

- `poll_involio.py` — the poller (GitHub Actions or any machine)
- `.github/workflows/poll.yml` — 5-minute cron schedule on GitHub Actions
- `vps-listener/listener.py` — FastAPI webhook receiver + demo trade logic
- `vps-listener/requirements.txt`

## GitHub Actions setup

1. Push this repo (done).
2. Repo Settings → Secrets and variables → Actions, add:
   - `BASE44_APP_ID`
   - `BASE44_API_KEY`
   - `VPS_WEBHOOK_URL` (from Cloudflare Tunnel, e.g. https://xxx.trycloudflare.com/webhook/involio-delta)
   - `WEBHOOK_SHARED_SECRET`
3. Actions tab → enable the "Involio delta poll" workflow → Run manually once to test.

## VPS setup (Ubuntu)

```bash
sudo apt update && sudo apt install -y python3-pip
git clone https://github.com/itsagha1/involio-hypercopy-runner
cd involio-hypercopy-runner/vps-listener
pip3 install -r requirements.txt
export WEBHOOK_SHARED_SECRET="choose-a-long-random-string"
uvicorn listener:app --host 127.0.0.1 --port 8000
```

### Cloudflare Tunnel (free, no open ports)

```bash
# install cloudflared, then:
cloudflared tunnel --url http://127.0.0.1:8000
# note the https://xxx.trycloudflare.com URL it prints
```

Point `VPS_WEBHOOK_URL` at that URL + `/webhook/involio-delta`.

### Run as a service (survives reboots)

```bash
sudo tee /etc/systemd/system/involio-listener.service <<'EOF'
[Unit]
Description=Involio webhook listener
After=network.target

[Service]
WorkingDirectory=/opt/involio-hypercopy-runner/vps-listener
ExecStart=/usr/bin/uvicorn listener:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=5
Environment=WEBHOOK_SHARED_SECRET=change-me

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl enable --now involio-listener
```

## Status

- [x] Repo + GitHub Actions workflow created (demo)
- [x] VPS listener script (demo, DRY_RUN)
- [ ] Real Involio endpoints wired into `poll_involio.py`
- [ ] Base44 backend function (poll → record → webhook)
- [ ] Live end-to-end test
- [ ] Switch from DRY_RUN to real credentials
