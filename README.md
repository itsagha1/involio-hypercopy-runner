# Involio → HyperCopy Runner

Hybrid copy-trading pipeline. **Base44 does the cheap work** (a free 5-minute
poll of the Involio books, writing `InvolioDelta` records for the dashboard),
and **this repo runs on your VPS** (the heavy trade-management logic) via a
webhook. No LLM credits are spent per poll.

```
Involio books ──(Base44 backend function, every 5 min, free)──> pollInvolioDeltas
                                     │
                                     ├─ write ──> Base44 InvolioDelta records (dashboard)
                                     └─ webhook ─> https://<your-tunnel>/webhook/involio-delta
                                                      │
                                                      └─ VPS listener (this repo)
                                                          - verifies signature
                                                          - keeps book state
                                                          - applies manage-only rules
                                                          - DRY_RUN=true: logs planned actions
                                                          - live mode: Bybit orders (keys only on VPS)
```

**Traders watched:** limpan96 (Scalping), nathanbrown (The Bakery), akira (Crypto)

**Manage-only rules (owner directive 2026-09-22):** new Involio entries are
paused. Only positions already open on Bybit sub-account `AIsub587820763`
are managed. No realized losses — dropped sources are closed only when in
comfortable profit, otherwise a 1% Bybit trailing stop protects breakeven.
Sizing per trader: full sub-account equity base, max 50 USDT, min 2 USDT.

## Layout

- `vps-listener/listener.py` — FastAPI webhook receiver + manage-only logic (DRY_RUN by default)
- `setup.sh` — one-shot Ubuntu installer (venv + systemd service)
- `poll_involio.py` — standalone Python poller with the real Involio API (backup / manual runs)
- `.github/workflows/poll.yml` — optional GitHub Actions fallback poller

## VPS setup (Oracle Cloud Free Tier, Dubai / Vultr)

1. **Create the VM** (Oracle: Compute → Create instance, Ubuntu 22.04,
   shape Ampere A1 Flex 1 OCPU / 6 GB RAM — always free; pick the Dubai
   region `me-dubai-1`). Download the SSH key pair during creation.
2. **Connect:** `ssh -i <your-key>.key ubuntu@<PUBLIC-IP>`
3. **Install:**
   ```bash
   sudo apt update && sudo apt install -y git
   git clone https://github.com/itsagha1/involio-hypercopy-runner.git
   cd involio-hypercopy-runner
   sudo bash setup.sh
   ```
4. **Set the webhook secret** in `vps-listener/.env`
   (`WEBHOOK_SHARED_SECRET=...`, same value as the Base44 secret) and
   `sudo systemctl restart hypercopy-listener`.
5. **Check:** `curl http://127.0.0.1:8000/health` → `{"ok": true, ...}`

## Cloudflare Tunnel

Keeps the VPS firewalled — no inbound ports open, the tunnel dials out.

- Install cloudflared and connect per Cloudflare's docs:
  https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/
- Quick tunnel (URL changes on restart, fine for testing):
  `cloudflared tunnel --url http://127.0.0.1:8000`
- Named tunnel via the Zero Trust dashboard (recommended: stable URL) or
  `sudo cloudflared service install <TUNNEL_TOKEN>`.
- Your webhook URL is `https://<tunnel-host>/webhook/involio-delta`.
- Put it into the Base44 secret `VPS_WEBHOOK_URL` and the next poll with
  activity will start delivering.

## Secrets

| Where | Secret |
|---|---|
| Base44 (agent settings) | `VPS_WEBHOOK_URL`, `WEBHOOK_SHARED_SECRET` |
| VPS `vps-listener/.env` | `WEBHOOK_SHARED_SECRET`, `DRY_RUN` |
| VPS (only when going live) | `BYBIT_API_KEY`, `BYBIT_API_SECRET`, Bybit proxy |

Bybit keys **never** enter Base44 or this repo.

## Standalone poller (`poll_involio.py`)

Backup poller with the real Involio API, for manual runs or if you ever move
polling off Base44:

```bash
INVOLIO_REFRESH_TOKEN=<token> python3 poll_involio.py
```

Optionally records to Base44 (`BASE44_APP_ID` + `BASE44_API_KEY`) and fires
the same webhook (`WEBHOOK_URL`).
