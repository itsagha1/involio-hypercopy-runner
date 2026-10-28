#!/usr/bin/env python3
"""Backtest: uniform add ladder (5/5/5) vs owner-proposed (5/10/10 -> cum -5/-15/-25) vs SL variants.
Matches bot v1.0.8 mechanics: trigger +-10% over 150min, 1% margin per entry/add at 10x,
min add age 5min, SL 20% from avg entry, trailing ROE>=50% activate / 1% distance,
profit cooldown 24h, entry cooldown 60min, taker fees 0.05% per side."""
import json, time, urllib.request

FAPI = "https://fapi.binance.com/fapi/v1/klines"
SYMBOLS = ["BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT","ADAUSDT","LINKUSDT","AVAXUSDT","SUIUSDT","POLUSDT",
           "ATOMUSDT","AAVEUSDT","ARBUSDT","DOGEUSDT","LTCUSDT","TRXUSDT","BCHUSDT","APTUSDT","XLMUSDT","UNIUSDT",
           "NEARUSDT","VETUSDT","HBARUSDT","OPUSDT","INJUSDT","ALGOUSDT","RENDERUSDT","GRTUSDT","IMXUSDT","FLOWUSDT",
           "ZECUSDT","LDOUSDT","STRKUSDT","CRVUSDT","FETUSDT","SANDUSDT","MANAUSDT","RUNEUSDT","DYDXUSDT","SEIUSDT",
           "TIAUSDT","ARUSDT","ONDOUSDT","JUPUSDT","TAOUSDT","STXUSDT","GMXUSDT","PENDLEUSDT","ENAUSDT","CAKEUSDT",
           "GALAUSDT","THETAUSDT","IOTAUSDT","CFXUSDT","MINAUSDT","ANKRUSDT","ENSUSDT","UMAUSDT","BANDUSDT","WLDUSDT",
           "ORDIUSDT","ARKMUSDT","AEVOUSDT","JTOUSDT","BLURUSDT","SUPERUSDT","ZRXUSDT","AXSUSDT","APEUSDT","CHRUSDT"]
DAYS = 60
TRIGGER, WINDOW_BARS = 10.0, 10
MARGIN_PCT, LEV, MAX_ADDS = 1.0, 10, 3
TRAIL_ACT_ROE, TRAIL_DIST = 50.0, 1.0
FEE = 0.0005
COOLDOWN_MIN, PROFIT_COOLDOWN_MIN, MIN_ADD_AGE_MIN = 60, 1440, 5

def fetch(symbol):
    bars, end = [], int(time.time() * 1000)
    start0 = end - DAYS * 86400000
    for off in range(0, DAYS * 86400, 1500 * 900):
        url = f"{FAPI}?symbol={symbol}&interval=15m&limit=1500&startTime={start0 + off*1000}"
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                got = json.load(r)
                for k in got:
                    bars.append([float(k[1]), float(k[2]), float(k[3]), float(k[4])])
                if len(got) < 1500:
                    break
        except Exception:
            break
        time.sleep(0.1)
    return bars

def simulate(bars, steps, sl_pct, sym):
    wallet = 1000.0
    trades = []
    cooldown_until = 0
    pos = None
    for i in range(WINDOW_BARS + 1, len(bars)):
        now = i * 900
        o, h, l, c = bars[i]
        if pos:
            side, qty, margin, avg, last_fill, adds, opened, peak = pos
            stop = avg * (1 - sl_pct/100.0) if side == "LONG" else avg * (1 + sl_pct/100.0)
            if adds < MAX_ADDS and now - opened >= MIN_ADD_AGE_MIN * 60:
                step = steps[min(adds, len(steps) - 1)]
                trig = last_fill * (1 - step/100.0) if side == "LONG" else last_fill * (1 + step/100.0)
                if (side == "LONG" and l <= trig) or (side == "SHORT" and h >= trig):
                    m = wallet * MARGIN_PCT / 100.0
                    nq = (m * LEV) / trig
                    tot = qty + nq
                    avg = (avg * qty + trig * nq) / tot
                    qty, margin, last_fill, adds = tot, margin + m, trig, adds + 1
            exit_px, reason = None, None
            if (side == "LONG" and l <= stop) or (side == "SHORT" and h >= stop):
                exit_px, reason = stop, "sl"
            else:
                if side == "LONG":
                    peak = max(peak, h)
                    roe = (peak - avg) / avg * LEV * 100
                    if roe >= TRAIL_ACT_ROE and l <= peak * (1 - TRAIL_DIST/100.0):
                        exit_px, reason = peak * (1 - TRAIL_DIST/100.0), "trail"
                else:
                    peak = min(peak, h)
                    roe = (avg - peak) / avg * LEV * 100
                    if roe >= TRAIL_ACT_ROE and h >= peak * (1 + TRAIL_DIST/100.0):
                        exit_px, reason = peak * (1 + TRAIL_DIST/100.0), "trail"
            if exit_px is not None:
                pnl = (exit_px - avg) * qty if side == "LONG" else (avg - exit_px) * qty
                fees = (avg * qty + exit_px * qty) * FEE
                wallet += pnl - fees
                trades.append((pnl - fees, reason, adds, sym))
                cooldown_until = now + (PROFIT_COOLDOWN_MIN if pnl - fees > 0 else COOLDOWN_MIN) * 60
                pos = None
            else:
                pos = [side, qty, margin, avg, last_fill, adds, opened, peak]
                continue
        if pos is None and cooldown_until <= now and i > WINDOW_BARS:
            prev = bars[i - 1 - WINDOW_BARS][3]
            ref = bars[i - 1][3]
            move = (ref - prev) / prev * 100.0
            entry = o
            m = wallet * MARGIN_PCT / 100.0
            if m * LEV >= 5.0:
                if move <= -TRIGGER:
                    pos = ["LONG", (m * LEV) / entry, m, entry, entry, 0, now, entry]
                elif move >= TRIGGER:
                    pos = ["SHORT", (m * LEV) / entry, m, entry, entry, 0, now, entry]
    return trades

def run(label, steps, sl_pct, data):
    all_t = []
    for sym, bars in data.items():
        all_t += simulate(bars, steps, sl_pct, sym)
    wins = [t for t in all_t if t[0] > 0]
    losses = [t for t in all_t if t[0] <= 0]
    tot = sum(t[0] for t in all_t)
    worst = min(all_t, key=lambda t: t[0]) if all_t else (0, "", 0, "")
    sl_hits = sum(1 for t in all_t if t[1] == "sl")
    add3 = sum(1 for t in all_t if t[2] == 3)
    print(f"{label}: trades={len(all_t)} W/L={len(wins)}/{len(losses)} "
          f"netPnL={tot:+.2f} avg={tot/max(1,len(all_t)):+.3f} "
          f"worst={worst[0]:+.2f}({worst[3]}) SLhits={sl_hits} 3addTrades={add3}")
    return tot

if __name__ == "__main__":
    data = {}
    for s in SYMBOLS:
        b = fetch(s)
        if len(b) > 200:
            data[s] = b
    print(f"data: {len(data)} symbols, {sum(len(v) for v in data.values())} bars")
    run("CURRENT  5/5/5   SL20", [5.0, 5.0, 5.0], 20.0, data)
    run("NEW      5/10/10 SL20", [5.0, 10.0, 10.0], 20.0, data)
    run("NEW      5/10/10 SL25", [5.0, 10.0, 10.0], 25.0, data)
