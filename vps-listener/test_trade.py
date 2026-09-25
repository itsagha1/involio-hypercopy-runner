"""
One-off LIVE test trade to verify the whole Bybit execution path.

Reads keys/proxy from vps-listener/.env, opens a small market position on a
linear USDT perp, shows it live on the account, then closes it immediately.

This is a MANUAL exception to the manage-only / no-new-entries rule: run it
once to prove signing, proxy, orders and position reads all work, then leave
the listener to manage-only mode.

Usage (on the VPS):
    cd ~/involio-hypercopy-runner/vps-listener
    ./venv/bin/python test_trade.py --symbol BTCUSDT --margin 5 --leverage 20
    add --yes to skip the confirmation prompt.
"""

import argparse
import time

from bybit_api import BybitClient, BybitError, bybit_side, close_side, position_idx, round_qty


def load_env(path: str = ".env") -> None:
    import os
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--margin", type=float, default=5.0, help="USDT margin to use")
    p.add_argument("--leverage", type=float, default=20.0)
    p.add_argument("--side", default="long", choices=["long", "short"])
    p.add_argument("--yes", action="store_true", help="skip confirmation")
    p.add_argument("--keep", action="store_true", help="do NOT close the position after opening")
    args = p.parse_args()

    load_env()

    client = BybitClient()
    if not client.configured:
        raise SystemExit("ERROR: BYBIT_API_KEY / BYBIT_API_SECRET missing from .env")

    print(f"[1/6] keys loaded. proxy: {'set' if client.session.proxies else 'NOT SET (UAE IP will be geo-blocked!)'}")

    ticker = client.get_ticker(args.symbol)
    price = float(ticker["lastPrice"])
    print(f"[2/6] {args.symbol} last price: {price}")

    instrument = client.get_instrument(args.symbol)
    lot = instrument.get("lotSizeFilter", {})
    notional = args.margin * args.leverage
    qty = round_qty(notional / price, instrument)
    print(f"[3/6] lot rules: minQty={lot.get('minQty')} step={lot.get('qtyStep')}"
          f" -> qty={qty} (~{qty * price:.2f} USDT notional, {args.margin} USDT margin @ {args.leverage}x)")

    if not args.yes:
        ans = input(f"Open {args.side} {qty} {args.symbol} (~{args.margin:.0f} USDT margin, real money)? [y/N] ")
        if ans.strip().lower() not in ("y", "yes"):
            raise SystemExit("aborted by user")

    idx = position_idx(args.symbol, args.side)
    order = client.place_order(args.symbol, bybit_side(args.side), qty, position_idx=idx)
    print(f"[4/6] OPEN order placed: orderId={order.get('orderId')}")

    time.sleep(3)
    positions = client.get_positions(args.symbol)
    ours = [x for x in positions if x.get("side") == ("Buy" if args.side == "long" else "Sell")]
    if ours:
        pos = ours[0]
        print(f"[5/6] LIVE POSITION: {pos.get('symbol')} size={pos.get('size')} "
              f"avgPrice={pos.get('avgPrice')} unrealisedPnl={pos.get('unrealisedPnl')} "
              f"leverage={pos.get('leverage')} positionIdx={pos.get('positionIdx')}")
    else:
        print("[5/6] WARNING: position not found after 3s (it may have closed already)")

    if args.keep:
        print("[6/6] --keep given: position left open. Close it manually when done testing.")
        return

    close = client.place_order(args.symbol, close_side(args.side), qty,
                               position_idx=idx, reduce_only=True)
    print(f"[6/6] CLOSE order placed: orderId={close.get('orderId')} — test complete.")
    print("If all six steps printed, signing, proxy, orders and position reads all work.")
    print("Next: sudo systemctl restart hypercopy-listener and flip DRY_RUN=false when ready.")


if __name__ == "__main__":
    try:
        main()
    except BybitError as e:
        raise SystemExit(f"BYBIT ERROR: {e}")
