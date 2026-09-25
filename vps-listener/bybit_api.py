"""
Bybit v5 API client for the HyperCopy runner (sub-account keys only).

- Keys live ONLY in vps-listener/.env on the VPS (never in Base44).
- Bybit geo-blocks UAE IPs, so all calls go through the Webshare Spain
  proxy set in BYBIT_PROXY (format: http://user:pass@host:port).
- Standard Bybit v5 HMAC signing: sign = HMAC_SHA256(secret,
  timestamp + api_key + recvWindow + (queryString | rawBody)).

Manage-only rule details encoded here:
  - HYPE uses hedge-mode positionIdx: 1 long / 2 short; everything else 0.
"""

import hashlib
import hmac
import json
import os
import time
import urllib.parse

import requests

RECV_WINDOW = "5000"


class BybitError(Exception):
    pass


class BybitClient:
    def __init__(self, api_key=None, api_secret=None, proxy=None, testnet=False):
        self.key = api_key or os.environ.get("BYBIT_API_KEY", "")
        self.secret = api_secret or os.environ.get("BYBIT_API_SECRET", "")
        proxy = proxy or os.environ.get("BYBIT_PROXY", "")
        self.base = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
        self.session = requests.Session()
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}
        self.session.headers["Content-Type"] = "application/json"

    @property
    def configured(self) -> bool:
        return bool(self.key and self.secret)

    def _sign(self, ts: str, param_str: str) -> str:
        msg = f"{ts}{self.key}{RECV_WINDOW}{param_str}"
        return hmac.new(self.secret.encode(), msg.encode(), hashlib.sha256).hexdigest()

    def _check(self, data: dict, path: str) -> dict:
        if data.get("retCode") != 0:
            raise BybitError(f"{path} -> retCode={data.get('retCode')} retMsg={data.get('retMsg')}")
        return data.get("result", {})

    def _get(self, path: str, params: dict) -> dict:
        if not self.configured:
            raise BybitError("Bybit keys not configured in .env")
        ts = str(int(time.time() * 1000))
        qs = urllib.parse.urlencode(params)
        headers = {
            "X-BAPI-API-KEY": self.key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": RECV_WINDOW,
            "X-BAPI-SIGN": self._sign(ts, qs),
        }
        r = self.session.get(self.base + path + "?" + qs, headers=headers, timeout=20)
        return self._check(r.json(), path)

    def _post(self, path: str, body: dict) -> dict:
        if not self.configured:
            raise BybitError("Bybit keys not configured in .env")
        ts = str(int(time.time() * 1000))
        raw = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        headers = {
            "X-BAPI-API-KEY": self.key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": RECV_WINDOW,
            "X-BAPI-SIGN": self._sign(ts, raw),
        }
        r = self.session.post(self.base + path, data=raw.encode(), headers=headers, timeout=20)
        return self._check(r.json(), path)

    # ---------- market data (public, still proxied) ----------

    def get_ticker(self, symbol: str) -> dict:
        res = self._get("/v5/market/tickers", {"category": "linear", "symbol": symbol})
        lst = res.get("list", [])
        if not lst:
            raise BybitError(f"no ticker for {symbol}")
        return lst[0]

    def get_instrument(self, symbol: str) -> dict:
        res = self._get("/v5/market/instruments-info", {"category": "linear", "symbol": symbol})
        lst = res.get("list", [])
        if not lst:
            raise BybitError(f"no instrument info for {symbol}")
        return lst[0]

    # ---------- account / positions ----------

    def get_balance(self, coin: str = "USDT") -> float:
        res = self._get("/v5/account/wallet-balance", {"accountType": "UNIFIED", "coin": coin})
        try:
            return float(res["list"][0]["coin"][0]["walletBalance"])
        except (KeyError, IndexError, TypeError, ValueError) as e:
            raise BybitError(f"could not parse wallet balance: {e}") from e

    def get_positions(self, symbol: str = None) -> list:
        params = {"category": "linear", "settleCoin": "USDT"}
        if symbol:
            params["symbol"] = symbol
        return self._get("/v5/position/list", params).get("list", [])

    # ---------- trading ----------

    def place_order(self, symbol: str, side: str, qty: float, position_idx: int = 0,
                    reduce_only: bool = False) -> dict:
        body = {
            "category": "linear",
            "symbol": symbol,
            "side": side,  # "Buy" opens long / closes short; "Sell" the reverse
            "orderType": "Market",
            "qty": f"{qty}",
            "positionIdx": position_idx,
        }
        if reduce_only:
            body["reduceOnly"] = True
        return self._post("/v5/order/create", body)

    def set_trailing_stop(self, symbol: str, active_price: float, distance_pct: float,
                          position_idx: int = 0) -> dict:
        body = {
            "category": "linear",
            "symbol": symbol,
            "positionIdx": position_idx,
            "trailingStop": f"{distance_pct}",
            "activePrice": f"{active_price}",
        }
        return self._post("/v5/position/set-trading-stop", body)


def position_idx(symbol: str, side: str) -> int:
    """HYPE runs in hedge mode: 1 = long, 2 = short. Everything else one-way: 0."""
    if symbol.startswith("HYPE"):
        return 1 if side == "long" else 2
    return 0


def bybit_side(side: str) -> str:
    """Involio 'long'/'short' -> Bybit open-side 'Buy'/'Sell'."""
    return "Buy" if side == "long" else "Sell"


def close_side(side: str) -> str:
    """Side that closes a position opened with `side`."""
    return "Sell" if side == "long" else "Buy"


def coin_to_symbol(coin: str) -> str:
    return coin.upper() + "USDT"


def symbol_to_coin(symbol: str) -> str:
    return symbol[:-4] if symbol.endswith("USDT") else symbol


def round_qty(qty: float, instrument: dict) -> float:
    """Round qty down onto the symbol's lot grid; raise if below minQty."""
    step = float(instrument.get("lotSizeFilter", {}).get("qtyStep", "0.001"))
    min_qty = float(instrument.get("lotSizeFilter", {}).get("minQty", "0.001"))
    rounded = max(0.0, (int(qty / step)) * step)
    if rounded < min_qty:
        raise BybitError(
            f"qty {qty} too small: minQty={min_qty} step={step} (increase --margin or --leverage)")
    return round(rounded, 10)
