"""
Bybit v5 API client for the HyperCopy runner (sub-account keys only).

- Keys live ONLY in vps-listener/.env on the VPS (never in Base44).
- Bybit geo-blocks UAE IPs, so all calls go through the Webshare Spain
  proxy set in BYBIT_PROXY (format: http://user:pass@host:port).
- Two auth modes (auto-detected from .env):
    HMAC (legacy): BYBIT_API_SECRET -> hex HMAC_SHA256(secret,
        timestamp + api_key + recvWindow + (queryString | rawBody))
    RSA (new Bybit flow): BYBIT_RSA_PRIVATE_KEY_FILE -> base64(RSA-SHA256
        PKCS1v15 signature of the same payload).
"""

from __future__ import annotations

import hashlib
from decimal import Decimal, ROUND_FLOOR
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
    def __init__(self, api_key: str | None = None, api_secret: str | None = None,
                 rsa_key_file: str | None = None, proxy: str | None = None,
                 testnet: bool = False):
        self.key = api_key or os.environ.get("BYBIT_API_KEY", "")
        self.secret = api_secret or os.environ.get("BYBIT_API_SECRET", "")
        self.rsa_key = None
        rsa_key_file = rsa_key_file or os.environ.get("BYBIT_RSA_PRIVATE_KEY_FILE", "")
        if not self.secret and rsa_key_file:
            try:
                with open(os.path.abspath(rsa_key_file), "rb") as f:
                    pem = f.read()
                if b"PRIVATE KEY" in pem:
                    from cryptography.hazmat.primitives import serialization
                    self.rsa_key = serialization.load_pem_private_key(pem, password=None)
            except OSError:
                pass
        proxy = proxy or os.environ.get("BYBIT_PROXY", "")
        self.base = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
        self.session = requests.Session()
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}
        self.session.headers["Content-Type"] = "application/json"

    @property
    def configured(self) -> bool:
        return bool(self.key and (self.secret or self.rsa_key))

    def _sign(self, ts: str, param_str: str) -> str:
        msg = f"{ts}{self.key}{RECV_WINDOW}{param_str}".encode()
        if self.secret:  # legacy HMAC
            return hmac.new(self.secret.encode(), msg, hashlib.sha256).hexdigest()
        import base64
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        return base64.b64encode(
            self.rsa_key.sign(msg, padding.PKCS1v15(), hashes.SHA256())
        ).decode()

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
        try:
            r=self.session.get(self.base+path+"?"+qs,headers=headers,timeout=20)
            r.raise_for_status()
            return self._check(r.json(),path)
        except (requests.RequestException,ValueError) as e:
            raise BybitError("Bybit read transport failure: "+type(e).__name__) from None

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
        try:
            r=self.session.post(self.base+path,data=raw.encode(),headers=headers,timeout=20)
            r.raise_for_status()
            return self._check(r.json(),path)
        except (requests.RequestException,ValueError) as e:
            raise BybitError("Bybit write transport outcome uncertain: "+type(e).__name__) from None

    # ---------- market data ----------

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

    def get_available_balance(self, coin: str = "USDT") -> float:
        res = self._get("/v5/account/wallet-balance", {"accountType": "UNIFIED", "coin": coin})
        try:
            acct = res["list"][0]
            avail = float(acct.get("totalAvailableBalance") or 0)
            if avail <= 0:
                avail = float(acct["coin"][0].get("availableToWithdraw") or 0)
            return avail
        except (KeyError, IndexError, TypeError, ValueError) as e:
            raise BybitError(f"could not parse available balance: {e}") from e

    def get_account_summary(self, coin: str = "USDT") -> dict:
        res = self._get("/v5/account/wallet-balance", {"accountType": "UNIFIED", "coin": coin})
        try:
            acct = res["list"][0]
        except (KeyError, IndexError, TypeError) as e:
            raise BybitError(f"could not parse account summary: {e}") from e

        def f(key, default=0.0):
            v = acct.get(key)
            try:
                return float(v) if v not in (None, "") else default
            except (TypeError, ValueError):
                return default

        return {
            "wallet_balance": f("totalWalletBalance"),
            "margin_balance": f("totalMarginBalance"),
            "equity": f("totalEquity"),
            "available_balance": f("totalAvailableBalance"),
            "unrealised_pnl": f("totalPerpUPL"),
            "initial_margin": f("totalInitialMargin"),
            "maintenance_margin": f("totalMaintenanceMargin"),
        }

    def get_closed_pnl(self, start_time_ms: int, category: str = "linear") -> float:
        total, cursor, guard = 0.0, "", 0
        while guard < 10:
            guard += 1
            params = {"category": category, "startTime": start_time_ms, "limit": 200}
            if cursor:
                params["cursor"] = cursor
            res = self._get("/v5/position/closed-pnl", params)
            for row in res.get("list", []):
                try:
                    total += float(row.get("closedPnl") or 0)
                except (TypeError, ValueError):
                    pass
            cursor = res.get("nextPageCursor") or ""
            if not cursor:
                break
        return total

    def get_positions(self, symbol: str | None = None) -> list:
        params = {"category": "linear", "settleCoin": "USDT", "limit": 200}
        if symbol:
            params["symbol"] = symbol
        out, cursor, guard = [], "", 0
        while True:
            if cursor:
                params["cursor"] = cursor
            res = self._get("/v5/position/list", params)
            out.extend(res.get("list", []))
            cursor = res.get("nextPageCursor") or ""
            if not cursor:
                break
            guard += 1
            if guard > 20:
                raise BybitError("position pagination did not terminate")
        return out

    def get_order_history(self, symbol: str, limit: int = 50) -> list:
        params = {"category": "linear", "symbol": symbol, "limit": limit}
        return self._get("/v5/order/history", params).get("list", [])

    # ---------- trading ----------

    def get_linked_order(self,symbol,link):
        params={"category":"linear","symbol":symbol,"orderLinkId":link,"limit":1}
        for path in ("/v5/order/realtime","/v5/order/history"):
            rows=self._get(path,params).get("list",[])
            matches=[r for r in rows if r.get("orderLinkId")==link]
            if matches:return matches[0]
        return None

    def place_order(self,symbol,side,qty,position_idx=0,reduce_only=False,order_link_id=None):
        body={"category":"linear","symbol":symbol,"side":side,"orderType":"Market","qty":str(qty),"positionIdx":position_idx}
        if reduce_only:body["reduceOnly"]=True
        if order_link_id:body["orderLinkId"]=order_link_id
        try:result=self._post("/v5/order/create",body)
        except BybitError as e:
            if not order_link_id:raise
            existing=self.get_linked_order(symbol,order_link_id)
            if not existing:raise e
            result={"orderId":existing.get("orderId"),"recovered":True}
        if order_link_id:
            for attempt in range(3):
                order=self.get_linked_order(symbol,order_link_id)
                if order and order.get("orderStatus")=="Filled" and abs(float(order.get("cumExecQty") or 0)-qty)<=max(1e-9,qty*1e-8):
                    return {**result,"fill_confirmed":True,"filled_qty":float(order["cumExecQty"]),"filled_avg_price":float(order.get("avgPrice") or 0)}
                if order and order.get("orderStatus") in ("Cancelled","PartiallyFilledCanceled") and float(order.get("cumExecQty") or 0)>0:
                    return {**result,"fill_confirmed":True,"partial_fill":True,"filled_qty":float(order["cumExecQty"]),"filled_avg_price":float(order.get("avgPrice") or 0)}
                if order and order.get("orderStatus") in ("Rejected","Cancelled","PartiallyFilledCanceled"):
                    raise BybitError("Linked order not fully filled; reconciliation required: "+order_link_id)
                time.sleep(0.3)
            raise BybitError("Linked order fill not confirmed; reconciliation required: "+order_link_id)
        return result

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        body = {
            "category": "linear",
            "symbol": symbol,
            "buyLeverage": f"{leverage}",
            "sellLeverage": f"{leverage}",
        }
        try:
            return self._post("/v5/position/set-leverage", body)
        except BybitError as e:
            if "110043" in str(e):   # leverage not modified = already correct
                return {}
            raise

    def set_sl_tp(self, symbol: str, stop_loss: float | str | None, take_profit: float | str | None,
                  position_idx: int = 0) -> dict:
        """Sync source SL/TP onto the Bybit position. Bybit v5 uses '0' to clear fields."""
        body = {
            "category": "linear",
            "symbol": symbol,
            "positionIdx": position_idx,
            "stopLoss": format(Decimal(str(stop_loss)),"f") if stop_loss else "0",
            "takeProfit": format(Decimal(str(take_profit)),"f") if take_profit else "0",
        }
        return self._post("/v5/position/set-trading-stop", body)

    def set_trailing_stop(self, symbol: str, active_price: float, trailing_distance: float,
                          position_idx: int = 0) -> dict:
        """Set absolute price trailing stop on Bybit v5 (distance in absolute USDT)."""
        body = {
            "category": "linear",
            "symbol": symbol,
            "positionIdx": position_idx,
            "trailingStop": f"{trailing_distance}",
            "activePrice": f"{active_price}",
        }
        return self._post("/v5/position/set-trading-stop", body)


def position_idx(symbol: str, side: str) -> int:
    # Account is in hedge mode: Buy/long = 1, Sell/short = 2 for ALL linear pairs.
    return 1 if side == "long" else 2


def bybit_side(side: str) -> str:
    return "Buy" if side == "long" else "Sell"


def close_side(side: str) -> str:
    return "Sell" if side == "long" else "Buy"


def coin_to_symbol(coin: str) -> str:
    c = coin.upper()
    if c in ("KPEPE", "1000PEPE"):
        return "1000PEPEUSDT"
    if c in ("KBONK", "1000BONK"):
        return "1000BONKUSDT"
    if c == "PUMP":
        return "PUMPFUNUSDT"
    if c.endswith("USDT"):
        return c
    return c + "USDT"


def symbol_to_coin(symbol: str) -> str:
    s = symbol.upper()
    if s in ("1000PEPEUSDT", "1000PEPE"):
        return "kPEPE"
    if s in ("1000BONKUSDT", "1000BONK"):
        return "kBONK"
    if s in ("PUMPFUNUSDT", "PUMPFUN"):
        return "PUMP"
    if s.endswith("USDT"):
        return symbol[:-4]
    return symbol


def round_qty(qty: float, instrument: dict) -> float:
    step = float(instrument.get("lotSizeFilter", {}).get("qtyStep", "0.001"))
    min_qty = float(instrument.get("lotSizeFilter", {}).get("minOrderQty",
                instrument.get("lotSizeFilter", {}).get("minQty", "0.001")))
    rounded = float(((Decimal(str(qty))/Decimal(str(step)))+Decimal("0.000000001")).to_integral_value(rounding=ROUND_FLOOR)*Decimal(str(step)))
    if rounded < min_qty:
        raise BybitError(
            f"qty {qty} too small: minQty={min_qty} step={step}")
    return round(rounded, 10)


def round_step(val: float, step: float) -> float:
    if step <= 0:
        return val
    return round(round(val / step) * step, 8)
