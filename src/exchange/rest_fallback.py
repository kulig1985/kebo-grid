"""Binance REST API fallback — CSAK shutdown / emergency esetén használjuk.

A normál működés WS-en megy. Ez akkor lép be, ha a WS nem reagál és
mindenképp tisztán le kell zárni a state-et.
"""
import hashlib
import hmac
import time
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import httpx

from app.config import ExchangeConfig
from app.log_setup import get_logger

log = get_logger(__name__)


class BinanceRestFallback:
    def __init__(self, config: ExchangeConfig):
        self.config = config

    def _sign(self, params: dict) -> str:
        query = urlencode(params)
        sig = hmac.new(
            self.config.secret_key.encode(),
            query.encode(),
            hashlib.sha256,
        ).hexdigest()
        return f"{query}&signature={sig}"

    @property
    def _headers(self) -> dict:
        return {"X-MBX-APIKEY": self.config.api_key}

    async def cancel_all_open_orders(self, symbol: str) -> list:
        """DELETE /api/v3/openOrders — minden order törlése."""
        params = {
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "recvWindow": 10000,
        }
        url = f"{self.config.rest_url}/api/v3/openOrders?{self._sign(params)}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.delete(url, headers=self._headers)
            if resp.status_code >= 400:
                log.error("REST cancel_all hiba", status=resp.status_code, body=resp.text[:200])
                resp.raise_for_status()
            return resp.json()

    async def get_open_orders(self, symbol: str) -> list:
        params = {
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "recvWindow": 10000,
        }
        url = f"{self.config.rest_url}/api/v3/openOrders?{self._sign(params)}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=self._headers)
            resp.raise_for_status()
            return resp.json()

    async def get_account(self) -> dict:
        params = {
            "timestamp": int(time.time() * 1000),
            "recvWindow": 10000,
        }
        url = f"{self.config.rest_url}/api/v3/account?{self._sign(params)}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=self._headers)
            resp.raise_for_status()
            return resp.json()

    async def market_sell(self, symbol: str, quantity: Decimal, client_order_id: str) -> dict:
        """POST /api/v3/order — MARKET SELL."""
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": "SELL",
            "type": "MARKET",
            "quantity": str(quantity),
            "newClientOrderId": client_order_id,
            "newOrderRespType": "FULL",
            "timestamp": int(time.time() * 1000),
            "recvWindow": 10000,
        }
        url = f"{self.config.rest_url}/api/v3/order?{self._sign(params)}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, headers=self._headers)
            if resp.status_code >= 400:
                log.error("REST market_sell hiba", status=resp.status_code, body=resp.text[:200])
                resp.raise_for_status()
            return resp.json()
