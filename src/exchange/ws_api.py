"""
Binance Trading WebSocket API kliens.

Két feladat:
- writer_task: WS send queue-ból olvas és küld
- reader_task: WS válaszokat fogad, hibákat logol

Az order.place SOHA nem vár ACK-ra – teljesen non-blocking.
Az igazság forrása kizárólag a user data stream executionReport.
"""
import asyncio
import json
import random
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from app.config import ExchangeConfig
from app.log_setup import get_logger
from .signing import sign_params, make_timestamp

log = get_logger(__name__)

# Újracsatlakozási konfig
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 60.0
RECONNECT_BEFORE_HOURS = 23  # 24h limit előtt 1 órával cseréljük


@dataclass
class WsSendCommand:
    """Egy WS API kérelem a send queue-ban."""
    request_id: str
    method: str
    params: dict[str, Any]
    is_authenticated: bool = True

    def to_dict(self) -> dict:
        return {
            "id": self.request_id,
            "method": self.method,
            "params": self.params,
        }


class BinanceWsApi:
    """
    Binance WebSocket API kliens.

    Két belső task kommunikál egymással:
    - writer_loop: send_queue -> WS
    - reader_loop: WS -> response_queue (telemetria/hibák)
    """

    def __init__(
        self,
        config: ExchangeConfig,
        send_queue: asyncio.Queue[WsSendCommand],
        db_queue: asyncio.Queue,
    ):
        self.config = config
        self.send_queue = send_queue
        self.db_queue = db_queue
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = asyncio.Event()
        self._running = False
        self._reconnect_count = 0
        self._connect_time: float = 0.0
        self._last_msg_time: float = 0.0
        # request_id -> asyncio.Future (csak query metódusokhoz)
        self._pending: dict[str, asyncio.Future] = {}

    async def writer_loop(self) -> None:
        """Send queue-ból olvas és elküldi WS-en. Auto-reconnect."""
        self._running = True
        delay = RECONNECT_BASE_DELAY

        while self._running:
            try:
                await self._connect()
                delay = RECONNECT_BASE_DELAY
                await self._write_loop()
            except ConnectionClosed as e:
                log.warning("WS API kapcsolat megszakadt", reason=str(e))
            except Exception as e:
                log.error("WS API writer hiba", error=str(e))
            finally:
                self._connected.clear()
                self._ws = None

            if not self._running:
                break

            # Exponenciális backoff + jitter
            jitter = random.uniform(0, delay * 0.3)
            await asyncio.sleep(delay + jitter)
            delay = min(delay * 2, RECONNECT_MAX_DELAY)
            self._reconnect_count += 1
            log.info("WS API újracsatlakozás", attempt=self._reconnect_count)

    async def reader_loop(self) -> None:
        """WS válaszokat fogad, hibákat feldolgozza. A writer_loop-pal párhuzamos."""
        while self._running:
            try:
                await self._connected.wait()
                if self._ws is None:
                    continue
                async for message in self._ws:
                    self._last_msg_time = time.monotonic()
                    await self._handle_response(message)
            except ConnectionClosed:
                pass
            except Exception as e:
                log.error("WS API reader hiba", error=str(e))
            await asyncio.sleep(0.1)

    async def _connect(self) -> None:
        url = self.config.ws_api_url
        log.info("WS API csatlakozás", url=url)
        self._ws = await websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=60,
            max_size=10 * 1024 * 1024,
        )
        self._connect_time = time.monotonic()
        self._connected.set()
        log.info("WS API csatlakozva")

    async def _write_loop(self) -> None:
        while self._running and self._ws is not None:
            # 23 óra után proaktív újracsatlakozás
            if time.monotonic() - self._connect_time > RECONNECT_BEFORE_HOURS * 3600:
                log.info("WS API proaktív újracsatlakozás (24h limit)")
                break

            try:
                cmd = await asyncio.wait_for(self.send_queue.get(), timeout=1.0)
                params = self._prepare_params(cmd)
                message = json.dumps({
                    "id": cmd.request_id,
                    "method": cmd.method,
                    "params": params,
                })
                await self._ws.send(message)
                self.send_queue.task_done()
                log.debug("WS API kérelem küldve", method=cmd.method, id=cmd.request_id)
            except asyncio.TimeoutError:
                continue

    def _prepare_params(self, cmd: WsSendCommand) -> dict:
        """Ha hiteles kérelem, timestamp és signature hozzáadása."""
        if not cmd.is_authenticated:
            return cmd.params

        params = {**cmd.params}
        params["apiKey"] = self.config.api_key
        params["timestamp"] = make_timestamp()
        params["recvWindow"] = self.config.recv_window_ms
        return sign_params(params, self.config.secret_key)

    async def _handle_response(self, message: str) -> None:
        try:
            data = json.loads(message)
        except Exception:
            log.warning("Érvénytelen WS API válasz JSON")
            return

        request_id = data.get("id")
        status = data.get("status")

        # Ha vár rá valaki (query hívás), teljesítjük a future-t
        if request_id and request_id in self._pending:
            future = self._pending.pop(request_id)
            if status == 200:
                future.set_result(data.get("result"))
            else:
                error = data.get("error", {})
                future.set_exception(
                    Exception(f"WS API hiba {status}: {error.get('msg', 'ismeretlen')}")
                )
            return

        # Order ACK / hiba logolás (nem authoritative!)
        if status and status != 200:
            error = data.get("error", {})
            log.warning("WS API order hiba (telemetria)", status=status, error=error, id=request_id)
            from persistence.writer import DbEvent
            self.db_queue.put_nowait(DbEvent(
                type="log_system_event",
                data={
                    "severity": "WARNING",
                    "component": "ws_api",
                    "event_type": "order_submission_error",
                    "message": f"WS API hiba: {error.get('msg', 'ismeretlen')}",
                    "payload": {"request_id": request_id, "error": error, "status": status},
                },
            ))

    async def _query(self, method: str, params: dict, authenticated: bool = True) -> Any:
        """Query hívás – vár a válaszra (csak nem-kereskedési hívásokhoz)."""
        await self._connected.wait()
        request_id = str(uuid.uuid4())
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[request_id] = future

        cmd = WsSendCommand(
            request_id=request_id,
            method=method,
            params=params,
            is_authenticated=authenticated,
        )
        self.send_queue.put_nowait(cmd)

        try:
            return await asyncio.wait_for(future, timeout=10.0)
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            raise TimeoutError(f"WS API timeout: {method}")

    # --- Non-blocking order metódusok ---

    def enqueue_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        price: Decimal,
        quantity: Decimal,
        client_order_id: str,
        time_in_force: str = "GTC",
    ) -> str:
        """Order küldése queue-ba – AZONNAL visszatér, nem vár semmit."""
        request_id = str(uuid.uuid4())
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": str(quantity),
            "price": str(price),
            "newClientOrderId": client_order_id,
            "newOrderRespType": "ACK",
        }
        # LIMIT_MAKER nem fogad timeInForce-t (mindig GTC); csak LIMIT-nél kell
        if order_type == "LIMIT":
            params["timeInForce"] = time_in_force

        cmd = WsSendCommand(
            request_id=request_id,
            method="order.place",
            params=params,
            is_authenticated=True,
        )
        self.send_queue.put_nowait(cmd)
        return request_id

    def enqueue_cancel(self, symbol: str, client_order_id: str) -> str:
        """Megbízás törlése queue-ba – non-blocking."""
        request_id = str(uuid.uuid4())
        cmd = WsSendCommand(
            request_id=request_id,
            method="order.cancel",
            params={"symbol": symbol, "origClientOrderId": client_order_id},
            is_authenticated=True,
        )
        self.send_queue.put_nowait(cmd)
        return request_id

    def enqueue_cancel_all(self, symbol: str) -> str:
        """Összes nyitott megbízás törlése egy szimbólumon – non-blocking."""
        request_id = str(uuid.uuid4())
        cmd = WsSendCommand(
            request_id=request_id,
            method="openOrders.cancelAll",
            params={"symbol": symbol},
            is_authenticated=True,
        )
        self.send_queue.put_nowait(cmd)
        return request_id

    # --- Query metódusok (várakozással) ---

    async def get_listen_key(self) -> str:
        """listenKey lekérése WS API-n keresztül (REST fallback helyett)."""
        result = await self._query_apikey_only("userDataStream.start", {})
        return result["listenKey"]

    async def keepalive_listen_key(self, listen_key: str) -> None:
        """listenKey megújítása WS API-n keresztül."""
        await self._query_apikey_only("userDataStream.ping", {"listenKey": listen_key})

    async def _query_apikey_only(self, method: str, params: dict) -> Any:
        """WS API hívás csak apiKey-jel (timestamp és signature nélkül)."""
        await self._connected.wait()
        request_id = str(uuid.uuid4())
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[request_id] = future
        cmd = WsSendCommand(
            request_id=request_id,
            method=method,
            params={**params, "apiKey": self.config.api_key},
            is_authenticated=False,
        )
        self.send_queue.put_nowait(cmd)
        try:
            return await asyncio.wait_for(future, timeout=10.0)
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            raise TimeoutError(f"WS API timeout: {method}")

    async def get_exchange_info(self, symbol: str) -> dict:
        return await self._query("exchangeInfo", {"symbol": symbol}, authenticated=False)

    async def get_account(self) -> dict:
        return await self._query("account.status", {})

    async def get_open_orders(self, symbol: str) -> list:
        result = await self._query("openOrders.status", {"symbol": symbol})
        return result if isinstance(result, list) else []

    async def get_order(self, symbol: str, client_order_id: str) -> dict:
        return await self._query("order.status", {
            "symbol": symbol,
            "origClientOrderId": client_order_id,
        })

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()

    @property
    def last_msg_age_sec(self) -> float:
        if self._last_msg_time == 0:
            return float("inf")
        return time.monotonic() - self._last_msg_time

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set() and self._ws is not None
