"""
Binance User Data Stream kliens.

listenKey kezelés KIZÁRÓLAG WS API-n keresztül (userDataStream.start / ping).
NINCS REST hívás ebben a modulban.

Felelős:
- executionReport, outboundAccountPosition, balanceUpdate esemény olvasás
- Automatikus újracsatlakozás
"""
import asyncio
import json
import random
import time
from typing import Callable, Optional

import websockets

from app.config import ExchangeConfig
from app.log_setup import get_logger

log = get_logger(__name__)

KEEPALIVE_INTERVAL = 1800  # 30 perc
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0


class UserDataStream:
    """
    Binance User Data Stream olvasó.

    listenKey kizárólag WS API-n keresztül (ws_api.get_listen_key()).
    NINCS REST hívás.
    """

    def __init__(
        self,
        config: ExchangeConfig,
        event_queue: asyncio.Queue,
        on_reconnect: Optional[Callable] = None,
        ws_api=None,  # BinanceWsApi – kötelező a listenKey WS API-hoz
    ):
        self.config = config
        self.event_queue = event_queue
        self.on_reconnect = on_reconnect
        self.ws_api = ws_api
        self._listen_key: Optional[str] = None
        self._running = False
        self._reconnect_count = 0
        self._last_event_time = 0.0
        self._connected_once = False

    async def start(self) -> None:
        """Fő reader loop elindítása."""
        self._running = True

        delay = RECONNECT_BASE_DELAY
        while self._running:
            try:
                await self._obtain_listen_key()
                await asyncio.gather(
                    self._reader_loop(),
                    self._keepalive_loop(),
                    return_exceptions=True,
                )
            except Exception as e:
                log.error("User stream hiba", error=str(e))

            if not self._running:
                break

            jitter = random.uniform(0, delay * 0.3)
            await asyncio.sleep(delay + jitter)
            delay = min(delay * 2, RECONNECT_MAX_DELAY)
            self._reconnect_count += 1
            log.info("User stream újracsatlakozás", attempt=self._reconnect_count)

            if self.on_reconnect:
                asyncio.get_event_loop().call_soon(self.on_reconnect)

    async def stop(self) -> None:
        self._running = False

    async def _obtain_listen_key(self) -> None:
        """listenKey lekérése WS API-n (userDataStream.start) – NEM REST."""
        if self.ws_api is None:
            raise RuntimeError("ws_api kötelező a listenKey megszerzéséhez!")
        self._listen_key = await self.ws_api.get_listen_key()
        log.info("listenKey megszerzve (WS API)")

    async def _keepalive_loop(self) -> None:
        """30 percenként megújítja a listenKey-t WS API-n (NEM REST)."""
        while self._running and self._listen_key:
            await asyncio.sleep(KEEPALIVE_INTERVAL)
            try:
                await self.ws_api.keepalive_listen_key(self._listen_key)
                log.info("listenKey megújítva (WS API)")
            except Exception as e:
                log.error("listenKey megújítás sikertelen", error=str(e))

    async def _reader_loop(self) -> None:
        """WebSocket stream olvasó – azonnal queue-ba tesz, nem ír DB-be."""
        ws_url = f"{self.config.user_stream_url}/{self._listen_key}"
        log.info("User stream csatlakozás", url=ws_url)

        async with websockets.connect(
            ws_url,
            ping_interval=20,
            ping_timeout=60,
        ) as ws:
            log.info("User stream csatlakozva")
            self._connected_once = True
            self._last_event_time = time.monotonic()  # grace period: 10s az első eseményig
            async for message in ws:
                self._last_event_time = time.monotonic()
                try:
                    data = json.loads(message)
                    event_type = data.get("e")

                    if event_type in ("executionReport", "outboundAccountPosition", "balanceUpdate"):
                        self.event_queue.put_nowait(data)
                    elif event_type == "listenKeyExpired":
                        log.warning("listenKey lejárt, újracsatlakozás szükséges")
                        return
                    else:
                        log.debug("Ismeretlen user stream esemény", event_type=event_type)
                except Exception as e:
                    log.error("User stream esemény feldolgozási hiba", error=str(e))

    @property
    def last_event_age_sec(self) -> float:
        if self._last_event_time == 0:
            return float("inf")
        return time.monotonic() - self._last_event_time
