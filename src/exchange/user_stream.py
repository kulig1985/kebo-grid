"""
Binance User Data Stream kliens.

Felelős:
- listenKey kezelés (REST, ez az egyetlen REST hívás a rendszerben)
- executionReport, outboundAccountPosition, balanceUpdate eventi olvasás
- Automatikus újracsatlakozás és RECONCILING állapot jelzés
"""
import asyncio
import json
import random
import time
from typing import Callable, Optional

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

from app.config import ExchangeConfig
from app.log_setup import get_logger

log = get_logger(__name__)

KEEPALIVE_INTERVAL = 1800  # 30 perc
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0


class UserDataStream:
    """
    Binance User Data Stream olvasó.

    Minden bejövő esemény azonnal az event_queue-ba kerül.
    A WebSocket olvasó loop-ban NINCS DB írás.
    """

    def __init__(
        self,
        config: ExchangeConfig,
        event_queue: asyncio.Queue,
        on_reconnect: Optional[Callable] = None,
    ):
        self.config = config
        self.event_queue = event_queue
        self.on_reconnect = on_reconnect  # hívható, ha reconnect történik
        self._listen_key: Optional[str] = None
        self._running = False
        self._reconnect_count = 0
        self._last_event_time = 0.0
        self._http_session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        """Fő reader loop elindítása."""
        self._running = True
        self._http_session = aiohttp.ClientSession()

        try:
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
        finally:
            if self._http_session:
                await self._http_session.close()

    async def stop(self) -> None:
        self._running = False

    async def _obtain_listen_key(self) -> None:
        """listenKey lekérése REST-en (egyetlen REST hívás a rendszerben)."""
        url = f"{self.config.rest_url}/api/v3/userDataStream"
        headers = {"X-MBX-APIKEY": self.config.api_key}
        async with self._http_session.post(url, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()
            self._listen_key = data["listenKey"]
            log.info("listenKey megszerzve")

    async def _keepalive_loop(self) -> None:
        """30 percenként megújítja a listenKey-t."""
        while self._running and self._listen_key:
            await asyncio.sleep(KEEPALIVE_INTERVAL)
            try:
                url = f"{self.config.rest_url}/api/v3/userDataStream"
                headers = {"X-MBX-APIKEY": self.config.api_key}
                params = {"listenKey": self._listen_key}
                async with self._http_session.put(url, headers=headers, params=params) as resp:
                    resp.raise_for_status()
                    log.info("listenKey megújítva")
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

    @property
    def is_stale(self, max_staleness_sec: float = 10.0) -> bool:
        return self.last_event_age_sec > max_staleness_sec
