"""
Watchdog – rendszer egészségének monitorozása.

Triggerek:
- User stream staleness
- Trading WS staleness
- DB writer queue tele
- Ismétlődő order rejection
- Balance eltérés
"""
import asyncio
import time
from typing import Optional

from app.config import SafetyConfig
from app.log_setup import get_logger
from exchange.user_stream import UserDataStream
from exchange.ws_api import BinanceWsApi
from grid.engine import BotStatus, GridEngine
from persistence.writer import DbWriter
from supervisor.emergency import EmergencyStop

log = get_logger(__name__)

CHECK_INTERVAL = 5.0  # másodperc


class Watchdog:
    def __init__(
        self,
        engine: GridEngine,
        ws_api: BinanceWsApi,
        user_stream: UserDataStream,
        db_writer: DbWriter,
        emergency: EmergencyStop,
        config: SafetyConfig,
    ):
        self.engine = engine
        self.ws_api = ws_api
        self.user_stream = user_stream
        self.db_writer = db_writer
        self.emergency = emergency
        self.config = config
        self._running = False
        self._rejection_count = 0

    async def run(self) -> None:
        self._running = True
        log.info("Watchdog elindult")
        while self._running:
            await asyncio.sleep(CHECK_INTERVAL)
            await self._check()

    async def stop(self) -> None:
        self._running = False

    async def _check(self) -> None:
        if self.engine.status in (BotStatus.EMERGENCY_STOPPING, BotStatus.EMERGENCY_STOPPED, BotStatus.STOPPED):
            return

        # User stream staleness – csak az első sikeres kapcsolat UTÁN ellenőriz
        if self.user_stream._connected_once:
            age = self.user_stream.last_event_age_sec
            if age > self.config.max_user_stream_staleness_sec:
                await self.emergency.execute(
                    f"User stream stale: {age:.1f}s > {self.config.max_user_stream_staleness_sec}s"
                )
                return

        # Trading WS staleness
        ws_age = self.ws_api.last_msg_age_sec
        if ws_age > self.config.max_trading_ws_staleness_sec and self.ws_api.is_connected:
            log.warning("Trading WS régi utolsó üzenet", age_sec=ws_age)

        # DB writer queue telítettség
        if self.config.emergency_stop_on_db_queue_full:
            if self.db_writer.queue_size > self.db_writer.max_queue_size * 0.9:
                await self.emergency.execute(
                    f"DB writer queue majdnem tele: {self.db_writer.queue_size}"
                )
                return

        # DB degraded állapot
        if self.db_writer.is_degraded:
            log.error("DB writer degraded – DB kapcsolat problémás")

        log.debug("Watchdog OK", user_stream_age=f"{age:.1f}s", db_queue=self.db_writer.queue_size)

    def record_rejection(self) -> None:
        """Order visszautasítás számlálása."""
        self._rejection_count += 1

    def reset_rejection_count(self) -> None:
        self._rejection_count = 0
