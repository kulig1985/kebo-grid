"""
Vészleállítás (Emergency Stop) logika.

Lépések:
1. Bot állapot -> EMERGENCY_STOPPING
2. Grid motor megállítása
3. openOrders.cancelAll WS-en (non-blocking)
4. Helyi WORKING order-ek -> CANCEL_QUEUED
5. Várakozás user stream CANCELED eseményekre
6. Ha timeout -> openOrders.status ellenőrzés
7. Állapot -> EMERGENCY_STOPPED
"""
import asyncio
from typing import Optional

from app.config import SafetyConfig
from app.logging import get_logger
from exchange.ws_api import BinanceWsApi
from grid.engine import BotStatus, GridEngine
from persistence.writer import DbEvent

log = get_logger(__name__)


class EmergencyStop:
    def __init__(
        self,
        engine: GridEngine,
        ws_api: BinanceWsApi,
        db_queue: asyncio.Queue[DbEvent],
        config: SafetyConfig,
    ):
        self.engine = engine
        self.ws_api = ws_api
        self.db_queue = db_queue
        self.config = config
        self._in_progress = False

    async def execute(self, reason: str = "API kérelem") -> None:
        """Vészleállítás végrehajtása."""
        if self._in_progress:
            log.warning("Vészleállítás már folyamatban")
            return

        self._in_progress = True
        log.critical("VÉSZLEÁLLÍTÁS INDÍTVA", reason=reason)

        # 1. Bot állapot beállítása
        self.engine.status = BotStatus.EMERGENCY_STOPPING

        # 2. Grid motor megállítása (nem generál új order-t)
        await self.engine.emergency_stop()

        # 3. cancelAll küldése WS-en (non-blocking)
        symbol = self.engine.settings.bot.symbol
        self.ws_api.enqueue_cancel_all(symbol)

        # 4. DB naplózás
        if self.engine.bot_run_id:
            self.db_queue.put_nowait(DbEvent(
                type="log_system_event",
                data={
                    "severity": "CRITICAL",
                    "component": "emergency_stop",
                    "event_type": "emergency_stop_initiated",
                    "message": f"Vészleállítás: {reason}",
                    "payload": {"reason": reason, "symbol": symbol},
                },
            ))
            self.db_queue.put_nowait(DbEvent(
                type="update_bot_status",
                data={"run_id": self.engine.bot_run_id, "status": BotStatus.EMERGENCY_STOPPING},
            ))

        # 5. Várakozás a CANCELED eseményekre (async, timeout-tal)
        asyncio.get_event_loop().create_task(
            self._wait_for_canceled(symbol)
        )

        log.info("Vészleállítás parancs bekülve, API visszatér")

    async def _wait_for_canceled(self, symbol: str) -> None:
        """
        Várakozás az összes order törlésére.
        Ha timeout → openOrders.status ellenőrzés és retry.
        """
        total_wait = (
            self.config.cancel_retry_interval_sec * self.config.cancel_retry_max
        )

        for attempt in range(self.config.cancel_retry_max):
            await asyncio.sleep(self.config.cancel_retry_interval_sec)

            try:
                open_orders = await self.ws_api.get_open_orders(symbol)
                if not open_orders:
                    log.info("Minden order törölve, EMERGENCY_STOPPED")
                    self.engine.status = BotStatus.EMERGENCY_STOPPED
                    if self.engine.bot_run_id:
                        self.db_queue.put_nowait(DbEvent(
                            type="update_bot_status",
                            data={"run_id": self.engine.bot_run_id, "status": BotStatus.EMERGENCY_STOPPED},
                        ))
                    return
                else:
                    log.warning(
                        "Még vannak nyitott order-ek, cancelAll újraküldés",
                        count=len(open_orders),
                        attempt=attempt + 1,
                    )
                    self.ws_api.enqueue_cancel_all(symbol)
            except Exception as e:
                log.error("openOrders.status hiba vészleállításkor", error=str(e))

        # Retry kimerítve
        log.error("VÉSZLEÁLLÍTÁS BEFEJEZETLEN – kézi beavatkozás szükséges!")
        if self.engine.bot_run_id:
            self.db_queue.put_nowait(DbEvent(
                type="log_system_event",
                data={
                    "severity": "CRITICAL",
                    "component": "emergency_stop",
                    "event_type": "emergency_stop_incomplete",
                    "message": "Vészleállítás nem tudta törölni az összes order-t!",
                    "payload": {},
                },
            ))
