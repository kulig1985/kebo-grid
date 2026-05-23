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
from app.log_setup import get_logger
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
        Ha 2× WS timeout egymás után → REST fallback (mert pont halott a WS).
        """
        ws_timeouts = 0
        for attempt in range(self.config.cancel_retry_max):
            await asyncio.sleep(self.config.cancel_retry_interval_sec)

            try:
                open_orders = await self.ws_api.get_open_orders(symbol)
                ws_timeouts = 0  # sikeres → reset
                if not open_orders:
                    log.info("Minden order törölve (WS)")
                    if self.config.sell_on_emergency_stop:
                        await self._sell_remaining_base(symbol)
                    self._mark_stopped()
                    return
                else:
                    log.warning(
                        "Még vannak nyitott order-ek, cancelAll újraküldés",
                        count=len(open_orders),
                        attempt=attempt + 1,
                    )
                    self.ws_api.enqueue_cancel_all(symbol)
            except Exception as e:
                ws_timeouts += 1
                log.error("openOrders.status hiba vészleállításkor",
                          error=str(e), ws_timeouts=ws_timeouts)
                if ws_timeouts >= 2:
                    log.warning("WS halott emergency közben — REST fallback indul")
                    await self._rest_fallback_cleanup(symbol)
                    return

        # Retry kimerítve, de WS-en próbáltunk — REST fallback
        log.warning("WS retry kimerítve emergency közben — REST fallback")
        await self._rest_fallback_cleanup(symbol)

    def _mark_stopped(self) -> None:
        """Bot állapot EMERGENCY_STOPPED + DB frissítés."""
        self.engine.status = BotStatus.EMERGENCY_STOPPED
        log.info("EMERGENCY_STOPPED")
        if self.engine.bot_run_id:
            self.db_queue.put_nowait(DbEvent(
                type="update_bot_status",
                data={"run_id": self.engine.bot_run_id, "status": BotStatus.EMERGENCY_STOPPED},
            ))

    async def _rest_fallback_cleanup(self, symbol: str) -> None:
        """REST API-n cancelAll + opcionális base sell, ha WS halott."""
        try:
            from exchange.rest_fallback import BinanceRestFallback
        except ImportError:
            log.error("REST fallback nem elérhető — VÉSZLEÁLLÍTÁS BEFEJEZETLEN!")
            self._log_incomplete()
            return

        rest = BinanceRestFallback(self.engine.settings.exchange)
        cancel_ok = False
        try:
            cancelled = await rest.cancel_all_open_orders(symbol)
            log.info("REST cancelAll OK emergency közben",
                     count=len(cancelled) if isinstance(cancelled, list) else 1)
            cancel_ok = True
        except Exception as e:
            log.error("REST cancelAll is sikertelen", error=str(e))

        # Ellenőrzés
        if cancel_ok:
            try:
                opens = await rest.get_open_orders(symbol)
                if opens:
                    log.warning("REST cancelAll után még maradt order", count=len(opens))
                else:
                    log.info("REST megerősíti: minden order törölve")
            except Exception as e:
                log.warning("REST get_open_orders hiba (ignorálva)", error=str(e))

        # Base sell ha konfigurálva
        if self.config.sell_on_emergency_stop:
            try:
                from decimal import Decimal
                from exchange.precision import round_down_to_step
                account = await rest.get_account()
                base_asset = self.engine.settings.bot.base_asset
                free_base = Decimal("0")
                for b in account.get("balances", []):
                    if b.get("asset") == base_asset:
                        free_base = Decimal(b.get("free", "0"))
                        break
                if free_base > 0 and self.engine.symbol_info:
                    qty = round_down_to_step(free_base, self.engine.symbol_info.lot_size.step_size)
                    if qty > 0:
                        cid = f"ESELL-{self.engine.bot_run_id or 0}-{int(asyncio.get_event_loop().time())}"[-36:]
                        result = await rest.market_sell(symbol, qty, cid)
                        log.info("REST emergency base sell OK", qty=str(qty),
                                 status=result.get("status"))
            except Exception as e:
                log.error("REST base sell hiba", error=str(e))

        if cancel_ok:
            self._mark_stopped()
        else:
            self._log_incomplete()

    def _log_incomplete(self) -> None:
        log.error("VÉSZLEÁLLÍTÁS BEFEJEZETLEN – kézi beavatkozás szükséges!")
        if self.engine.bot_run_id:
            self.db_queue.put_nowait(DbEvent(
                type="log_system_event",
                data={
                    "severity": "CRITICAL",
                    "component": "emergency_stop",
                    "event_type": "emergency_stop_incomplete",
                    "message": "Vészleállítás nem tudta törölni az összes order-t (WS+REST is bukott)!",
                    "payload": {},
                },
            ))

    async def _sell_remaining_base(self, symbol: str) -> None:
        """Megmaradt base eszköz eladása piaci áron."""
        try:
            account = await self.ws_api.get_account()
            base_asset = self.engine.settings.bot.base_asset
            for b in account.get("balances", []):
                if b["a"] == base_asset:
                    from decimal import Decimal
                    free = Decimal(b["f"])
                    if free <= Decimal("0") or self.engine.symbol_info is None:
                        break
                    from exchange.precision import round_down_to_step
                    qty = round_down_to_step(free, self.engine.symbol_info.lot_size.step_size)
                    if qty <= 0:
                        break
                    cid = f"ESELL-{self.engine.bot_run_id or 0}"
                    self.ws_api.enqueue_market_sell(symbol, qty, cid)
                    log.info("Emergency base sell beküldve", asset=base_asset, qty=str(qty))
                    self.db_queue.put_nowait(DbEvent(
                        type="log_system_event",
                        data={
                            "severity": "WARNING",
                            "component": "emergency_stop",
                            "event_type": "emergency_base_sell",
                            "message": f"Base likvidálás: {qty} {base_asset} piaci áron",
                            "payload": {"qty": str(qty), "asset": base_asset},
                        },
                    ))
                    break
        except Exception as e:
            log.error("Emergency base sell hiba", error=str(e))
