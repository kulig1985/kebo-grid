"""
Reconciliation – állapot-javítás WS API-n keresztül.

Fut:
- Startup-kor
- User stream reconnect után
- Periodikusan (konfig szerint)

SOHA nem blokkolja az order küldési útvonalat.
Saját task-ban fut, az event dispatcher-en keresztül kommunikál.
"""
import asyncio
from decimal import Decimal
from typing import Optional

from app.config import SafetyConfig
from app.logging import get_logger
from exchange.models import SymbolInfo
from exchange.ws_api import BinanceWsApi
from grid.engine import GridEngine
from grid.inventory import InventoryManager
from persistence.writer import DbEvent

log = get_logger(__name__)


class Reconciliation:
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
        self._running = False
        self._reconciling = asyncio.Lock()

    async def run(self) -> None:
        """Periodikus reconciliation task."""
        self._running = True
        log.info("Reconciliation task elindult")
        while self._running:
            await asyncio.sleep(self.config.reconciliation_interval_sec)
            await self.reconcile("periodic")

    async def stop(self) -> None:
        self._running = False

    async def reconcile(self, reason: str = "manual") -> None:
        """
        Állapot szinkronizálás a Binance-szel.
        WS API-t használ, soha nem REST-et.
        """
        if self._reconciling.locked():
            log.debug("Reconciliation már fut, kihagyás")
            return

        async with self._reconciling:
            log.info("Reconciliation indítás", reason=reason)
            try:
                await self._do_reconcile()
            except Exception as e:
                log.error("Reconciliation hiba", error=str(e))

    async def _do_reconcile(self) -> None:
        symbol = self.engine.settings.bot.symbol

        # 1. Nyitott order-ek lekérése WS-en
        try:
            open_orders = await self.ws_api.get_open_orders(symbol)
        except Exception as e:
            log.error("openOrders.status lekérés sikertelen", error=str(e))
            return

        exchange_order_ids = {o["orderId"] for o in open_orders}
        exchange_client_ids = {o.get("clientOrderId", "") for o in open_orders}

        log.info("Exchange nyitott order-ek", count=len(open_orders))

        # 2. Account egyenleg szinkronizálás
        try:
            account = await self.ws_api.get_account()
            self.engine.inventory.update_from_account(account.get("balances", []))

            if self.engine.bot_run_id:
                for b in account.get("balances", []):
                    self.db_queue.put_nowait(DbEvent(
                        type="update_balance",
                        data={
                            "bot_run_id": self.engine.bot_run_id,
                            "asset": b["asset"],
                            "free": Decimal(str(b.get("free", "0"))),
                            "locked": Decimal(str(b.get("locked", "0"))),
                            "source": "reconciliation",
                        },
                    ))
        except Exception as e:
            log.error("Account lekérés sikertelen reconciliation-kor", error=str(e))

        # 3. Naplózás
        self.db_queue.put_nowait(DbEvent(
            type="log_system_event",
            data={
                "severity": "INFO",
                "component": "reconciliation",
                "event_type": "reconciliation_complete",
                "message": f"Reconciliation kész: {len(open_orders)} nyitott order",
                "payload": {"open_order_count": len(open_orders)},
            },
        ))

        log.info("Reconciliation kész", open_orders=len(open_orders))
