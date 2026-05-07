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

from sqlalchemy import select

from app.config import SafetyConfig
from app.log_setup import get_logger
from exchange.models import SymbolInfo
from exchange.signing import make_timestamp
from exchange.ws_api import BinanceWsApi
from grid.engine import GridEngine, MissedLevel
from grid.inventory import InventoryManager
from persistence.db import get_session
from persistence.models import Order
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

        # 1.b GHOST CID detection — memóriában van, exchange-en nincs
        await self._detect_ghost_cids(exchange_client_ids)

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

    async def _detect_ghost_cids(self, exchange_client_ids: set[str]) -> None:
        """Memóriában van CID, exchange-en nincs → DB lookup, MISSED-re tisztítás."""
        ghosts: list[tuple[int, str]] = []
        for level_index, mem_cid in list(self.engine._level_orders.items()):
            if mem_cid not in exchange_client_ids:
                ghosts.append((level_index, mem_cid))

        if not ghosts:
            return

        # DB lookup batch
        cids = [g[1] for g in ghosts]
        try:
            async with get_session() as session:
                result = await session.execute(
                    select(Order.client_order_id, Order.status_local, Order.side, Order.price, Order.original_quantity, Order.pair_id)
                    .where(Order.client_order_id.in_(cids))
                )
                db_rows = {r[0]: r for r in result.all()}
        except Exception as e:
            log.error("Ghost CID DB lookup hiba", error=str(e))
            return

        for level_index, mem_cid in ghosts:
            row = db_rows.get(mem_cid)
            if row is None:
                # DB-ben sincs még → még async insert in-flight, várj
                continue
            _, status_local, side, price, original_qty, pair_id = row

            if status_local in ("REJECTED", "EXPIRED"):
                # MISSED-re téve (ha még nincs)
                grid_line = self.engine.grid_map.get(level_index)
                if grid_line and level_index not in self.engine._missed_levels:
                    buy_quote = self.engine._buy_fill_quote_by_pair.get(pair_id) if pair_id else None
                    self.engine._missed_levels[level_index] = MissedLevel(
                        level_index=level_index,
                        side=side,
                        target_price=grid_line.price,
                        target_qty=grid_line.quantity if grid_line.quantity > 0 else original_qty,
                        original_pair_id=pair_id,
                        buy_fill_quote=buy_quote,
                        retry_count=0,
                        last_attempt_ms=make_timestamp(),
                    )
                self.engine._level_orders.pop(level_index, None)
                log.warning("Reconciliation: ghost CID MISSED-re téve",
                            cid=mem_cid, lvl=level_index, db_state=status_local)
            elif status_local == "FILLED":
                log.error("Reconciliation: elveszett FILL esemény",
                          cid=mem_cid, lvl=level_index)
            elif status_local in ("CANCELED", "EXTERNAL_CANCELED"):
                self.engine._level_orders.pop(level_index, None)
                log.warning("Reconciliation: cancelt ghost CID tisztítva",
                            cid=mem_cid, lvl=level_index, db_state=status_local)
            else:
                log.debug("Ghost CID egyéb státuszban — várjunk",
                          cid=mem_cid, lvl=level_index, db_state=status_local)
