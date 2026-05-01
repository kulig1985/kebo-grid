"""Aszinkron DB writer – queue-ból olvassa az eseményeket és menti az adatbázisba."""
import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional
from app.logging import get_logger
from .db import get_session
from .repositories import (
    BotRunRepo, OrderRepo, FillRepo, ExecutionEventRepo,
    BalanceRepo, SystemEventRepo, ExternalEventRepo,
)

log = get_logger(__name__)


@dataclass
class DbEvent:
    type: str
    data: dict[str, Any]


class DbWriter:
    """
    Queue-alapú DB writer task.
    A WebSocket olvasó loop-ok közvetlenül queue-ba tesznek,
    soha nem írnak egyenesen az adatbázisba.
    """

    def __init__(self, queue: asyncio.Queue[DbEvent], max_queue_size: int = 10000):
        self.queue = queue
        self.max_queue_size = max_queue_size
        self._running = False
        self._degraded = False

    @property
    def is_degraded(self) -> bool:
        return self._degraded

    @property
    def queue_size(self) -> int:
        return self.queue.qsize()

    async def run(self) -> None:
        self._running = True
        log.info("DbWriter elindult")
        while self._running:
            try:
                event = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                await self._handle(event)
                self.queue.task_done()
                self._degraded = False
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.error("DbWriter hiba", error=str(e))
                self._degraded = True

    async def stop(self) -> None:
        self._running = False

    async def _handle(self, event: DbEvent) -> None:
        try:
            async with get_session() as session:
                match event.type:
                    case "upsert_order_from_intent":
                        from .models import OrderIntent
                        repo = OrderRepo(session)
                        # Az intent már mentve volt, csak az ordert hozzuk létre
                        intent_data = event.data
                        # Közvetlenül az Order táblába írunk
                        from sqlalchemy.dialects.postgresql import insert
                        from .models import Order
                        stmt = insert(Order).values(**intent_data).on_conflict_do_nothing(
                            index_elements=["client_order_id"]
                        )
                        await session.execute(stmt)

                    case "update_order_from_execution":
                        repo = OrderRepo(session)
                        await repo.update_from_execution_report(**event.data)

                    case "insert_fill":
                        repo = FillRepo(session)
                        await repo.upsert(event.data)

                    case "insert_execution_event":
                        repo = ExecutionEventRepo(session)
                        await repo.insert_idempotent(event.data)

                    case "update_balance":
                        repo = BalanceRepo(session)
                        await repo.upsert_asset(**event.data)

                    case "update_bot_status":
                        repo = BotRunRepo(session)
                        await repo.update_status(event.data["run_id"], event.data["status"])

                    case "log_system_event":
                        repo = SystemEventRepo(session)
                        await repo.log(**event.data)

                    case "log_external_event":
                        repo = ExternalEventRepo(session)
                        await repo.log(event.data)

                    case "update_intent_state":
                        from sqlalchemy import update as sa_update
                        from .models import OrderIntent
                        await session.execute(
                            sa_update(OrderIntent)
                            .where(OrderIntent.client_order_id == event.data["client_order_id"])
                            .values(local_state=event.data["state"])
                        )

                    case _:
                        log.warning("Ismeretlen DbEvent típus", event_type=event.type)

        except Exception as e:
            log.error("DB írási hiba", event_type=event.type, error=str(e))
            raise
