"""Adatelérési réteg – idempotens CRUD műveletek."""
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from .models import (
    BotRun, GridLevel, OrderIntent, Order, Fill,
    ExecutionEvent, Balance, ExternalEvent, SystemEvent, WsConnection,
)


class BotRunRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, data: dict) -> BotRun:
        run = BotRun(**data)
        self.session.add(run)
        await self.session.flush()
        return run

    async def get(self, run_id: int) -> Optional[BotRun]:
        return await self.session.get(BotRun, run_id)

    async def update_status(self, run_id: int, status: str) -> None:
        await self.session.execute(
            update(BotRun)
            .where(BotRun.id == run_id)
            .values(status=status, updated_at=datetime.now(timezone.utc))
        )

    async def list_all(self) -> list[BotRun]:
        result = await self.session.execute(select(BotRun).order_by(BotRun.id.desc()))
        return list(result.scalars().all())


class OrderRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def upsert_from_intent(self, intent: OrderIntent) -> None:
        """OrderIntent alapján létrehozza vagy frissíti az ordert."""
        stmt = insert(Order).values(
            bot_run_id=intent.bot_run_id,
            client_order_id=intent.client_order_id,
            symbol=intent.symbol,
            side=intent.side,
            order_type=intent.order_type,
            time_in_force=intent.time_in_force,
            price=intent.price,
            original_quantity=intent.quantity,
            executed_quantity=Decimal("0"),
            cumulative_quote_quantity=Decimal("0"),
            status_local=intent.local_state,
            grid_level_index=intent.grid_level_index,
            pair_id=intent.pair_id,
            cycle_id=intent.cycle_id,
        ).on_conflict_do_nothing(index_elements=["client_order_id"])
        await self.session.execute(stmt)

    async def update_from_execution_report(
        self,
        client_order_id: str,
        exchange_order_id: int,
        status_exchange: str,
        status_local: str,
        executed_qty: Decimal,
        cumulative_quote_qty: Decimal,
        is_working: bool,
        reject_reason: Optional[str],
        last_event_time: int,
        created_exchange_time: Optional[int] = None,
    ) -> None:
        values: dict = {
            "exchange_order_id": exchange_order_id,
            "status_exchange": status_exchange,
            "status_local": status_local,
            "executed_quantity": executed_qty,
            "cumulative_quote_quantity": cumulative_quote_qty,
            "is_working": is_working,
            "last_event_time": last_event_time,
            "updated_at": datetime.now(timezone.utc),
        }
        if reject_reason:
            values["reject_reason"] = reject_reason
        if created_exchange_time:
            values["created_exchange_time"] = created_exchange_time

        await self.session.execute(
            update(Order)
            .where(Order.client_order_id == client_order_id)
            .values(**values)
        )

    async def get_by_client_id(self, client_order_id: str) -> Optional[Order]:
        result = await self.session.execute(
            select(Order).where(Order.client_order_id == client_order_id)
        )
        return result.scalar_one_or_none()

    async def list_working(self, bot_run_id: int) -> list[Order]:
        result = await self.session.execute(
            select(Order).where(
                Order.bot_run_id == bot_run_id,
                Order.is_working == True,
            )
        )
        return list(result.scalars().all())


class FillRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def upsert(self, data: dict) -> None:
        """Idempotens fill mentés – dupla event esetén nem dupláz."""
        stmt = insert(Fill).values(**data).on_conflict_do_nothing(
            constraint="uq_fill"
        )
        await self.session.execute(stmt)


class ExecutionEventRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def insert_idempotent(self, data: dict) -> None:
        """Idempotens executionReport esemény mentés execution_id alapján."""
        stmt = insert(ExecutionEvent).values(**data)
        if data.get("execution_id") is not None:
            stmt = stmt.on_conflict_do_nothing(index_elements=["execution_id"])
        await self.session.execute(stmt)


class BalanceRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def upsert_asset(self, bot_run_id: int, asset: str, free: Decimal, locked: Decimal, source: str, event_time: Optional[int] = None) -> None:
        balance = Balance(
            bot_run_id=bot_run_id,
            asset=asset,
            free=free,
            locked=locked,
            source=source,
            event_time=event_time,
        )
        self.session.add(balance)


class SystemEventRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def log(self, severity: str, component: str, event_type: str, message: str, payload: Optional[dict] = None) -> None:
        event = SystemEvent(
            severity=severity,
            component=component,
            event_type=event_type,
            message=message,
            payload_json=payload,
        )
        self.session.add(event)


class ExternalEventRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def log(self, data: dict) -> None:
        event = ExternalEvent(**data)
        self.session.add(event)
