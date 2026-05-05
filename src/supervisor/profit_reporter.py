"""Periodikus profit riport — half-match engine + portfolio-mark-to-market."""
import asyncio
import time
from decimal import Decimal

from sqlalchemy import select

from app.log_setup import get_logger
from grid.pnl import HalfMatch, compute_half_match_profit
from persistence.db import get_session
from persistence.models import Fill

log = get_logger(__name__)


class ProfitReporter:
    def __init__(self, engine, interval_sec: int = 600):
        self.engine = engine
        self.interval_sec = interval_sec
        self._start_time = time.monotonic()
        self._running = True

    async def run(self) -> None:
        while self._running:
            await asyncio.sleep(self.interval_sec)
            if not self._running:
                break
            try:
                await self._report()
            except Exception as e:
                log.error("ProfitReporter hiba", error=str(e))

    async def stop(self) -> None:
        self._running = False

    async def _report(self) -> None:
        if not self.engine.bot_run_id:
            return

        async with get_session() as session:
            result = await session.execute(
                select(Fill)
                .where(Fill.bot_run_id == self.engine.bot_run_id)
                .order_by(Fill.transaction_time)
            )
            fills_db = result.scalars().all()

        if not fills_db:
            return

        half_matches = [HalfMatch(
            fill_id=f.id,
            side=f.side,
            price=f.price,
            quantity=f.quantity,
            quote_qty=f.quote_quantity,
            commission=f.commission_amount,
            commission_asset=f.commission_asset or "",
            remaining=f.quantity,
        ) for f in fills_db]

        current_price = Decimal("0")
        if self.engine.market_stream and self.engine.market_stream.mid_price:
            current_price = self.engine.market_stream.mid_price

        bot = self.engine.settings.bot
        snapshot = self.engine.inventory.snapshot()
        wallet_base = snapshot.get(bot.base_asset, Decimal("0"))
        wallet_quote = snapshot.get(bot.quote_asset, Decimal("0"))
        initial_capital = bot.total_capital_quote

        report = compute_half_match_profit(
            half_matches,
            current_price=current_price,
            initial_capital_quote=initial_capital,
            wallet_base=wallet_base,
            wallet_quote=wallet_quote,
        )

        elapsed_h = (time.monotonic() - self._start_time) / 3600
        per_hour = report.grid_profit / Decimal(str(elapsed_h)) if elapsed_h > 0.01 else Decimal("0")

        log.info(
            "PROFIT",
            realized=f"{report.grid_profit:.4f}",
            matches=report.completed_matches,
            per_hour=f"{per_hour:.4f}",
            portfolio=f"{report.portfolio_quote:.2f}",
            total_pnl=f"{report.total_pnl:.4f}",
            open_base=wallet_base.normalize(),
            open_quote=f"{wallet_quote:.2f}",
            hours=f"{elapsed_h:.1f}",
        )
