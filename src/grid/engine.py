"""
Grid Motor – a rendszer üzleti logikájának szíve.

Felelős:
- Inicializáció (exchangeInfo, account, anchor, grid generálás)
- fill eseményekre reagálás (counter order küldés)
- Külső beavatkozás kezelése
- Teljesen non-blocking
"""
import asyncio
import time
from decimal import Decimal
from typing import Optional

from app.config import Settings
from app.logging import get_logger
from exchange.market_stream import MarketStream
from exchange.models import ExecutionReport, SymbolInfo
from exchange.ws_api import BinanceWsApi
from grid.calculator import GridCalculator, GridLevel, GridPlan
from grid.inventory import InventoryManager
from grid.order_router import OrderIntent, OrderRouter, make_client_order_id
from grid.pnl import CyclePnl, PnlTracker
from grid.state_machine import LocalOrderState
from persistence.writer import DbEvent

log = get_logger(__name__)


class BotStatus(str):
    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    PAUSED_EXTERNAL_INTERVENTION = "PAUSED_EXTERNAL_INTERVENTION"
    EMERGENCY_STOPPING = "EMERGENCY_STOPPING"
    EMERGENCY_STOPPED = "EMERGENCY_STOPPED"
    STOPPED = "STOPPED"
    ERROR = "ERROR"


class GridEngine:
    """
    Grid kereskedési motor.

    Minden művelet non-blocking: OrderRouter-en keresztül küldi a megbízásokat,
    és queue-n keresztül kommunikál a többi komponenssel.
    """

    def __init__(
        self,
        settings: Settings,
        ws_api: BinanceWsApi,
        db_queue: asyncio.Queue[DbEvent],
        event_queue: asyncio.Queue,
        market_stream: Optional[MarketStream] = None,
    ):
        self.settings = settings
        self.ws_api = ws_api
        self.db_queue = db_queue
        self.event_queue = event_queue
        self.market_stream = market_stream

        self.bot_run_id: Optional[int] = None
        self._bot_run_short_id: str = ""
        self.status: str = BotStatus.INITIALIZING
        self.symbol_info: Optional[SymbolInfo] = None
        self.grid_plan: Optional[GridPlan] = None
        self.inventory = InventoryManager(settings.bot)
        self.pnl = PnlTracker()
        self.router: Optional[OrderRouter] = None
        self.calculator = GridCalculator()

        # Szint -> aktív order ID tracking
        self._level_orders: dict[int, str] = {}  # level_index -> client_order_id
        self._order_levels: dict[str, int] = {}  # client_order_id -> level_index
        self._order_sides: dict[str, str] = {}   # client_order_id -> BUY|SELL
        self._filled_buys: dict[str, ExecutionReport] = {}  # cid -> report

        self._cycle_seq = 0
        self._order_seq = 0
        self._stop_event = asyncio.Event()

    async def initialize(self, bot_run_id: int, bot_run_short_id: str) -> None:
        """
        Startup szekvencia:
        1. exchangeInfo lekérés
        2. Account lekérés + inventory
        3. Anchor price meghatározás
        4. Grid generálás és validálás
        5. Kezdeti order-ek elküldése (non-blocking)
        """
        self.bot_run_id = bot_run_id
        self._bot_run_short_id = bot_run_short_id
        self.router = OrderRouter(self.ws_api, self.db_queue, bot_run_id, self.settings.bot)

        log.info("Grid engine inicializálás", bot_run_id=bot_run_id)

        # 1. exchangeInfo
        symbol = self.settings.bot.symbol
        info_data = await self.ws_api.get_exchange_info(symbol)
        symbols = info_data.get("symbols", [])
        sym_data = next((s for s in symbols if s["symbol"] == symbol), None)
        if sym_data is None:
            raise ValueError(f"Symbol {symbol} nem található az exchangeInfo-ban")
        self.symbol_info = SymbolInfo.from_exchange_info(sym_data)
        log.info("exchangeInfo betöltve", symbol=symbol)

        # 2. Account + inventory
        account_data = await self.ws_api.get_account()
        self.inventory.update_from_account(account_data.get("balances", []))
        log.info("Account betöltve", balances=self.inventory.snapshot())

        # 3. Anchor price
        anchor_price = await self._determine_anchor_price()
        log.info("Anchor price", price=str(anchor_price))

        # 4. Grid kalkuláció
        bot = self.settings.bot
        fees = self.settings.fees
        fb = fees.effective_buy_fee
        fs = fees.effective_sell_fee

        if bot.grid_type == "geometric":
            step = self.calculator.compute_geometric_step(
                bot.order_quote_value,
                bot.target_net_profit_per_cycle_quote,
                fb, fs,
                bot.min_grid_step_pct,
                bot.max_grid_step_pct,
            )
            k_buy, k_sell = self.calculator.compute_grid_counts(
                bot.total_capital_quote,
                bot.order_quote_value,
                bot.quote_reserve_pct,
                bot.buy_allocation_ratio,
                bot.max_grid_levels,
                bot.buy_side_order_count,
                bot.sell_side_order_count,
            )
            self.grid_plan = self.calculator.generate_geometric_grid(
                anchor_price, step, k_buy, k_sell,
                bot.order_quote_value, self.symbol_info,
            )
        else:
            # Aritmetikai grid
            worst_buy_price = anchor_price  # legmagasabb buy szint közel az anchor-hoz
            step = self.calculator.compute_arithmetic_step(
                bot.order_quote_value,
                bot.target_net_profit_per_cycle_quote,
                fb, fs, worst_buy_price,
                bot.min_grid_step_pct,
                bot.max_grid_step_pct,
            )
            k_buy, k_sell = self.calculator.compute_grid_counts(
                bot.total_capital_quote, bot.order_quote_value,
                bot.quote_reserve_pct, bot.buy_allocation_ratio,
                bot.max_grid_levels, bot.buy_side_order_count, bot.sell_side_order_count,
            )
            self.grid_plan = self.calculator.generate_arithmetic_grid(
                anchor_price, step, k_buy, k_sell,
                bot.order_quote_value, self.symbol_info,
            )

        log.info(
            "Grid generálva",
            type=bot.grid_type,
            k_buy=self.grid_plan.k_buy,
            k_sell=self.grid_plan.k_sell,
            quote_needed=str(self.grid_plan.total_quote_required),
            base_needed=str(self.grid_plan.total_base_required),
        )

        # 5. Kezdeti order-ek elküldése (non-blocking)
        self.status = BotStatus.RUNNING
        await self._place_initial_orders()

    async def _determine_anchor_price(self) -> Decimal:
        """
        Anchor price meghatározás konfig szerint.

        - manual: a konfig fájlban megadott ár
        - best_bid_ask_mid: legjobb bid/ask közép (market stream bookTicker)
        - last_trade: utolsó kereskedési ár (market stream bookTicker mid)
        """
        src = self.settings.anchor.source

        if src == "manual":
            if self.settings.anchor.manual_price is None:
                raise ValueError("anchor.manual_price kötelező ha source=manual")
            return self.settings.anchor.manual_price

        if src in ("best_bid_ask_mid", "last_trade"):
            if self.market_stream is None:
                raise ValueError(
                    f"anchor.source='{src}' esetén market_stream szükséges. "
                    "Ellenőrizd a main.py-t."
                )
            price = await self.market_stream.wait_for_price(timeout=15.0)
            log.info("Anchor price live market adatból", source=src, price=str(price))
            return price

        raise ValueError(f"Ismeretlen anchor.source: '{src}'")

    async def _place_initial_orders(self) -> None:
        """
        Kezdeti grid order-ek elküldése – MIND NON-BLOCKING.
        Nem várunk ACK-ra az order-ek között.
        """
        bot = self.settings.bot
        assert self.grid_plan is not None
        assert self.router is not None

        # Buy order-ek
        for level in self.grid_plan.buy_levels:
            if not self.inventory.has_quote_for_buy(level.notional):
                log.warning("Nincs elég quote a buy order-hez", level=level.index, price=str(level.price))
                continue
            self._submit_level_order(level, cycle_id=0)

        # Sell order-ek (csak ha van base inventory)
        if bot.inventory_mode in ("prebalanced", "use_existing_balances"):
            for level in self.grid_plan.sell_levels:
                if not self.inventory.has_base_for_sell(level.quantity):
                    log.warning("Nincs elég base a sell order-hez", level=level.index)
                    break
                self._submit_level_order(level, cycle_id=0)
        elif bot.inventory_mode == "quote_only_bootstrap":
            log.info("quote_only_bootstrap mód – nincs kezdeti sell order")

        log.info("Kezdeti order-ek bekülve (non-blocking)")

    def _submit_level_order(self, level: GridLevel, cycle_id: int) -> str:
        """Egy grid szint order-ét beküldi a routerbe."""
        assert self.router is not None
        self._order_seq += 1
        cid = make_client_order_id(
            self._bot_run_short_id,
            level.side,
            level.index,
            cycle_id,
            self._order_seq % 100,
        )
        intent = OrderIntent(
            bot_run_id=self.bot_run_id,
            client_order_id=cid,
            symbol=self.settings.bot.symbol,
            side=level.side,
            order_type=self.settings.bot.order_type,
            time_in_force=self.settings.bot.time_in_force,
            price=level.price,
            quantity=level.quantity,
            grid_level_index=level.index,
            quote_value_estimate=level.notional,
            cycle_id=str(cycle_id),
        )
        self.router.submit_order(intent)
        self._level_orders[level.index] = cid
        self._order_levels[cid] = level.index
        self._order_sides[cid] = level.side
        return cid

    async def on_execution_report(self, report: ExecutionReport) -> None:
        """
        executionReport esemény feldolgozása.

        Ha fill → counter order küldés.
        Ha külső cancel → policy alapján reakció.
        """
        if self.status == BotStatus.EMERGENCY_STOPPING:
            return  # Emergency stop közben nem generálunk új order-t

        cid = report.client_order_id

        # Csak a saját bot-hoz tartozó order-ek
        if not cid.startswith("G-"):
            return

        if report.execution_type == "TRADE" and report.order_status == "FILLED":
            await self._on_order_filled(report)

    async def _on_order_filled(self, report: ExecutionReport) -> None:
        """Fill esemény: counter order küldése."""
        if self.status != BotStatus.RUNNING:
            log.info("Bot nem fut, counter order kihagyva", status=self.status)
            return

        assert self.grid_plan is not None
        assert self.router is not None

        cid = report.client_order_id
        level_index = self._order_levels.get(cid)
        side = self._order_sides.get(cid)

        if level_index is None or side is None:
            log.warning("Ismeretlen level a fill-hez", cid=cid)
            return

        self._cycle_seq += 1

        if side == "BUY":
            # Buy filled → sell counter order a következő szinten
            next_index = level_index + 1
            sell_level = self._find_sell_level(next_index)
            if sell_level is None:
                log.warning("Nincs sell szint a buy fill-hez", level_index=level_index)
                return

            # Tényleges töltött mennyiség (base commission levonva ha base-ben fizettük)
            qty = report.cumulative_filled_qty
            if report.commission_asset == self.settings.bot.base_asset:
                qty -= report.commission_amount

            sell_level_copy = GridLevel(
                index=sell_level.index,
                price=sell_level.price,
                side="SELL",
                quantity=qty,
                notional=qty * sell_level.price,
                zone=sell_level.zone,
            )
            self._submit_level_order(sell_level_copy, cycle_id=self._cycle_seq)
            log.info("Counter sell order beküldve", level=next_index, qty=str(qty))

        elif side == "SELL":
            # Sell filled → buy counter order az előző szinten
            next_index = level_index - 1
            buy_level = self._find_buy_level(next_index)
            if buy_level is None:
                log.warning("Nincs buy szint a sell fill-hez", level_index=level_index)
                return

            self._submit_level_order(buy_level, cycle_id=self._cycle_seq)
            log.info("Counter buy order beküldve", level=next_index)

    def _find_sell_level(self, index: int) -> Optional[GridLevel]:
        assert self.grid_plan is not None
        for lv in self.grid_plan.sell_levels:
            if lv.index == index:
                return lv
        return None

    def _find_buy_level(self, index: int) -> Optional[GridLevel]:
        assert self.grid_plan is not None
        for lv in self.grid_plan.buy_levels:
            if lv.index == index:
                return lv
        return None

    async def on_external_cancel(self, report: ExecutionReport) -> None:
        """Külső beavatkozás: order törlése nem a bot által."""
        policy = self.settings.safety.external_intervention_policy
        log.warning("Külső order törlés detektálva", cid=report.client_order_id, policy=policy)

        if policy == "pause":
            self.status = BotStatus.PAUSED_EXTERNAL_INTERVENTION
            log.warning("Bot szüneteltetve külső beavatkozás miatt")
        elif policy == "emergency_stop":
            await self.emergency_stop()
        # continue_reconcile esetén nem csinálunk semmit extra

    async def emergency_stop(self) -> None:
        """Vészleállítás indítása – részletek az emergency.py-ban."""
        self.status = BotStatus.EMERGENCY_STOPPING
        self._stop_event.set()
        log.critical("Grid motor vészleállítás elindult")

    async def pause(self) -> None:
        if self.status == BotStatus.RUNNING:
            self.status = BotStatus.PAUSED

    async def resume(self) -> None:
        if self.status in (BotStatus.PAUSED, BotStatus.PAUSED_EXTERNAL_INTERVENTION):
            self.status = BotStatus.RUNNING

    async def stop(self) -> None:
        self.status = BotStatus.STOPPED
        self._stop_event.set()
