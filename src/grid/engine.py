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
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from app.config import Settings
from app.log_setup import get_logger
from exchange.market_stream import MarketStream
from exchange.models import ExecutionReport, SymbolInfo
from exchange.precision import round_down_to_step, round_price_to_tick
from exchange.signing import make_timestamp
from exchange.ws_api import BinanceWsApi
from grid.calculator import GridCalculator, GridLevel, GridPlan
from grid.inventory import InventoryManager
from grid.order_router import OrderIntent, OrderRouter, make_client_order_id
from grid.pnl import PnlTracker
from grid.state_machine import LocalOrderState
from persistence.writer import DbEvent

log = get_logger(__name__)


def _ms_to_timestr(ms: int) -> str:
    """Binance epoch ms → 'HH:MM:SS.mmm' (UTC)."""
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


@dataclass
class MissedLevel:
    """Post-only reject vagy EXPIRED után — várja a maker-szafe ár visszatérését."""
    level_index: int
    side: str                              # BUY | SELL
    target_price: Decimal
    target_qty: Decimal
    original_pair_id: Optional[str]
    buy_fill_quote: Optional[Decimal]      # csak SELL counter — profit check
    retry_count: int
    last_attempt_ms: int


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

        # Grid map: level_index → GridLevel – O(1) counter order lookup
        self.grid_map: dict[int, GridLevel] = {}

        # Szint -> aktív order ID tracking
        self._level_orders: dict[int, str] = {}  # level_index -> client_order_id
        self._order_levels: dict[str, int] = {}  # client_order_id -> level_index
        self._order_sides: dict[str, str] = {}   # client_order_id -> BUY|SELL
        self._order_pairs: dict[str, Optional[str]] = {}   # client_order_id -> pair_id

        # MISSED state (post-only reject vagy EXPIRED — várja maker-szafe ár visszatérését)
        self._missed_levels: dict[int, "MissedLevel"] = {}
        # pair_id -> BUY fill quote — counter SELL profit-checkhez
        self._buy_fill_quote_by_pair: dict[str, Decimal] = {}

        self._cycle_seq = 0
        self._order_seq = 0
        self._pair_seq = 0
        self._stop_event = asyncio.Event()
        self._bootstrap_cid: Optional[str] = None
        self._bootstrap_pending = False

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

        # 0. Szerver idő szinkronizálás WS API-n keresztül (NEM REST)
        try:
            result = await self.ws_api._query("time", {}, authenticated=False)
            server_ms = result["serverTime"]
            local_ms = int(__import__("time").time() * 1000)
            offset = server_ms - local_ms
            from exchange.signing import set_time_offset
            set_time_offset(offset)
            if abs(offset) > 1000:
                log.warning("Rendszeróra eltérés korrigálva", offset_ms=offset)
            else:
                log.info("Szerver idő szinkronizálva", offset_ms=offset)
        except Exception as e:
            log.warning("Szerver idő szinkron sikertelen", error=str(e))

        # 1. exchangeInfo
        symbol = self.settings.bot.symbol
        info_data = await self.ws_api.get_exchange_info(symbol)
        symbols = info_data.get("symbols", [])
        sym_data = next((s for s in symbols if s["symbol"] == symbol), None)
        if sym_data is None:
            raise ValueError(f"Symbol {symbol} nem található az exchangeInfo-ban")
        self.symbol_info = SymbolInfo.from_exchange_info(sym_data)
        log.info("exchangeInfo betöltve", symbol=symbol)

        # exchangeInfo után order_quote_value számítása (ha nincs explicit megadva)
        min_notional = self.symbol_info.notional.min_notional
        bot_cfg = self.settings.bot
        available = bot_cfg.total_capital_quote * (1 - bot_cfg.quote_reserve_pct)

        if bot_cfg.order_quote_value is None:
            # Auto-számítás: tőke / szintek száma, de legalább min_notional × 1.1
            per_level = (available / bot_cfg.max_grid_levels).quantize(Decimal("0.01"))
            min_safe = (min_notional * Decimal("1.1")).quantize(Decimal("0.01"))
            bot_cfg.order_quote_value = max(per_level, min_safe)
            log.info(
                "order_quote_value auto-kalkulálva",
                value=str(bot_cfg.order_quote_value),
                capital_per_level=str(per_level),
                min_notional=str(min_notional),
            )
        elif bot_cfg.order_quote_value < min_notional:
            # Explicit megadva, de Binance visszautasítaná – hibával leállunk
            raise ValueError(
                f"order_quote_value ({bot_cfg.order_quote_value}) kisebb mint a Binance "
                f"min_notional ({min_notional}) a {symbol} páron. "
                f"Állítsd be legalább: order_quote_value: \"{(min_notional * Decimal('1.1')).quantize(Decimal('0.01'))}\""
            )

        # 2. Account + inventory
        account_data = await self.ws_api.get_account()
        self.inventory.update_from_account(account_data.get("balances", []))
        # Csak a bot által kezelt asseteket logoljuk (nem az összes 600+ assetot)
        bot_assets = {self.settings.bot.base_asset, self.settings.bot.quote_asset}
        relevant = {a: v for a, v in self.inventory.snapshot().items() if a in bot_assets}
        log.info("Account betöltve", balances=relevant)

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
                bot.target_profit_pct,
                bot.target_net_profit_per_cycle_quote,
                fb, fs,
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
                bot.target_profit_pct,
                bot.target_net_profit_per_cycle_quote,
                fb, fs, worst_buy_price,
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

        if self.grid_plan.k_buy == 0 and self.grid_plan.k_sell == 0:
            raise ValueError(
                f"Grid generálás sikertelen: 0 érvényes szint. "
                f"Lehetséges okok: order_quote_value ({bot.order_quote_value}) < min_notional, "
                f"vagy nincs elég tőke. "
                f"Ajánlott order_quote_value: {self.symbol_info.notional.min_notional * Decimal('1.2'):.2f} USDC, "
                f"total_capital_quote legalább: "
                f"{self.symbol_info.notional.min_notional * Decimal('1.2') * 2:.2f} USDC."
            )

        # Profit/cycle kiszámítása és logolás
        step = self.grid_plan.grid_step_pct or (self.grid_plan.grid_step_abs / anchor_price if self.grid_plan.grid_step_abs else Decimal("0"))
        break_even_step = (1 + fb) / (1 - fs) - 1
        cost_per_cycle = bot.order_quote_value * (1 + fb)
        revenue_per_cycle = bot.order_quote_value * (1 + step) * (1 - fs)
        profit_per_cycle = revenue_per_cycle - cost_per_cycle

        # Grid táblázat — minden szint felsorolva (felülről lefelé)
        rows = []
        for lv in sorted(self.grid_plan.sell_levels, key=lambda x: -x.index):
            rows.append((lv.index, "SELL", lv.price, lv.quantity, lv.notional))
        rows.append((0, "ANCHOR", anchor_price, Decimal("0"), Decimal("0")))
        for lv in sorted(self.grid_plan.buy_levels, key=lambda x: -x.index):
            rows.append((lv.index, "BUY", lv.price, lv.quantity, lv.notional))

        log.info(
            "GRID_TABLE",
            symbol=bot.symbol,
            type=bot.grid_type,
            anchor=str(anchor_price),
            step_pct=f"{step*100:.3f}%",
            break_even_pct=f"{break_even_step*100:.3f}%",
            grid_low=str(self.grid_plan.grid_low_price),
            grid_high=str(self.grid_plan.grid_high_price),
            grid_range_pct=f"{((self.grid_plan.grid_high_price / self.grid_plan.grid_low_price - 1) * 100):.1f}%"
            if self.grid_plan.grid_low_price and self.grid_plan.grid_high_price
            else "N/A",
            k_buy=self.grid_plan.k_buy,
            k_sell=self.grid_plan.k_sell,
            order_quote_value=f"{bot.order_quote_value} {bot.quote_asset}",
            profit_per_cycle=f"{profit_per_cycle:.4f} {bot.quote_asset}",
            quote_needed=f"{self.grid_plan.total_quote_required:.2f} {bot.quote_asset}",
            base_needed=f"{self.grid_plan.total_base_required.normalize()} {bot.base_asset}",
            rows=rows,
            base_asset=bot.base_asset,
            quote_asset=bot.quote_asset,
        )

        # Grid map építés – O(1) counter order lookup
        self.grid_map = {}
        for level in self.grid_plan.buy_levels:
            self.grid_map[level.index] = level
        for level in self.grid_plan.sell_levels:
            self.grid_map[level.index] = level
        rounded_anchor = round_price_to_tick(anchor_price, self.symbol_info.price_filter.tick_size)
        self.grid_map[0] = GridLevel(
            index=0, price=rounded_anchor, side="ANCHOR",
            quantity=Decimal("0"), notional=Decimal("0"), zone="ANCHOR",
        )

        # Grid paraméterek + szintek mentése DB-be (recovery-hez)
        self.db_queue.put_nowait(DbEvent(
            type="update_bot_run_grid_params",
            data={
                "run_id": bot_run_id,
                "anchor_price": self.grid_plan.anchor_price,
                "grid_step_pct": self.grid_plan.grid_step_pct,
                "grid_step_abs": self.grid_plan.grid_step_abs,
                "order_quote_value": bot.order_quote_value,
                "grid_low_price": self.grid_plan.grid_low_price,
                "grid_high_price": self.grid_plan.grid_high_price,
            },
        ))
        levels_to_save = []
        for level in self.grid_map.values():
            levels_to_save.append({
                "level_index": level.index,
                "price": level.price,
                "side_zone": level.zone,
            })
        self.db_queue.put_nowait(DbEvent(
            type="save_grid_levels",
            data={"bot_run_id": bot_run_id, "levels": levels_to_save},
        ))

        # 5. Pre-flight balance check (CRITICAL: ne küldjünk ordert ha nem fér el!)
        if bot.inventory_mode == "quote_only_bootstrap":
            self._preflight_balance_check()

        # 6. Kezdeti order-ek elküldése (non-blocking)
        self.status = BotStatus.RUNNING

        # MISSED recovery callback regisztrálás a market stream-en
        if self.market_stream is not None:
            self.market_stream.register_tick_callback(self._on_market_tick)

        await self._place_initial_orders()

    def _preflight_balance_check(self) -> None:
        """
        Indulás előtti tőke ellenőrzés quote_only_bootstrap módban.
        Hibát dob, ha a buy orderek + bootstrap nem fér el az elérhető quote-ba.
        Ha bukik: SEMMI nem megy ki az exchange-re, a bot gracefully leáll.
        """
        assert self.grid_plan is not None
        bot = self.settings.bot
        bootstrap_cfg = self.settings.bootstrap
        fb = self.settings.fees.effective_buy_fee
        buffer = bootstrap_cfg.base_buffer_pct

        # Bootstrap quote igénye: vagy explicit megadva, vagy ugyanaz a képlet mint az _execute_bootstrap-ben
        bootstrap_qty = bootstrap_cfg.quote_qty
        if bootstrap_qty is None:
            required_base = sum(
                (lv.quantity for lv in self.grid_plan.sell_levels),
                Decimal("0"),
            )
            gross_base = required_base * (Decimal("1") + buffer) / (Decimal("1") - fb)
            bootstrap_qty = (gross_base * self.grid_plan.anchor_price).quantize(Decimal("0.01"))

        # Buy orderek lockolt tőkéje
        buy_lock = sum(
            (lv.notional for lv in self.grid_plan.buy_levels),
            Decimal("0"),
        )
        total_needed = buy_lock + bootstrap_qty
        free_quote = self.inventory.free(bot.quote_asset)

        if total_needed > free_quote:
            raise ValueError(
                f"Nincs elég {bot.quote_asset} a grid + bootstrap induláshoz!\n"
                f"  szükséges: {total_needed:.2f} {bot.quote_asset}\n"
                f"    - {self.grid_plan.k_buy} BUY order: {buy_lock:.2f}\n"
                f"    - bootstrap (base vétel): {bootstrap_qty:.2f}\n"
                f"  rendelkezésre áll: {free_quote:.2f} {bot.quote_asset}\n"
                f"  hiány: {total_needed - free_quote:.2f} {bot.quote_asset}\n"
                f"  Megoldás: növeld total_capital_quote-t legalább {total_needed:.2f}-re, "
                f"vagy csökkentsd max_grid_levels-t."
            )

        log.info(
            "Pre-flight balance OK",
            free=f"{free_quote:.2f}",
            needed=f"{total_needed:.2f}",
            buy_lock=f"{buy_lock:.2f}",
            bootstrap=f"{bootstrap_qty:.2f}",
        )

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
        buy_placed = 0
        for level in self.grid_plan.buy_levels:
            if not self.inventory.has_quote_for_buy(level.notional):
                log.warning("Nincs elég quote a buy order-hez", lvl=level.index, price=str(level.price))
                continue
            cid = self._submit_level_order(level, cycle_id=0)
            log.info(
                "INIT_ORDER", side="BUY", lvl=level.index,
                price=level.price.normalize(), qty=level.quantity.normalize(),
                notional=f"{level.notional:.2f}",
                quote_asset=bot.quote_asset, cid=cid,
            )
            buy_placed += 1

        # Sell order-ek (csak ha van base inventory)
        sell_placed = 0
        if bot.inventory_mode in ("prebalanced", "use_existing_balances"):
            for level in self.grid_plan.sell_levels:
                if not self.inventory.has_base_for_sell(level.quantity):
                    log.warning("Nincs elég base a sell order-hez", lvl=level.index)
                    break
                cid = self._submit_level_order(level, cycle_id=0)
                log.info(
                    "INIT_ORDER", side="SELL", lvl=level.index,
                    price=level.price.normalize(), qty=level.quantity.normalize(),
                    notional=f"{level.notional:.2f}",
                    quote_asset=bot.quote_asset, cid=cid,
                )
                sell_placed += 1
        elif bot.inventory_mode == "quote_only_bootstrap":
            await self._execute_bootstrap()

        log.info("Kezdeti order-ek bekülve", buy=buy_placed, sell=sell_placed)

    def _submit_level_order(self, level: GridLevel, cycle_id: int, pair_id: Optional[str] = None) -> str:
        """Egy grid szint order-ét beküldi a routerbe. Anti-double védett."""
        assert self.router is not None

        # Anti-double: ha már van élő order ezen a szinten, skip
        existing_cid = self._level_orders.get(level.index)
        if existing_cid:
            log.warning(
                "Skip duplicate submit",
                lvl=level.index, side=level.side, existing_cid=existing_cid,
            )
            return existing_cid

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
            pair_id=pair_id,
            cycle_id=str(cycle_id),
        )
        self.router.submit_order(intent)
        self._level_orders[level.index] = cid
        self._order_levels[cid] = level.index
        self._order_sides[cid] = level.side
        self._order_pairs[cid] = pair_id
        return cid

    async def on_execution_report(self, report: ExecutionReport) -> None:
        """
        executionReport esemény feldolgozása.

        Ha fill → counter order küldés.
        Ha külső cancel → policy alapján reakció.
        """
        if self.status == BotStatus.EMERGENCY_STOPPING:
            return

        cid = report.client_order_id
        if not cid.startswith("G-"):
            return

        # Bootstrap fill kezelése
        if self._bootstrap_cid and cid == self._bootstrap_cid:
            if report.execution_type == "TRADE" and report.order_status == "FILLED":
                await self._on_bootstrap_filled(report)
            return

        if report.execution_type == "TRADE" and report.order_status == "FILLED":
            await self._on_order_filled(report)

    async def on_order_rejected(self, report: ExecutionReport) -> None:
        """REJECTED/EXPIRED feldolgozás: post-only → MISSED, egyéb → log + cleanup."""
        if self.status == BotStatus.EMERGENCY_STOPPING:
            return
        cid = report.client_order_id
        if not cid.startswith("G-"):
            return

        # Bootstrap reject külön branch
        if self._bootstrap_cid and cid == self._bootstrap_cid:
            await self._on_bootstrap_rejected(report)
            return

        level_index = self._order_levels.get(cid)
        side = self._order_sides.get(cid)
        if level_index is None or side is None:
            log.warning("Reject ismeretlen CID-re", cid=cid)
            return

        grid_line = self.grid_map.get(level_index)
        if grid_line is None:
            log.error("Reject ismeretlen szinten", cid=cid, lvl=level_index)
            self._level_orders.pop(level_index, None)
            return

        reason = (report.reject_reason or "").upper()
        is_post_only = (
            "IMMEDIATELY_MATCH" in reason
            or "POST_ONLY" in reason
            or "WOULD_TAKE" in reason
            or report.execution_type == "EXPIRED"
            or report.order_status == "EXPIRED"
        )

        # Cleanup mindenképp
        self._level_orders.pop(level_index, None)

        # DB: Order.status_local frissítés
        new_state = "MISSED" if is_post_only else "REJECTED"
        self.db_queue.put_nowait(DbEvent(
            type="update_order_local_state",
            data={
                "client_order_id": cid,
                "state": new_state,
                "reject_reason": reason or report.execution_type,
            },
        ))

        if is_post_only:
            pair_id = self._order_pairs.get(cid)
            buy_quote = self._buy_fill_quote_by_pair.get(pair_id) if pair_id else None
            self._missed_levels[level_index] = MissedLevel(
                level_index=level_index, side=side,
                target_price=grid_line.price,
                target_qty=grid_line.quantity if grid_line.quantity > 0 else Decimal("0"),
                original_pair_id=pair_id, buy_fill_quote=buy_quote,
                retry_count=0, last_attempt_ms=make_timestamp(),
            )
            log.info("MISSED", side=side, lvl=level_index,
                     price=str(grid_line.price), reason=reason or report.execution_type)
        else:
            log.error("Order REJECTED nem-post-only",
                      cid=cid, side=side, lvl=level_index, reason=reason)

    def _on_market_tick(self, mid_price: Decimal) -> None:
        """Market stream callback: missed cellák próbálkozása ha az ár újra maker-szafe."""
        if not self._missed_levels or self.status != BotStatus.RUNNING:
            return
        try:
            self._try_recover_missed(mid_price)
        except Exception as e:
            log.warning("MISSED recovery loop hiba", error=str(e))

    def _try_recover_missed(self, mid_price: Decimal) -> None:
        """Mid_price tick: ha egy MISSED cella maker-szafe lett, újra submit."""
        fees = self.settings.fees
        fs = fees.effective_sell_fee
        debounce_ms = 500
        now_ms = make_timestamp()

        for level_index, missed in list(self._missed_levels.items()):
            if now_ms - missed.last_attempt_ms < debounce_ms:
                continue

            if missed.side == "BUY":
                # BUY @ P maker-szafe ha mid > P (ár felette van)
                if mid_price <= missed.target_price:
                    continue
            else:  # SELL
                # SELL @ P maker-szafe ha mid < P (ár alatta van)
                if mid_price >= missed.target_price:
                    continue
                # Profit-check (pair_id alapján): a SELL ezen az áron + fee után
                # ne adjon kevesebb quote-ot mint az eredeti BUY-ra elköltött
                if missed.buy_fill_quote and missed.buy_fill_quote > 0 and missed.target_qty > 0:
                    expected = missed.target_qty * missed.target_price * (Decimal("1") - fs)
                    if expected <= missed.buy_fill_quote:
                        continue  # várj amíg magasabb az ár

            grid_level = self.grid_map.get(level_index)
            if grid_level is None:
                self._missed_levels.pop(level_index, None)
                continue

            log.info(
                "MISSED_RETRY", side=missed.side, lvl=level_index,
                price=str(missed.target_price),
                retry=missed.retry_count + 1, mid=str(mid_price),
            )
            missed.retry_count += 1
            missed.last_attempt_ms = now_ms
            # Kivesszük a missed-ből; ha újra reject jön, on_order_rejected visszateszi
            self._missed_levels.pop(level_index, None)
            self._submit_level_order(
                grid_level,
                cycle_id=self._cycle_seq,
                pair_id=missed.original_pair_id,
            )

    async def _on_bootstrap_rejected(self, report: ExecutionReport) -> None:
        """Bootstrap LIMIT_MAKER reject → MARKET fallback. MARKET reject → emergency."""
        log.error("Bootstrap REJECTED",
                  cid=report.client_order_id, reason=report.reject_reason,
                  execution_type=report.execution_type)
        if self.settings.bootstrap.order_type != "MARKET":
            log.warning(
                "Bootstrap fallback MARKET-re",
                original_type=self.settings.bootstrap.order_type,
            )
            self._bootstrap_cid = None
            self._bootstrap_pending = False
            original = self.settings.bootstrap.order_type
            self.settings.bootstrap.order_type = "MARKET"
            try:
                await self._execute_bootstrap()
            finally:
                self.settings.bootstrap.order_type = original
        else:
            from supervisor.emergency import EmergencyStop
            # Az emergency referenciát nem itt birtokoljuk — de a watchdog majd észleli
            log.critical(
                "Bootstrap MARKET REJECTED — bot leblokkol, kézi beavatkozás kell!",
                reason=report.reject_reason,
            )

    async def _on_order_filled(self, report: ExecutionReport) -> None:
        """
        Fill esemény: counter order küldése fix grid vonalakról.

        O(1) grid_map lookup – nincs újraszámolás, nincs DB olvasás.
        """
        if self.status != BotStatus.RUNNING:
            log.info("Bot nem fut, counter order kihagyva", status=self.status)
            return

        assert self.symbol_info is not None

        cid = report.client_order_id
        level_index = self._order_levels.get(cid)
        side = self._order_sides.get(cid)

        if level_index is None or side is None:
            log.warning("Ismeretlen level a fill-hez", cid=cid)
            return

        self._cycle_seq += 1
        self._pair_seq += 1
        pair_id = f"P-{self._pair_seq:04d}"

        log.info("FILL", side=side, lvl=level_index,
                 price=report.last_executed_price.normalize(),
                 qty=report.cumulative_filled_qty.normalize(),
                 quote=f"{report.cumulative_quote_qty:.2f}",
                 fee=f"{float(report.commission_amount):.4f} {report.commission_asset}",
                 filled_at=_ms_to_timestr(report.transaction_time),
                 pair=pair_id)

        # FILLED cleanup: a fillelő ordert kivesszük a _level_orders-ből,
        # különben a következő counter SELL/BUY ezen a szinten anti-double-skip-pel ütközne.
        self._level_orders.pop(level_index, None)

        # BUY fill esetén mentjük a quote-ot a counter SELL profit-checkhez (pair_id alapján)
        if side == "BUY":
            self._buy_fill_quote_by_pair[pair_id] = report.cumulative_quote_qty

        if side == "BUY":
            counter_index = level_index + 1
            counter_side = "SELL"
        else:
            counter_index = level_index - 1
            counter_side = "BUY"

        grid_line = self.grid_map.get(counter_index)
        if grid_line is None:
            log.error("Nincs grid vonal a counter indexhez", counter_index=counter_index, filled_cid=cid)
            return
        counter_price = round_price_to_tick(grid_line.price, self.symbol_info.price_filter.tick_size)

        filled_grid_line = self.grid_map.get(level_index)
        if filled_grid_line and filled_grid_line.quantity > 0:
            qty = filled_grid_line.quantity
        else:
            qty = round_down_to_step(
                self.settings.bot.order_quote_value / counter_price,
                self.symbol_info.lot_size.step_size,
            )

        counter_level = GridLevel(
            index=counter_index,
            price=counter_price,
            side=counter_side,
            quantity=qty,
            notional=qty * counter_price,
            zone="ABOVE_ANCHOR" if counter_price > self.grid_map.get(0, grid_line).price else "BELOW_ANCHOR",
        )
        self._submit_level_order(counter_level, cycle_id=self._cycle_seq, pair_id=pair_id)

        sent_ms = make_timestamp()
        log.info("COUNTER", side=counter_side, lvl=counter_index,
                 price=counter_price.normalize(), qty=qty.normalize(),
                 value=f"{qty * counter_price:.2f}",
                 sent_at=_ms_to_timestr(sent_ms),
                 latency_ms=sent_ms - report.transaction_time,
                 pair=pair_id)

    async def _execute_bootstrap(self) -> None:
        """Bootstrap: MARKET buy küldése a sell grid orderekhez szükséges base megszerzéséhez."""
        bootstrap = self.settings.bootstrap
        bot = self.settings.bot
        assert self.grid_plan is not None

        quote_qty = bootstrap.quote_qty
        if quote_qty is None:
            if self.grid_plan and self.grid_plan.sell_levels:
                required_base = sum(
                    (lv.quantity for lv in self.grid_plan.sell_levels),
                    Decimal("0"),
                )
                buffer = bootstrap.base_buffer_pct
                fee_buy = self.settings.fees.effective_buy_fee
                gross_base = required_base * (Decimal("1") + buffer) / (Decimal("1") - fee_buy)
                quote_qty = (gross_base * self.grid_plan.anchor_price).quantize(Decimal("0.01"))
                log.info(
                    "Bootstrap quote_qty kalkulált",
                    k_sell=len(self.grid_plan.sell_levels),
                    required_base=str(required_base),
                    buffer_pct=str(buffer),
                    gross_base=str(gross_base),
                    quote_qty=str(quote_qty),
                )
            else:
                quote_qty = bot.total_capital_quote * bot.buy_allocation_ratio
                log.warning(
                    "Bootstrap fallback: nincs sell_levels — total_capital × buy_allocation_ratio",
                    quote_qty=str(quote_qty),
                )

        self._order_seq += 1
        cid = f"G-{self._bot_run_short_id}-BOOT-{self._order_seq:04d}"
        self._bootstrap_cid = cid
        self._bootstrap_pending = True

        bootstrap_price = Decimal("0")
        bootstrap_qty = Decimal("0")

        if bootstrap.order_type == "MARKET":
            self.ws_api.enqueue_market_buy(
                symbol=bot.symbol,
                quote_order_qty=quote_qty,
                client_order_id=cid,
            )
            bootstrap_price = self.grid_plan.anchor_price
            bootstrap_qty = quote_qty / bootstrap_price
            log.info("Bootstrap MARKET buy beküldve", cid=cid, quote_qty=str(quote_qty))
        else:
            assert self.symbol_info is not None
            anchor = self.grid_plan.anchor_price
            offset = bootstrap.limit_price_offset_pct
            from exchange.precision import round_price_to_tick, round_down_to_step
            bootstrap_price = round_price_to_tick(
                anchor * (Decimal("1") - offset),
                self.symbol_info.price_filter.tick_size,
            )
            bootstrap_qty = round_down_to_step(
                quote_qty / bootstrap_price,
                self.symbol_info.lot_size.step_size,
            )
            self.ws_api.enqueue_order(
                symbol=bot.symbol,
                side="BUY",
                order_type=bootstrap.order_type,
                price=bootstrap_price,
                quantity=bootstrap_qty,
                client_order_id=cid,
                time_in_force="GTC",
            )
            log.info("Bootstrap LIMIT buy beküldve", cid=cid, price=str(bootstrap_price), qty=str(bootstrap_qty))

        self.db_queue.put_nowait(DbEvent(
            type="upsert_order_from_intent",
            data={
                "bot_run_id": self.bot_run_id,
                "client_order_id": cid,
                "symbol": bot.symbol,
                "side": "BUY",
                "order_type": bootstrap.order_type,
                "time_in_force": "GTC",
                "price": bootstrap_price,
                "original_quantity": bootstrap_qty,
                "executed_quantity": Decimal("0"),
                "cumulative_quote_quantity": Decimal("0"),
                "status_local": "SUBMIT_QUEUED",
                "grid_level_index": 0,
                "cycle_id": "bootstrap",
            },
        ))

        self.db_queue.put_nowait(DbEvent(
            type="log_system_event",
            data={
                "severity": "INFO",
                "component": "engine",
                "event_type": "bootstrap_initiated",
                "message": f"Bootstrap {bootstrap.order_type} buy beküldve: {quote_qty} {bot.quote_asset}",
                "payload": {"cid": cid, "quote_qty": str(quote_qty), "order_type": bootstrap.order_type},
            },
        ))

    async def _on_bootstrap_filled(self, report: ExecutionReport) -> None:
        """Bootstrap fill: sell grid orderek elhelyezése a szerzett base-szel."""
        self._bootstrap_pending = False
        self._bootstrap_cid = None
        assert self.grid_plan is not None

        acquired = report.cumulative_filled_qty
        if report.commission_asset == self.settings.bot.base_asset:
            acquired -= report.commission_amount

        avg_price = report.cumulative_quote_qty / report.cumulative_filled_qty if report.cumulative_filled_qty > 0 else Decimal("0")
        log.info(
            "Bootstrap fill kész",
            acquired_base=str(acquired),
            spent_quote=str(report.cumulative_quote_qty),
            avg_price=str(avg_price),
        )

        placed = 0
        remaining = acquired
        bot = self.settings.bot
        for level in self.grid_plan.sell_levels:
            if remaining < level.quantity:
                log.info("Bootstrap: nincs elég base a további sell szintekhez",
                         remaining=str(remaining), needed=str(level.quantity))
                break
            cid = self._submit_level_order(level, cycle_id=0)
            log.info(
                "INIT_ORDER", side="SELL", lvl=level.index,
                price=level.price.normalize(), qty=level.quantity.normalize(),
                notional=f"{level.notional:.2f}",
                quote_asset=bot.quote_asset, cid=cid,
            )
            remaining -= level.quantity
            placed += 1

        log.info("Bootstrap sell orderek beküldve", count=placed, remaining_base=str(remaining))

    async def recover(
        self,
        bot_run_id: int,
        bot_run_short_id: str,
        bot_run_record,
        grid_levels_db: list,
        open_orders: list[dict],
        symbol_info: SymbolInfo,
        max_pair_seq: int = 0,
        missed_records: Optional[list] = None,
        buy_fills_by_pair: Optional[dict] = None,
    ) -> None:
        """
        Recovery: bent ragadt orderekből újraépíti a grid állapotot.

        1. grid_map újraépítés DB-ből
        2. In-memory tracking újraépítés open orderekből
        3. Sequence counters visszaállítás
        4. Account frissítés
        """
        self.bot_run_id = bot_run_id
        self._bot_run_short_id = bot_run_short_id
        self.symbol_info = symbol_info
        self.router = OrderRouter(self.ws_api, self.db_queue, bot_run_id, self.settings.bot)

        log.info("Recovery indítás", bot_run_id=bot_run_id)

        # Szerver idő szinkronizálás
        try:
            result = await self.ws_api._query("time", {}, authenticated=False)
            server_ms = result["serverTime"]
            local_ms = int(time.time() * 1000)
            offset = server_ms - local_ms
            from exchange.signing import set_time_offset
            set_time_offset(offset)
            log.info("Szerver idő szinkronizálva (recovery)", offset_ms=offset)
        except Exception as e:
            log.warning("Szerver idő szinkron sikertelen", error=str(e))

        # Grid map újraépítés DB-ből
        self.grid_map = {}
        buy_levels: list[GridLevel] = []
        sell_levels: list[GridLevel] = []
        for gl in grid_levels_db:
            if gl.level_index < 0:
                side = "BUY"
            elif gl.level_index > 0:
                side = "SELL"
            else:
                side = "ANCHOR"
            level = GridLevel(
                index=gl.level_index, price=gl.price, side=side,
                quantity=Decimal("0"), notional=Decimal("0"), zone=gl.side_zone,
            )
            self.grid_map[gl.level_index] = level
            if gl.level_index < 0:
                buy_levels.append(level)
            elif gl.level_index > 0:
                sell_levels.append(level)

        self.grid_plan = GridPlan(
            grid_type=bot_run_record.grid_type,
            anchor_price=bot_run_record.anchor_price,
            grid_step_pct=bot_run_record.grid_step_pct,
            grid_step_abs=bot_run_record.grid_step_abs,
            buy_levels=buy_levels,
            sell_levels=sell_levels,
            k_buy=len(buy_levels),
            k_sell=len(sell_levels),
            total_quote_required=Decimal("0"),
            total_base_required=Decimal("0"),
        )

        # order_quote_value visszaállítás
        if bot_run_record.order_quote_value:
            self.settings.bot.order_quote_value = bot_run_record.order_quote_value

        # In-memory tracking újraépítés az exchange open orderekből
        for order in open_orders:
            cid = order.get("clientOrderId", "")
            if not cid.startswith("G-"):
                continue
            parts = cid.split("-")
            if len(parts) < 6:
                continue
            side_char = parts[2]
            if side_char == "BOOT":
                self._bootstrap_cid = cid
                self._bootstrap_pending = True
                log.info("Bootstrap order recovery", cid=cid)
                continue
            level_abs = int(parts[3])
            side = "BUY" if side_char == "B" else "SELL"
            level_index = -level_abs if side == "BUY" else level_abs

            self._level_orders[level_index] = cid
            self._order_levels[cid] = level_index
            self._order_sides[cid] = side
            self.router._submitted.add(cid)

        # Sequence counters: legmagasabb cycle és seq megkeresése
        max_cycle = 0
        max_seq = 0
        for cid in self._order_levels:
            parts = cid.split("-")
            if len(parts) >= 6:
                max_cycle = max(max_cycle, int(parts[4]))
                max_seq = max(max_seq, int(parts[5]))
        self._cycle_seq = max_cycle + 1
        self._order_seq = max_seq + 1
        self._pair_seq = max_pair_seq
        log.info(
            "Sequence counters visszaállítva",
            cycle_seq=self._cycle_seq,
            order_seq=self._order_seq,
            pair_seq=self._pair_seq,
        )

        # MISSED state visszaállítása DB-ből
        if missed_records:
            buy_fills_map = buy_fills_by_pair or {}
            now_ms = make_timestamp()
            for o in missed_records:
                lvl_idx = o.grid_level_index
                if lvl_idx is None:
                    continue
                grid_line = self.grid_map.get(lvl_idx)
                target_price = grid_line.price if grid_line else o.price
                target_qty = grid_line.quantity if (grid_line and grid_line.quantity > 0) else o.original_quantity
                self._missed_levels[lvl_idx] = MissedLevel(
                    level_index=lvl_idx,
                    side=o.side,
                    target_price=target_price,
                    target_qty=target_qty,
                    original_pair_id=o.pair_id,
                    buy_fill_quote=buy_fills_map.get(o.pair_id),
                    retry_count=0,
                    last_attempt_ms=now_ms,
                )
            log.info("MISSED visszaállítva DB-ből", count=len(self._missed_levels))

        # Account frissítés
        account_data = await self.ws_api.get_account()
        self.inventory.update_from_account(account_data.get("balances", []))
        bot_assets = {self.settings.bot.base_asset, self.settings.bot.quote_asset}
        relevant = {a: v for a, v in self.inventory.snapshot().items() if a in bot_assets}

        self.status = BotStatus.RUNNING

        # MISSED recovery callback regisztrálás a market stream-en
        if self.market_stream is not None:
            self.market_stream.register_tick_callback(self._on_market_tick)

        log.info("Recovery kész",
                 bot_run_id=bot_run_id,
                 open_orders=len(self._level_orders),
                 grid_levels=len(self.grid_map),
                 missed_levels=len(self._missed_levels),
                 balances=relevant,
                 bootstrap_pending=self._bootstrap_pending)

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
