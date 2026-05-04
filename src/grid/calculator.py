"""Grid matematika: geometriai és aritmetikai grid számítás."""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from exchange.models import SymbolInfo
from exchange.filters import adjust_order_to_filters, validate_order
from exchange.precision import round_down_to_step, round_price_to_tick


@dataclass
class GridLevel:
    index: int              # pozitív = sell (fölötte), negatív = buy (alatta)
    price: Decimal
    side: str               # BUY | SELL
    quantity: Decimal
    notional: Decimal
    zone: str               # BELOW_ANCHOR | ABOVE_ANCHOR


@dataclass
class GridPlan:
    grid_type: str
    anchor_price: Decimal
    grid_step_pct: Optional[Decimal]    # geometriai esetén
    grid_step_abs: Optional[Decimal]    # aritmetikai esetén
    buy_levels: list[GridLevel]
    sell_levels: list[GridLevel]
    k_buy: int
    k_sell: int
    total_quote_required: Decimal
    total_base_required: Decimal


class GridCalculator:
    """Grid paraméter számítás és szint generálás."""

    def compute_geometric_step(
        self,
        order_quote_value: Decimal,
        target_profit: Decimal,
        fee_buy: Decimal,
        fee_sell: Decimal,
        min_step_pct: Decimal,
        max_step_pct: Decimal,
    ) -> Decimal:
        """
        Geometriai grid lépés számítása profit cél alapján.
        r = (1 + fb + π/V) / (1 - fs)
        Break-even step abszolút padló: a grid SOHA nem lehet veszteséges.
        """
        break_even_r = (1 + fee_buy) / (1 - fee_sell)
        break_even_step = break_even_r - 1

        if target_profit > 0:
            r = (1 + fee_buy + target_profit / order_quote_value) / (1 - fee_sell)
        else:
            r = break_even_r

        g = r - 1

        effective_min = max(min_step_pct, break_even_step)
        g = max(g, effective_min)
        if g > max_step_pct:
            raise ValueError(
                f"Szükséges grid step ({g:.6f}) meghaladja max_grid_step_pct={max_step_pct}. "
                f"Növeld a profitcélt vagy csökkentsd a díjakat."
            )
        return g

    def compute_arithmetic_step(
        self,
        order_quote_value: Decimal,
        target_profit: Decimal,
        fee_buy: Decimal,
        fee_sell: Decimal,
        worst_case_price: Decimal,
        min_step_pct: Decimal,
        max_step_pct: Decimal,
    ) -> Decimal:
        """
        Aritmetikai grid lépés számítása.
        d >= b * ((1 + fb + π/V) / (1 - fs) - 1)
        Break-even step abszolút padló: a grid SOHA nem lehet veszteséges.
        """
        break_even_r = (1 + fee_buy) / (1 - fee_sell)
        break_even_step = break_even_r - 1

        numerator = 1 + fee_buy + target_profit / order_quote_value
        d = worst_case_price * (numerator / (1 - fee_sell) - 1)

        d_pct = d / worst_case_price
        effective_min = max(min_step_pct, break_even_step)
        d_pct = max(d_pct, effective_min)
        if d_pct > max_step_pct:
            raise ValueError(
                f"Szükséges aritmetikai step ({d_pct:.6f}) meghaladja max_grid_step_pct={max_step_pct}."
            )
        return worst_case_price * d_pct

    def compute_grid_counts(
        self,
        total_capital_quote: Decimal,
        order_quote_value: Decimal,
        quote_reserve_pct: Decimal,
        buy_allocation_ratio: Decimal,
        max_grid_levels: int,
        buy_side_override: Optional[int] = None,
        sell_side_override: Optional[int] = None,
    ) -> tuple[int, int]:
        """Meghatározza a buy és sell szintek számát tőke alapján."""
        available = total_capital_quote * (1 - quote_reserve_pct)
        k_total = int(available // order_quote_value)
        k_total = min(k_total, max_grid_levels)

        if buy_side_override is not None and sell_side_override is not None:
            k_buy = buy_side_override
            k_sell = sell_side_override
            required = (k_buy + k_sell) * order_quote_value
            if required > available:
                raise ValueError(
                    f"K_buy={k_buy} + K_sell={k_sell} szint ({required} USDT) "
                    f"meghaladja az elérhető tőkét ({available} USDT)."
                )
        else:
            k_buy = int(k_total * buy_allocation_ratio)
            k_sell = k_total - k_buy

        if k_buy == 0 and k_sell == 0:
            raise ValueError("Nincs elég tőke egyetlen grid szinthez sem.")

        return k_buy, k_sell

    def generate_geometric_grid(
        self,
        anchor_price: Decimal,
        grid_step_pct: Decimal,
        k_buy: int,
        k_sell: int,
        order_quote_value: Decimal,
        symbol_info: SymbolInfo,
    ) -> GridPlan:
        """Geometriai grid szintek generálása."""
        r = 1 + grid_step_pct
        buy_levels: list[GridLevel] = []
        sell_levels: list[GridLevel] = []

        # Buy szintek: P0 / r^i, i=1..K_buy
        for i in range(1, k_buy + 1):
            raw_price = anchor_price / (r ** i)
            price, _ = adjust_order_to_filters(raw_price, Decimal("1"), symbol_info)
            raw_qty = order_quote_value / price
            qty = round_down_to_step(raw_qty, symbol_info.lot_size.step_size)
            notional = qty * price

            result = validate_order(price, qty, symbol_info)
            if not result.valid:
                if i == 1:
                    # Az első szint is bukik → order_quote_value túl kicsi
                    from app.log_setup import get_logger
                    log = get_logger(__name__)
                    log.error(
                        "BUY szint 1 érvénytelen – order_quote_value túl kicsi! "
                        "Növeld order_quote_value-t (min_notional felett kell legyen).",
                        price=str(price), qty=str(qty), notional=str(qty * price),
                        errors=result.errors,
                        min_notional=str(symbol_info.notional.min_notional),
                        current_order_value=str(order_quote_value),
                    )
                break

            buy_levels.append(GridLevel(
                index=-i,
                price=price,
                side="BUY",
                quantity=qty,
                notional=notional,
                zone="BELOW_ANCHOR",
            ))

        # Sell szintek: P0 * r^i, i=1..K_sell
        for i in range(1, k_sell + 1):
            raw_price = anchor_price * (r ** i)
            price, _ = adjust_order_to_filters(raw_price, Decimal("1"), symbol_info)
            raw_qty = order_quote_value / anchor_price  # anchor price-on számított qty
            qty = round_down_to_step(raw_qty, symbol_info.lot_size.step_size)
            notional = qty * price

            result = validate_order(price, qty, symbol_info)
            if not result.valid:
                break

            sell_levels.append(GridLevel(
                index=i,
                price=price,
                side="SELL",
                quantity=qty,
                notional=notional,
                zone="ABOVE_ANCHOR",
            ))

        total_quote = sum(lv.notional for lv in buy_levels)
        total_base = sum(lv.quantity for lv in sell_levels)

        return GridPlan(
            grid_type="geometric",
            anchor_price=anchor_price,
            grid_step_pct=grid_step_pct,
            grid_step_abs=None,
            buy_levels=buy_levels,
            sell_levels=sell_levels,
            k_buy=len(buy_levels),
            k_sell=len(sell_levels),
            total_quote_required=total_quote,
            total_base_required=total_base,
        )

    def generate_arithmetic_grid(
        self,
        anchor_price: Decimal,
        grid_step_abs: Decimal,
        k_buy: int,
        k_sell: int,
        order_quote_value: Decimal,
        symbol_info: SymbolInfo,
    ) -> GridPlan:
        """Aritmetikai grid szintek generálása."""
        buy_levels: list[GridLevel] = []
        sell_levels: list[GridLevel] = []

        for i in range(1, k_buy + 1):
            raw_price = anchor_price - grid_step_abs * i
            price, _ = adjust_order_to_filters(raw_price, Decimal("1"), symbol_info)
            raw_qty = order_quote_value / price
            qty = round_down_to_step(raw_qty, symbol_info.lot_size.step_size)
            notional = qty * price

            result = validate_order(price, qty, symbol_info)
            if not result.valid or price <= 0:
                break

            buy_levels.append(GridLevel(
                index=-i, price=price, side="BUY",
                quantity=qty, notional=notional, zone="BELOW_ANCHOR",
            ))

        for i in range(1, k_sell + 1):
            raw_price = anchor_price + grid_step_abs * i
            price, _ = adjust_order_to_filters(raw_price, Decimal("1"), symbol_info)
            raw_qty = order_quote_value / anchor_price
            qty = round_down_to_step(raw_qty, symbol_info.lot_size.step_size)
            notional = qty * price

            result = validate_order(price, qty, symbol_info)
            if not result.valid:
                break

            sell_levels.append(GridLevel(
                index=i, price=price, side="SELL",
                quantity=qty, notional=notional, zone="ABOVE_ANCHOR",
            ))

        total_quote = sum(lv.notional for lv in buy_levels)
        total_base = sum(lv.quantity for lv in sell_levels)

        return GridPlan(
            grid_type="arithmetic",
            anchor_price=anchor_price,
            grid_step_pct=None,
            grid_step_abs=grid_step_abs,
            buy_levels=buy_levels,
            sell_levels=sell_levels,
            k_buy=len(buy_levels),
            k_sell=len(sell_levels),
            total_quote_required=total_quote,
            total_base_required=total_base,
        )
