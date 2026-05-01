"""Exchange filter validáció order paraméterekhez."""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from .models import SymbolInfo
from .precision import round_down_to_step, round_price_to_tick


@dataclass
class ValidationResult:
    valid: bool
    errors: list[str]

    @classmethod
    def ok(cls) -> "ValidationResult":
        return cls(valid=True, errors=[])

    @classmethod
    def fail(cls, *errors: str) -> "ValidationResult":
        return cls(valid=False, errors=list(errors))


def validate_order(
    price: Decimal,
    qty: Decimal,
    symbol_info: SymbolInfo,
) -> ValidationResult:
    """Ellenőrzi, hogy az order megfelel-e az exchange filter-eknek."""
    errors: list[str] = []
    pf = symbol_info.price_filter
    lf = symbol_info.lot_size
    nf = symbol_info.notional

    # PRICE_FILTER
    if price < pf.min_price:
        errors.append(f"Ár {price} kisebb mint minPrice {pf.min_price}")
    if pf.max_price > 0 and price > pf.max_price:
        errors.append(f"Ár {price} nagyobb mint maxPrice {pf.max_price}")
    rounded_price = round_price_to_tick(price, pf.tick_size)
    if rounded_price != price:
        errors.append(f"Ár {price} nem illeszkedik tick_size {pf.tick_size}-re (várható: {rounded_price})")

    # LOT_SIZE
    if qty < lf.min_qty:
        errors.append(f"Mennyiség {qty} kisebb mint minQty {lf.min_qty}")
    if lf.max_qty > 0 and qty > lf.max_qty:
        errors.append(f"Mennyiség {qty} nagyobb mint maxQty {lf.max_qty}")
    rounded_qty = round_down_to_step(qty, lf.step_size)
    if rounded_qty != qty:
        errors.append(f"Mennyiség {qty} nem illeszkedik stepSize {lf.step_size}-re (várható: {rounded_qty})")

    # NOTIONAL (MIN_NOTIONAL)
    notional = price * qty
    if notional < nf.min_notional:
        errors.append(f"Notional {notional} kisebb mint minNotional {nf.min_notional}")
    if nf.max_notional and notional > nf.max_notional:
        errors.append(f"Notional {notional} nagyobb mint maxNotional {nf.max_notional}")

    return ValidationResult(valid=len(errors) == 0, errors=errors)


def adjust_order_to_filters(
    price: Decimal,
    qty: Decimal,
    symbol_info: SymbolInfo,
) -> tuple[Decimal, Decimal]:
    """Automatikusan igazítja az árat és mennyiséget az exchange filter-ekhez."""
    pf = symbol_info.price_filter
    lf = symbol_info.lot_size
    adjusted_price = round_price_to_tick(price, pf.tick_size)
    adjusted_qty = round_down_to_step(qty, lf.step_size)
    return adjusted_price, adjusted_qty
