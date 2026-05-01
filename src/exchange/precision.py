"""Decimal precíziós kerekítő függvények exchange filter-ekhez."""
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP


def round_down_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Lefelé kerekít step méretű lépésekhez (mennyiségekhez)."""
    if step <= 0:
        raise ValueError(f"step nem lehet <= 0: {step}")
    return (value // step) * step


def round_price_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    """Legközelebbi tick-re kerekít (árakhoz)."""
    if tick <= 0:
        raise ValueError(f"tick nem lehet <= 0: {tick}")
    return (value / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick


def round_up_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Felfelé kerekít step méretű lépésekhez."""
    if step <= 0:
        raise ValueError(f"step nem lehet <= 0: {step}")
    remainder = value % step
    if remainder == 0:
        return value
    return value - remainder + step


def decimal_places(value: Decimal) -> int:
    """Visszaadja a tizedesjegyek számát."""
    sign, digits, exp = value.as_tuple()
    return max(-exp, 0)
