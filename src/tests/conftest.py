"""Közös teszt fixture-ök."""
import asyncio
import pytest
from decimal import Decimal
from exchange.models import SymbolInfo, PriceFilter, LotSizeFilter, NotionalFilter


@pytest.fixture
def symbol_info() -> SymbolInfo:
    """Tesztre optimalizált SOLUSDT szimbólum konfig."""
    return SymbolInfo(
        symbol="SOLUSDT",
        base_asset="SOL",
        quote_asset="USDT",
        price_filter=PriceFilter(
            min_price=Decimal("0.01"),
            max_price=Decimal("99999"),
            tick_size=Decimal("0.01"),
        ),
        lot_size=LotSizeFilter(
            min_qty=Decimal("0.001"),
            max_qty=Decimal("99999"),
            step_size=Decimal("0.001"),
        ),
        notional=NotionalFilter(
            min_notional=Decimal("1.0"),
        ),
    )
