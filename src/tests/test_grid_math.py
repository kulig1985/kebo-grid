"""Grid matematika tesztek."""
import pytest
from decimal import Decimal
from grid.calculator import GridCalculator


@pytest.fixture
def calc() -> GridCalculator:
    return GridCalculator()


def test_geometriai_step_profit_cél(calc):
    """A kiszámított step fedezi a díjakat és a profitcélt."""
    V = Decimal("5")
    pi = Decimal("0.02")
    fb = Decimal("0.001")
    fs = Decimal("0.001")

    step = calc.compute_geometric_step(V, pi, fb, fs, Decimal("0.001"), Decimal("0.05"))
    r = 1 + step

    # Nettó profit ellenőrzés
    buy_cost = V * (1 + fb)
    sell_received = V * r * (1 - fs)
    net_profit = sell_received - buy_cost

    assert net_profit >= pi, f"Net profit {net_profit} < target {pi}"
    assert step > 0
    assert step <= Decimal("0.05")


def test_geometriai_step_csak_díj_fedezet(calc):
    """Ha nincs profitcél, legalább a díjakat fedezi."""
    step = calc.compute_geometric_step(
        Decimal("5"), Decimal("0"), Decimal("0.001"), Decimal("0.001"),
        Decimal("0.001"), Decimal("0.05"),
    )
    assert step >= Decimal("0.002")  # ~0.2% díj fedezet


def test_geometriai_step_min_korlát(calc):
    """Ha a szükséges step kisebb mint a minimum, a minimum érvényesül."""
    step = calc.compute_geometric_step(
        Decimal("100"), Decimal("0.001"), Decimal("0.0001"), Decimal("0.0001"),
        Decimal("0.01"), Decimal("0.05"),  # min = 1%
    )
    assert step >= Decimal("0.01")


def test_geometriai_step_max_korlát_hiba(calc):
    """Ha a szükséges step meghaladja a maximumot, ValueError keletkezik."""
    with pytest.raises(ValueError, match="meghaladja max_grid_step_pct"):
        calc.compute_geometric_step(
            Decimal("1"),     # nagyon kis order érték
            Decimal("10"),    # irreálisan nagy profitcél
            Decimal("0.01"),  # 1% díj
            Decimal("0.01"),
            Decimal("0.001"),
            Decimal("0.01"),  # max csak 1%
        )


def test_grid_szintek_száma(calc):
    """K_buy és K_sell helyes számítása."""
    k_buy, k_sell = calc.compute_grid_counts(
        total_capital_quote=Decimal("50"),
        order_quote_value=Decimal("5"),
        quote_reserve_pct=Decimal("0.02"),
        buy_allocation_ratio=Decimal("0.5"),
        max_grid_levels=20,
    )
    # 50 * 0.98 = 49 USDT elérhető, 49/5 = 9 szint
    assert k_buy + k_sell <= 9
    assert k_buy > 0
    assert k_sell > 0


def test_geometriai_szintek_generálás(calc, symbol_info):
    """Grid szintek helyes generálása és validálása."""
    plan = calc.generate_geometric_grid(
        anchor_price=Decimal("80"),
        grid_step_pct=Decimal("0.004"),
        k_buy=3,
        k_sell=3,
        order_quote_value=Decimal("5"),
        symbol_info=symbol_info,
    )

    assert len(plan.buy_levels) == 3
    assert len(plan.sell_levels) == 3

    # Buy szintek lentebb vannak az anchor-nál
    for lv in plan.buy_levels:
        assert lv.price < Decimal("80")
        assert lv.side == "BUY"
        assert lv.quantity > 0
        assert lv.notional >= Decimal("1")  # minNotional

    # Sell szintek fentebb vannak
    for lv in plan.sell_levels:
        assert lv.price > Decimal("80")
        assert lv.side == "SELL"

    # Szintek sorrendben vannak
    buy_prices = [lv.price for lv in plan.buy_levels]
    assert buy_prices == sorted(buy_prices, reverse=True)  # közelitől távoliig


def test_aritmetikai_step_számítás(calc):
    """Aritmetikai step fedezi a profitcélt a legrosszabb árnál."""
    V = Decimal("5")
    pi = Decimal("0.02")
    fb = Decimal("0.001")
    fs = Decimal("0.001")
    worst_price = Decimal("85")  # legmagasabb buy szint

    d = calc.compute_arithmetic_step(V, pi, fb, fs, worst_price, Decimal("0.001"), Decimal("0.05"))

    # Ellenőrzés: worst case-nél fedezi-e a profitot
    q = V / worst_price
    net = q * (worst_price + d) * (1 - fs) - q * worst_price * (1 + fb)
    assert net >= pi
