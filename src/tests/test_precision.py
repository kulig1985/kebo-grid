"""Decimal precíziós kerekítés tesztek."""
import pytest
from decimal import Decimal
from exchange.precision import round_down_to_step, round_price_to_tick, round_up_to_step


def test_round_down_to_step_alap():
    assert round_down_to_step(Decimal("1.234"), Decimal("0.01")) == Decimal("1.23")
    assert round_down_to_step(Decimal("0.0059"), Decimal("0.001")) == Decimal("0.005")
    assert round_down_to_step(Decimal("10.0"), Decimal("0.01")) == Decimal("10.0")


def test_round_down_to_step_pontosan_illeszkedik():
    assert round_down_to_step(Decimal("1.23"), Decimal("0.01")) == Decimal("1.23")


def test_round_down_to_step_lefelé():
    """Mindig lefelé kerekít (mennyiségekhez biztonságos)."""
    result = round_down_to_step(Decimal("0.00999"), Decimal("0.001"))
    assert result == Decimal("0.009")


def test_round_price_to_tick():
    assert round_price_to_tick(Decimal("80.005"), Decimal("0.01")) == Decimal("80.01")
    assert round_price_to_tick(Decimal("80.004"), Decimal("0.01")) == Decimal("80.00")
    assert round_price_to_tick(Decimal("79.995"), Decimal("0.01")) == Decimal("80.00")


def test_round_price_to_tick_egész():
    assert round_price_to_tick(Decimal("81.0"), Decimal("1")) == Decimal("81")


def test_kerekítés_invalid_step():
    with pytest.raises(ValueError):
        round_down_to_step(Decimal("1.0"), Decimal("0"))
    with pytest.raises(ValueError):
        round_price_to_tick(Decimal("1.0"), Decimal("-0.01"))


def test_qty_nem_negatív():
    """Kerekítés nem adhat negatív mennyiséget."""
    result = round_down_to_step(Decimal("0.0001"), Decimal("0.001"))
    assert result == Decimal("0")
