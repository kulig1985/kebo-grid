"""
Grid Pair Finder — USDC párok elemzése grid bot szempontból.

Standalone script. Bot-tól független. Csak public Binance REST API-t használ.
NEM backteszt — csak leíró stat + a config bot paramétereivel elméleti szimuláció.

Használat:
    python finder.py config.yaml
"""
from __future__ import annotations  # PEP 563: list[...] működjön Python 3.8 alatt is

import math
import sys
import time
from pathlib import Path
from typing import Optional

import httpx
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import yaml


REST_URL = "https://api.binance.com"
USER_AGENT = "kebo-grid-finder/2.0"

# Kline interval → percek (napi-ekvivalens normalizáláshoz, paginációhoz)
INTERVAL_MIN = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "8h": 480, "12h": 720,
    "1d": 1440, "3d": 4320, "1w": 10080,
}


# ────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg.setdefault("quote_asset", "USDC")
    cfg.setdefault("lookback_days", 30)
    cfg.setdefault("kline_interval", "15m")
    cfg.setdefault("exclude_symbols", [])
    cfg.setdefault("min_volume_24h_quote", 50000)
    cfg.setdefault("top_n", 30)
    cfg.setdefault("output_dir", "output")
    cfg.setdefault("anchor_simulations", 5)
    if cfg["kline_interval"] not in INTERVAL_MIN:
        raise ValueError(f"Ismeretlen kline_interval: {cfg['kline_interval']}. "
                         f"Engedélyezett: {list(INTERVAL_MIN.keys())}")
    bot = cfg.setdefault("bot", {})
    bot.setdefault("capital_quote", 55)
    bot.setdefault("quote_reserve_pct", 0.0)
    bot.setdefault("max_grid_levels", 20)
    bot.setdefault("buy_allocation_ratio", 0.5)
    bot.setdefault("target_profit_pct", 0.005)
    bot.setdefault("fee_buy", 0.00075)
    bot.setdefault("fee_sell", 0.00075)
    bot.setdefault("base_buffer_pct", 0.05)
    return cfg


# ────────────────────────────────────────────────────────────────
# REST fetch (public, nincs auth)
# ────────────────────────────────────────────────────────────────

def _client() -> httpx.Client:
    return httpx.Client(
        base_url=REST_URL,
        timeout=20.0,
        headers={"User-Agent": USER_AGENT},
    )


def fetch_exchange_info(client: httpx.Client) -> dict:
    r = client.get("/api/v3/exchangeInfo")
    r.raise_for_status()
    return r.json()


def fetch_ticker_24h(client: httpx.Client) -> list:
    r = client.get("/api/v3/ticker/24hr")
    r.raise_for_status()
    return r.json()


def fetch_klines(client: httpx.Client, symbol: str, interval: str = "1d", limit: int = 30) -> list:
    r = client.get(
        "/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
    )
    r.raise_for_status()
    return r.json()


def fetch_klines_paginated(
    client: httpx.Client, symbol: str, interval: str, lookback_days: int,
) -> list:
    """Lapozott kline lekérés — Binance limit 1000/hívás."""
    if interval not in INTERVAL_MIN:
        raise ValueError(f"Ismeretlen interval: {interval}")
    interval_ms = INTERVAL_MIN[interval] * 60 * 1000
    end_ts = int(time.time() * 1000)
    start_ts = end_ts - lookback_days * 86_400_000

    all_klines: list = []
    cur = start_ts
    safety_max = 50  # max 50 batch (50k kline) — sanity
    while cur < end_ts and safety_max > 0:
        r = client.get(
            "/api/v3/klines",
            params={"symbol": symbol, "interval": interval,
                    "startTime": cur, "limit": 1000},
        )
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        all_klines.extend(batch)
        last_close_time = batch[-1][6]
        if last_close_time <= cur:
            break
        cur = last_close_time + 1
        safety_max -= 1
        # batch < 1000 → minden lekérdezve
        if len(batch) < 1000:
            break
    return all_klines


# ────────────────────────────────────────────────────────────────
# Symbol filters parse
# ────────────────────────────────────────────────────────────────

def parse_symbol_filters(symbol_data: dict) -> dict:
    """exchangeInfo egy symbol-jából min_notional, tick_size, step_size kinyerése."""
    min_notional = 0.0
    tick_size = 0.0
    step_size = 0.0
    for f in symbol_data.get("filters", []):
        if f["filterType"] == "NOTIONAL":
            min_notional = float(f.get("minNotional", "0"))
        elif f["filterType"] == "MIN_NOTIONAL":  # legacy
            min_notional = float(f.get("minNotional", "0"))
        elif f["filterType"] == "PRICE_FILTER":
            tick_size = float(f.get("tickSize", "0"))
        elif f["filterType"] == "LOT_SIZE":
            step_size = float(f.get("stepSize", "0"))
    return {
        "min_notional": min_notional,
        "tick_size": tick_size,
        "step_size": step_size,
        "base_asset": symbol_data.get("baseAsset", ""),
    }


# ────────────────────────────────────────────────────────────────
# Metrics — leíró stat per-symbol
# ────────────────────────────────────────────────────────────────

KLINE_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades", "taker_buy_base", "taker_buy_quote", "ignore",
]


def _candles_to_df(klines: list) -> pd.DataFrame:
    df = pd.DataFrame(klines, columns=KLINE_COLS)
    for c in ("open", "high", "low", "close", "volume", "quote_volume"):
        df[c] = df[c].astype(float)
    df["close_time"] = df["close_time"].astype(int)
    return df


def _compute_atr(df: pd.DataFrame, period: int) -> pd.Series:
    tr = np.maximum.reduce([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs(),
    ])
    return pd.Series(tr).rolling(period).mean()


def compute_metrics(klines: list, ticker_24h: dict, interval: str = "1d") -> dict:
    """OHLCV → leíró metrikák. Interval-aware (15m, 1h, 1d, stb.)."""
    df = _candles_to_df(klines)

    if len(df) < 20:
        raise ValueError(f"Túl kevés kline ({len(df)}) — legalább 20 kell")

    periods_per_day = max(1, 1440 // INTERVAL_MIN[interval])

    # Log returns — per-candle
    df["log_ret"] = np.log(df["close"] / df["close"].shift(1))
    realized_vol_per_candle = df["log_ret"].std()
    if math.isnan(realized_vol_per_candle):
        realized_vol_per_candle = 0.0
    # Napi-ekvivalensre normalizálva (sqrt(N))
    realized_vol_daily = realized_vol_per_candle * math.sqrt(periods_per_day)

    # ATR (14 nap-ekvivalens)
    atr_period = min(14 * periods_per_day, max(2, len(df) // 4))
    atr_series = _compute_atr(df, atr_period)
    atr = atr_series.iloc[-1] if not math.isnan(atr_series.iloc[-1]) else float(atr_series.dropna().mean() or 0)
    mean_close = df["close"].mean()
    atr_pct = (atr / mean_close * 100) if mean_close > 0 else 0.0

    # Range %
    range_pct = ((df["high"].max() - df["low"].min()) / mean_close * 100) if mean_close > 0 else 0.0

    # Trend strength: lineáris regresszió R²
    x = np.arange(len(df), dtype=float)
    y = df["close"].values
    if len(x) >= 2 and y.std() > 0:
        slope, intercept = np.polyfit(x, y, 1)
        fit = slope * x + intercept
        ss_res = float(np.sum((y - fit) ** 2))
        ss_tot = float(np.sum((y - y.mean()) ** 2))
        trend_strength = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    else:
        trend_strength = 0.0
    trend_strength = max(0.0, min(1.0, trend_strength))

    # Swing count: hányszor változik az irány (close diff sign változás)
    sign = np.sign(df["close"].diff().dropna())
    if len(sign) > 1:
        swing_count = int(((sign != sign.shift(1)) & (sign != 0)).sum())
    else:
        swing_count = 0

    return {
        "realized_vol_daily": float(realized_vol_daily),
        "realized_vol_weekly": float(realized_vol_daily * math.sqrt(7)),
        "realized_vol_annual": float(realized_vol_daily * math.sqrt(365)),
        "atr_pct": float(atr_pct),
        "range_pct_lookback": float(range_pct),
        "trend_strength": float(trend_strength),
        "swing_count": swing_count,
        "volume_24h_quote": float(ticker_24h.get("quoteVolume", 0)),
        "current_price": float(df["close"].iloc[-1]),
    }


# ────────────────────────────────────────────────────────────────
# Hurst exponent (R/S analysis) — Mandelbrot 1972
# ────────────────────────────────────────────────────────────────

def compute_hurst_exponent(prices: np.ndarray, min_lag: int = 4, max_lag: int = 100) -> float:
    """
    Hurst exponent rescaled-range (R/S) analysis-szel.

    Visszaad: H ∈ [0, 1]
      - H < 0.5: mean-reverting (anti-persistent) — JÓ grid-nek
      - H ≈ 0.5: random walk
      - H > 0.5: trending (persistent) — ROSSZ grid-nek

    Forrás: Mandelbrot & Wallis (1969), Hurst (1951).
    """
    prices = np.asarray(prices, dtype=float)
    if len(prices) < 30:
        return 0.5

    # Log returns
    log_returns = np.diff(np.log(prices[prices > 0]))
    if len(log_returns) < min_lag * 2:
        return 0.5

    # Lags: kis és nagy időskálák között
    actual_max = min(max_lag, len(log_returns) // 4)
    if actual_max < min_lag + 2:
        return 0.5
    # Geometriai sorozat (kevesebb lag, gyorsabb)
    lags = np.unique(np.geomspace(min_lag, actual_max, num=10).astype(int))

    rs_values = []
    valid_lags = []
    for lag in lags:
        n_chunks = len(log_returns) // lag
        if n_chunks < 1:
            continue
        rs_per_chunk = []
        for i in range(n_chunks):
            chunk = log_returns[i * lag:(i + 1) * lag]
            mean = chunk.mean()
            dev = chunk - mean
            cumdev = np.cumsum(dev)
            R = cumdev.max() - cumdev.min()
            S = chunk.std()
            if S > 1e-12:
                rs_per_chunk.append(R / S)
        if rs_per_chunk:
            rs_values.append(float(np.mean(rs_per_chunk)))
            valid_lags.append(int(lag))

    if len(rs_values) < 4:
        return 0.5

    # log-log lineáris regresszió: log(R/S) = H × log(lag) + c
    log_lags = np.log(valid_lags)
    log_rs = np.log(rs_values)
    H, _ = np.polyfit(log_lags, log_rs, 1)
    return float(np.clip(H, 0.0, 1.0))


# ────────────────────────────────────────────────────────────────
# Volume Profile / POC anchor jelöltek
# ────────────────────────────────────────────────────────────────

def compute_volume_profile_anchors(klines: list, n_bins: int = 50, top_n: int = 5) -> list[dict]:
    """
    Volume-súlyozott price hisztogram → top N density-csúcs (POC jelöltek).
    A POC (Point of Control) az ipari standard: ahol a legtöbb forgalom volt.
    """
    df = _candles_to_df(klines)
    if len(df) < 5:
        return []
    # Typical price = (H+L+C)/3 — standard VP módszer
    tp = ((df["high"] + df["low"] + df["close"]) / 3).values
    weights = df["volume"].values
    if weights.sum() <= 0:
        weights = np.ones_like(weights)  # ha nincs volume, idő-súlyozás

    lo, hi = float(df["low"].min()), float(df["high"].max())
    if hi <= lo:
        return [{"price": float(df["close"].iloc[-1]), "density": 1.0, "volume_share": 1.0}]
    bin_edges = np.linspace(lo, hi, n_bins + 1)
    hist, _ = np.histogram(tp, bins=bin_edges, weights=weights)
    centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    total = hist.sum()
    if total <= 0:
        return []

    # Local maxima keresése (egyszerű)
    peaks = []
    for i in range(1, len(hist) - 1):
        if hist[i] > hist[i - 1] and hist[i] > hist[i + 1]:
            peaks.append({
                "price": float(centers[i]),
                "density": float(hist[i]),
                "volume_share": float(hist[i] / total),
            })
    # Ha nincs jó local maximum (monoton hist), az abszolút max-ot vesszük
    if not peaks:
        idx = int(np.argmax(hist))
        peaks.append({
            "price": float(centers[idx]),
            "density": float(hist[idx]),
            "volume_share": float(hist[idx] / total),
        })
    peaks.sort(key=lambda p: -p["density"])
    return peaks[:top_n]


# ────────────────────────────────────────────────────────────────
# Recency weight (ATR_recent / ATR_full arány)
# ────────────────────────────────────────────────────────────────

def compute_recency_weight(klines: list, interval: str, recent_days: int = 7) -> float:
    """ATR_recent / ATR_full arány — clipped 0.3..2.0.
    <0.7 = csillapodó vol (büntetés), >1.3 = növekvő vol (bónusz)."""
    df = _candles_to_df(klines)
    periods_per_day = max(1, 1440 // INTERVAL_MIN[interval])
    n_recent = recent_days * periods_per_day
    atr_period = min(14 * periods_per_day, max(2, len(df) // 4))

    if len(df) < n_recent + atr_period or len(df) < 50:
        return 1.0

    atr_full_series = _compute_atr(df, atr_period)
    atr_recent_series = _compute_atr(df.tail(n_recent).reset_index(drop=True),
                                      min(atr_period, max(2, n_recent // 4)))
    atr_full = atr_full_series.dropna().iloc[-1] if len(atr_full_series.dropna()) else 0
    atr_recent = atr_recent_series.dropna().iloc[-1] if len(atr_recent_series.dropna()) else 0

    if atr_full <= 0:
        return 1.0
    ratio = atr_recent / atr_full
    return float(np.clip(ratio, 0.3, 2.0))


# ────────────────────────────────────────────────────────────────
# Bot szimuláció (a config alapján)
# ────────────────────────────────────────────────────────────────

def simulate_bot(metrics: dict, filters: dict, bot_cfg: dict) -> dict:
    """
    A config bot paraméterekkel becsli:
    K (szintszám), step_pct, profit/cycle, est cycle/nap, est profit/nap.
    """
    capital = float(bot_cfg["capital_quote"])
    reserve = float(bot_cfg["quote_reserve_pct"])
    max_levels = int(bot_cfg["max_grid_levels"])
    target = float(bot_cfg["target_profit_pct"])
    fb = float(bot_cfg["fee_buy"])
    fs = float(bot_cfg["fee_sell"])
    buffer = float(bot_cfg["base_buffer_pct"])

    min_notional = max(filters.get("min_notional", 5.0), 1.0)
    available = capital * (1 - reserve)

    # V (per-line) — ugyanaz a logika mint az engine.initialize-ban
    per_level_auto = available / max_levels
    min_safe = min_notional * 1.1
    V = max(per_level_auto, min_safe)

    bootstrap_per_sell = V * (1 + buffer) / (1 - fb)
    cost_per_pair = V + bootstrap_per_sell
    K = int(available // cost_per_pair) if cost_per_pair > 0 else 0
    K = min(K, max_levels // 2)
    K = max(K, 0)

    # Step %
    step_decimal = (1 + target) / ((1 - fb) * (1 - fs)) - 1
    step_pct = step_decimal * 100

    # Sanity check: ha a step túl nagy, már nem reális
    profit_per_cycle = V * target

    # Heurisztikus cycle/nap: napi átlagos range osztva a step-pel
    # (1 napi mozgás nagyjából ennyi step-en megy keresztül oda-vissza)
    if step_pct > 0:
        est_cycles_per_day = max(0.0, metrics["atr_pct"] / step_pct)
    else:
        est_cycles_per_day = 0.0

    # Több párhuzamos szint közelítőleg additív
    est_profit_per_day = profit_per_cycle * est_cycles_per_day * K

    days_to_100 = 100 / est_profit_per_day if est_profit_per_day > 0 else float("inf")
    days_to_double_capital = capital / est_profit_per_day if est_profit_per_day > 0 else float("inf")

    return {
        "V": float(V),
        "K": int(K),
        "step_pct": float(step_pct),
        "profit_per_cycle": float(profit_per_cycle),
        "est_cycles_per_day": float(est_cycles_per_day),
        "est_profit_per_day": float(est_profit_per_day),
        "days_to_100_usdc": float(days_to_100) if days_to_100 != float("inf") else None,
        "days_to_double_capital": float(days_to_double_capital) if days_to_double_capital != float("inf") else None,
    }


# ────────────────────────────────────────────────────────────────
# Multi-anchor szimuláció
# ────────────────────────────────────────────────────────────────

def _empty_anchor_result() -> dict:
    return {
        "anchor_results": [], "hurst_exponent": 0.5,
        "median_profit": 0.0, "min_profit": 0.0, "max_profit": 0.0,
        "median_profit_per_day": 0.0, "min_profit_per_day": 0.0, "max_profit_per_day": 0.0,
        "median_out_of_range_pct": 0.0, "max_out_of_range_pct": 0.0,
        "median_crossings": 0.0, "best_anchor_price": 0.0,
        "vp_anchors": [],
    }


def simulate_anchor_v2(
    df: pd.DataFrame, anchor_price: float, sim: dict, interval: str,
) -> dict:
    """
    Egy anchor-ra: minden grid szintre megszámolja a candle-átmenetek számát.

    cycle = floor(crossings / 2) per szint (egy le-fel kéne legalább 2 metszéshez)
    """
    K = int(sim["K"])
    step = float(sim["step_pct"]) / 100
    profit_per_cycle = float(sim["profit_per_cycle"])
    periods_per_day = max(1, 1440 // INTERVAL_MIN[interval])

    if K <= 0 or step <= 0 or anchor_price <= 0 or len(df) == 0:
        return {"anchor_price": anchor_price, "total_crossings": 0, "total_cycles": 0,
                "profit": 0.0, "profit_per_day": 0.0, "out_of_range_pct": 0.0,
                "grid_low": anchor_price, "grid_high": anchor_price, "duration_days": 0.0,
                "hot_levels": []}

    # K BUY + K SELL szint anchor körül (geometric)
    levels = np.array(sorted(
        [anchor_price / ((1 + step) ** i) for i in range(1, K + 1)] +
        [anchor_price * ((1 + step) ** i) for i in range(1, K + 1)]
    ))
    grid_low = float(levels[0])
    grid_high = float(levels[-1])

    high = df["high"].values
    low = df["low"].values
    out_of_range = int(((high < grid_low) | (low > grid_high)).sum())

    # Per-szint crossings: vektorizált
    # crossings[i] = hány candle-ra igaz: low[c] <= levels[i] <= high[c]
    crossings = np.zeros(len(levels), dtype=int)
    for i, lv in enumerate(levels):
        crossings[i] = int(((low <= lv) & (lv <= high)).sum())

    total_crossings = int(crossings.sum())
    # Egy cycle = oda-vissza átkelés ≈ 2 metszés
    total_cycles = int((crossings // 2).sum())
    profit = total_cycles * profit_per_cycle

    duration_days = len(df) / periods_per_day
    profit_per_day = profit / duration_days if duration_days > 0 else 0.0
    out_pct = out_of_range / len(df) * 100 if len(df) > 0 else 0.0

    # Hot levels: top 3 legtöbbet metszett szint
    hot_idx = np.argsort(-crossings)[:3]
    hot_levels = [
        {"price": float(levels[i]), "crossings": int(crossings[i])}
        for i in hot_idx if crossings[i] > 0
    ]

    return {
        "anchor_price": float(anchor_price),
        "grid_low": grid_low, "grid_high": grid_high,
        "total_crossings": total_crossings,
        "total_cycles": total_cycles,
        "profit": float(profit),
        "profit_per_day": float(profit_per_day),
        "out_of_range_pct": float(out_pct),
        "duration_days": float(duration_days),
        "hot_levels": hot_levels,
    }


def simulate_anchors(
    klines: list, sim: dict, interval: str, n_anchors: int = 5,
) -> dict:
    """
    v3: Volume Profile (POC) alapú anchor jelöltek + per-szint crossings + Hurst exponent.

    Algoritmus:
    1. Top N POC anchor jelölt a Volume Profile-ből
    2. Mindegyikre simulate_anchor_v2 (per-szint crossings)
    3. Hurst exponent a teljes árra
    """
    df = _candles_to_df(klines)
    total = len(df)
    if total < 30 or sim["K"] <= 0 or sim["step_pct"] <= 0:
        return _empty_anchor_result()

    # 1. POC anchor jelöltek
    vp_anchors = compute_volume_profile_anchors(klines, n_bins=50, top_n=n_anchors)
    if not vp_anchors:
        # Fallback: egyenletesen elosztott anchor-ok az ár-tartományból
        prices = np.linspace(df["low"].min(), df["high"].max(), n_anchors)
        vp_anchors = [{"price": float(p), "density": 0, "volume_share": 0} for p in prices]

    # 2. Per-anchor szimuláció (per-szint crossings)
    results = []
    for vp in vp_anchors:
        r = simulate_anchor_v2(df, vp["price"], sim, interval)
        r["volume_share"] = vp.get("volume_share", 0)
        r["density"] = vp.get("density", 0)
        results.append(r)

    # 3. Hurst exponent (teljes close árra)
    hurst = compute_hurst_exponent(df["close"].values)

    # 4. Aggregátumok
    profits = [r["profit"] for r in results]
    profits_per_day = [r["profit_per_day"] for r in results]
    out_pcts = [r["out_of_range_pct"] for r in results]
    crossings_all = [r["total_crossings"] for r in results]
    best_idx = int(np.argmax(profits_per_day)) if profits_per_day else 0

    return {
        "anchor_results": results,
        "vp_anchors": vp_anchors,
        "hurst_exponent": hurst,
        "median_profit": float(np.median(profits)) if profits else 0.0,
        "min_profit": float(np.min(profits)) if profits else 0.0,
        "max_profit": float(np.max(profits)) if profits else 0.0,
        "median_profit_per_day": float(np.median(profits_per_day)) if profits_per_day else 0.0,
        "min_profit_per_day": float(np.min(profits_per_day)) if profits_per_day else 0.0,
        "max_profit_per_day": float(np.max(profits_per_day)) if profits_per_day else 0.0,
        "median_out_of_range_pct": float(np.median(out_pcts)) if out_pcts else 0.0,
        "max_out_of_range_pct": float(np.max(out_pcts)) if out_pcts else 0.0,
        "median_crossings": float(np.median(crossings_all)) if crossings_all else 0.0,
        "best_anchor_price": float(results[best_idx]["anchor_price"]) if results else 0.0,
    }


# ────────────────────────────────────────────────────────────────
# Score
# ────────────────────────────────────────────────────────────────

def grid_score(
    metrics: dict, sim: dict,
    anchor_sim: Optional[dict] = None, recency_weight: float = 1.0,
) -> float:
    """
    Kombinált rangsor — magas score = jó grid jelölt.

    Komponensek (v3):
    - epd_anchor: a multi-anchor szimuláció median profit/day-je (per-szint crossings)
    - out_penalty: range-out büntetés (max −70%)
    - recency_weight: ATR_recent / ATR_full (0.3..2.0 clipped)
    - hurst_factor: Hurst exponent alapú szorzó (mean-reverting bónusz, trending büntetés)
    """
    if anchor_sim and anchor_sim.get("median_profit_per_day", 0) > 0:
        epd = float(anchor_sim["median_profit_per_day"])
    else:
        epd = float(sim["est_profit_per_day"])

    out_pct = float(anchor_sim.get("median_out_of_range_pct", 0)) if anchor_sim else 0.0
    out_penalty = max(0.3, 1.0 - min(out_pct / 100, 0.7))

    # Hurst: <0.4 mean-reverting (jó), >0.6 trending (rossz)
    hurst = float(anchor_sim.get("hurst_exponent", 0.5)) if anchor_sim else 0.5
    if hurst < 0.4:
        hurst_factor = 1.3
    elif hurst > 0.6:
        hurst_factor = 0.6
    else:
        hurst_factor = 1.0

    ranging = 1 - 0.5 * metrics["trend_strength"]
    liq = min(metrics["volume_24h_quote"] / 100_000, 5.0)
    k_factor = 1.0 if sim["K"] >= 3 else 0.3
    return float(epd * ranging * liq * k_factor * out_penalty * recency_weight * hurst_factor)


# ────────────────────────────────────────────────────────────────
# Main pipeline
# ────────────────────────────────────────────────────────────────

def main(config_path: str, from_csv: bool = False) -> None:
    cfg = load_config(config_path)
    quote = cfg["quote_asset"]
    lookback = int(cfg["lookback_days"])
    out_dir = Path(cfg["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "data.csv"
    html_path = out_dir / "report.html"
    md_path = out_dir / "report.md"

    if from_csv:
        # Offline mód: a meglévő CSV-ből újra szimulál (új config paraméterekkel)
        if not csv_path.exists():
            print(f"[!] Nincs CSV: {csv_path}. Először futtass fetch módban (CSV nélkül).")
            return
        print(f"[+] Offline mód: {csv_path} alapján újraszámolás")
        print(f"    (új paraméterek: capital={cfg['bot']['capital_quote']}, "
              f"target_pct={cfg['bot']['target_profit_pct']}, fee={cfg['bot']['fee_buy']})")
        df_raw = pd.read_csv(csv_path)
        results = []
        for _, row in df_raw.iterrows():
            metrics = {
                "realized_vol_daily": row["realized_vol_daily"],
                "realized_vol_annual": row.get("realized_vol_annual", row["realized_vol_daily"] * math.sqrt(365)),
                "atr_pct": row["atr_pct"],
                "range_pct_lookback": row.get("range_pct_lookback", row.get("range_pct_30d", 0)),
                "trend_strength": row["trend_strength"],
                "swing_count": row.get("swing_count", 0),
                "volume_24h_quote": row["volume_24h_quote"],
                "current_price": row.get("current_price", 0),
            }
            filters = {
                "min_notional": row["min_notional"],
                "tick_size": row.get("tick_size", 0),
                "step_size": row.get("step_size", 0),
                "base_asset": row.get("base_asset", ""),
            }
            sim = simulate_bot(metrics, filters, cfg["bot"])
            # Anchor-szim adatok megőrzése a régi CSV-ből (ha vannak)
            anchor_sim = {
                "median_profit": row.get("anchor_median_profit", 0),
                "min_profit": row.get("anchor_min_profit", 0),
                "max_profit": row.get("anchor_max_profit", 0),
                "median_profit_per_day": row.get("anchor_median_profit_per_day", 0),
                "min_profit_per_day": row.get("anchor_min_profit_per_day", 0),
                "max_profit_per_day": row.get("anchor_max_profit_per_day", 0),
                "median_out_of_range_pct": row.get("out_of_range_pct", 0),
                "max_out_of_range_pct": row.get("max_out_of_range_pct", 0),
                "hurst_exponent": row.get("hurst_exponent", 0.5),
            } if "anchor_median_profit" in row else None
            recency_w = float(row.get("recency_weight", 1.0))
            score = grid_score(metrics, sim, anchor_sim=anchor_sim, recency_weight=recency_w)
            extras = {
                "anchor_median_profit": anchor_sim["median_profit"] if anchor_sim else 0,
                "anchor_min_profit": anchor_sim["min_profit"] if anchor_sim else 0,
                "anchor_max_profit": anchor_sim["max_profit"] if anchor_sim else 0,
                "anchor_median_profit_per_day": anchor_sim["median_profit_per_day"] if anchor_sim else 0,
                "anchor_min_profit_per_day": anchor_sim["min_profit_per_day"] if anchor_sim else 0,
                "anchor_max_profit_per_day": anchor_sim["max_profit_per_day"] if anchor_sim else 0,
                "out_of_range_pct": anchor_sim["median_out_of_range_pct"] if anchor_sim else 0,
                "max_out_of_range_pct": anchor_sim["max_out_of_range_pct"] if anchor_sim else 0,
                "recency_weight": recency_w,
                "hurst_exponent": row.get("hurst_exponent", 0.5),
                "best_anchor_price": row.get("best_anchor_price", 0),
                "median_crossings": row.get("median_crossings", 0),
                "vp_anchor_1_price": row.get("vp_anchor_1_price", 0),
                "vp_anchor_1_share": row.get("vp_anchor_1_share", 0),
                "vp_anchor_2_price": row.get("vp_anchor_2_price", 0),
                "vp_anchor_2_share": row.get("vp_anchor_2_share", 0),
                "vp_anchor_3_price": row.get("vp_anchor_3_price", 0),
                "vp_anchor_3_share": row.get("vp_anchor_3_share", 0),
                "interval": row.get("interval", cfg["kline_interval"]),
            }
            results.append({
                "symbol": row["symbol"],
                "base_asset": filters["base_asset"],
                **{k: v for k, v in filters.items() if k != "base_asset"},
                **metrics,
                **sim,
                **extras,
                "score": score,
            })
        df = pd.DataFrame(results).sort_values("score", ascending=False).reset_index(drop=True)
        # Új CSV (mert szimuláció új értékekkel jött)
        df.to_csv(csv_path, index=False)
        print(f"[+] {len(df)} symbol újraszámolva")
        generate_html_report(df, cfg, html_path)
        print(f"[+] HTML report: {html_path}")
        generate_md_report(df, cfg, md_path)
        print(f"[+] MD report:   {md_path}")
        generate_bot_configs(df, cfg, out_dir, top_n=5)
        _print_top5(df)
        return

    print(f"[+] Grid Pair Finder — quote={quote}, lookback={lookback}d, "
          f"capital={cfg['bot']['capital_quote']}, target_pct={cfg['bot']['target_profit_pct']}")

    with _client() as client:
        # 1. exchangeInfo
        print("[1/4] exchangeInfo lekérés...")
        info = fetch_exchange_info(client)
        symbols = [
            s for s in info["symbols"]
            if s["quoteAsset"] == quote
            and s["status"] == "TRADING"
            and s["symbol"] not in cfg["exclude_symbols"]
        ]
        print(f"      → {len(symbols)} {quote} TRADING symbol")

        # 2. 24h ticker minden symbolra
        print("[2/4] 24h ticker lekérés...")
        tickers_list = fetch_ticker_24h(client)
        tickers = {t["symbol"]: t for t in tickers_list}

        # 3. likviditás filter
        min_vol = float(cfg["min_volume_24h_quote"])
        symbols = [
            s for s in symbols
            if float(tickers.get(s["symbol"], {}).get("quoteVolume", 0)) >= min_vol
        ]
        print(f"      → {len(symbols)} symbol after liquidity filter ({min_vol:.0f} {quote}/24h)")

        # 4. per-symbol klines + metrics + simulate (interval-aware paginated)
        interval = cfg["kline_interval"]
        n_anchors = int(cfg["anchor_simulations"])
        periods_per_day = max(1, 1440 // INTERVAL_MIN[interval])
        expected_klines = lookback * periods_per_day
        print(f"[3/4] Per-symbol kline + metrika + szimuláció "
              f"({len(symbols)} symbol, interval={interval}, ~{expected_klines} kline/symbol)...")
        results = []
        for i, s in enumerate(symbols, 1):
            sym = s["symbol"]
            print(f"      [{i}/{len(symbols)}] {sym:20s}", end="\r", flush=True)
            try:
                klines = fetch_klines_paginated(client, sym, interval, lookback)
                if len(klines) < 20:
                    continue
                filters = parse_symbol_filters(s)
                metrics = compute_metrics(klines, tickers[sym], interval)
                sim = simulate_bot(metrics, filters, cfg["bot"])
                anchor_sim = simulate_anchors(klines, sim, interval, n_anchors)
                recency_w = compute_recency_weight(klines, interval, recent_days=7)
                score = grid_score(metrics, sim, anchor_sim=anchor_sim,
                                   recency_weight=recency_w)
                vp_top = anchor_sim.get("vp_anchors", [])
                extras = {
                    "interval": interval,
                    "hurst_exponent": anchor_sim.get("hurst_exponent", 0.5),
                    "best_anchor_price": anchor_sim.get("best_anchor_price", 0),
                    "median_crossings": anchor_sim.get("median_crossings", 0),
                    "anchor_median_profit": anchor_sim.get("median_profit", 0),
                    "anchor_min_profit": anchor_sim.get("min_profit", 0),
                    "anchor_max_profit": anchor_sim.get("max_profit", 0),
                    "anchor_median_profit_per_day": anchor_sim.get("median_profit_per_day", 0),
                    "anchor_min_profit_per_day": anchor_sim.get("min_profit_per_day", 0),
                    "anchor_max_profit_per_day": anchor_sim.get("max_profit_per_day", 0),
                    "out_of_range_pct": anchor_sim.get("median_out_of_range_pct", 0),
                    "max_out_of_range_pct": anchor_sim.get("max_out_of_range_pct", 0),
                    "recency_weight": recency_w,
                    # Top 3 POC anchor (price + share)
                    "vp_anchor_1_price": vp_top[0]["price"] if len(vp_top) >= 1 else 0,
                    "vp_anchor_1_share": vp_top[0]["volume_share"] if len(vp_top) >= 1 else 0,
                    "vp_anchor_2_price": vp_top[1]["price"] if len(vp_top) >= 2 else 0,
                    "vp_anchor_2_share": vp_top[1]["volume_share"] if len(vp_top) >= 2 else 0,
                    "vp_anchor_3_price": vp_top[2]["price"] if len(vp_top) >= 3 else 0,
                    "vp_anchor_3_share": vp_top[2]["volume_share"] if len(vp_top) >= 3 else 0,
                }
                results.append({
                    "symbol": sym,
                    "base_asset": filters["base_asset"],
                    **{k: v for k, v in filters.items() if k != "base_asset"},
                    **metrics,
                    **sim,
                    **extras,
                    "score": score,
                })
                # gyenge rate-limit elkerülés (paginated -> kicsi)
                time.sleep(0.02)
            except Exception as e:
                print(f"\n[!] {sym}: {type(e).__name__}: {e}")
        print()

    if not results:
        print("[!] Nincs eredmény — lehet hogy a likviditás filter túl szigorú vagy hálózati hiba.")
        return

    df = pd.DataFrame(results).sort_values("score", ascending=False).reset_index(drop=True)

    # 5. Output
    df.to_csv(csv_path, index=False)
    print(f"[4/4] CSV mentve: {csv_path}")
    generate_html_report(df, cfg, html_path)
    print(f"      HTML report: {html_path}")
    generate_md_report(df, cfg, md_path)
    print(f"      MD report:   {md_path}")
    generate_bot_configs(df, cfg, out_dir, top_n=5)
    _print_top5(df)


def _print_top5(df: pd.DataFrame) -> None:
    print(f"\n[+] Top 5 jelölt:")
    for i, row in df.head(5).iterrows():
        print(f"  {i+1:2d}. {row['symbol']:14s}  K={row['K']:>2}  "
              f"step={row['step_pct']:>5.2f}%  ATR={row['atr_pct']:>5.2f}%  "
              f"trend={row['trend_strength']:.2f}  "
              f"profit/d≈{row['est_profit_per_day']:>6.3f}  score={row['score']:.2f}")


# ────────────────────────────────────────────────────────────────
# Bot config generálás (top N párokra kész tiausdc-szerű YAML)
# ────────────────────────────────────────────────────────────────

def generate_bot_configs(df: pd.DataFrame, cfg: dict, out_dir: Path, top_n: int = 5) -> None:
    """A top N párokhoz külön bot config YAML-eket generál (tiausdc.yaml mintájára)."""
    cfg_dir = out_dir / "configs"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    quote = cfg["quote_asset"]
    bot = cfg["bot"]
    lookback = cfg["lookback_days"]

    for i, row in df.head(top_n).iterrows():
        sym = row["symbol"]
        # ASCII-only symbol (filename safety): ha unicode, skip
        if not sym.isascii():
            continue
        base = row["base_asset"] if isinstance(row["base_asset"], str) else sym.replace(quote, "")
        sym_lower = sym.lower()

        yaml_text = f"""# ============================================================================
#  {sym} ÉLES KONFIG — generated by grid_finder ({lookback} napi adatok alapján)
#
#  Finder eredmény (rang #{i+1}):
#    score: {row['score']:.2f}
#    ATR%: {row['atr_pct']:.2f}%, vol(napi)%: {row['realized_vol_daily']*100:.2f}%
#    trend (R²): {row['trend_strength']:.2f}, swing count: {row['swing_count']}
#    24h volume: {row['volume_24h_quote']:,.0f} {quote}
#    min_notional: {row['min_notional']:.4f} {quote}
#  Szimuláció:
#    K (BUY/SELL szintek): {int(row['K'])}+{int(row['K'])} = {2*int(row['K'])} összesen
#    V (per-line): {row['V']:.4f} {quote}
#    step: {row['step_pct']:.4f}%, profit/cycle: {row['profit_per_cycle']:.4f} {quote}
#    becsült cycles/d: {row['est_cycles_per_day']:.1f}
#    becsült profit/d: {row['est_profit_per_day']:.3f} {quote}
#    days → 100 {quote}: {int(row['days_to_100_usdc']) if row['days_to_100_usdc'] else '∞'}
# ============================================================================

exchange:
  env: mainnet
  api_key_env: BINANCE_API_KEY
  secret_key_env: BINANCE_SECRET_KEY
  recv_window_ms: 5000

bot:
  symbol: {sym}
  base_asset: {base}
  quote_asset: {quote}

  # Teljes tőke {quote}-ben.
  total_capital_quote: "{bot['capital_quote']}"

  # order_quote_value: NEM kell explicit megadni!
  # A bot az exchangeInfo min_notional ({row['min_notional']:.4f} {quote}) alapján auto-számolja.
  # A finder szimuláció V = {row['V']:.4f} {quote}-t várja.

  target_profit_pct: "{bot['target_profit_pct']}"

  grid_type: geometric
  inventory_mode: quote_only_bootstrap

  buy_allocation_ratio: "0.5"
  quote_reserve_pct: "{bot['quote_reserve_pct']}"
  base_reserve_pct: "0.0"

  order_type: LIMIT_MAKER
  time_in_force: GTC

  max_grid_levels: {bot['max_grid_levels']}
  max_grid_step_pct: "0.05"

  external_intervention_policy: pause
  stop_policy: cancel_orders_only

bootstrap:
  order_type: MARKET
  base_buffer_pct: "{bot['base_buffer_pct']}"
  limit_price_offset_pct: "0.001"

fees:
  fee_mode: maker_assumed
  maker_fee_buy: "{bot['fee_buy']}"
  maker_fee_sell: "{bot['fee_sell']}"
  taker_fee_buy: "{bot['fee_buy']}"
  taker_fee_sell: "{bot['fee_sell']}"

anchor:
  source: best_bid_ask_mid

safety:
  external_intervention_policy: pause
  max_user_stream_staleness_sec: 120
  max_trading_ws_staleness_sec: 30
  max_trading_ws_idle_force_close_sec: 300
  cancel_retry_interval_sec: 2
  cancel_retry_max: 5
  emergency_stop_on_db_queue_full: true
  emergency_stop_on_balance_mismatch: true
  sell_on_emergency_stop: false
  shutdown_action: cancel_and_sell
  reconciliation_interval_sec: 60
  profit_report_interval_sec: 600

database:
  pool_min_size: 1
  pool_max_size: 10
  writer_queue_max_size: 10000

logging:
  level: INFO
  format: rich

# ============================================================================
#  ELINDÍTÁS — másold be a docker-compose.yml-be:
#
#  bot-{sym_lower}:
#    <<: *bot-common
#    container_name: kebo-grid-{sym_lower}
#    volumes:
#      - ./configs/{sym_lower}.yaml:/app/config.yaml:ro
#      - ./logs/{sym_lower}:/app/logs
#
#  Aztán:
#    cp tools/grid_finder/output/configs/{sym_lower}.yaml configs/{sym_lower}.yaml
#    docker-compose up -d bot-{sym_lower}
# ============================================================================
"""
        out_path = cfg_dir / f"{sym_lower}.yaml"
        out_path.write_text(yaml_text, encoding="utf-8")

    print(f"[+] {min(top_n, len(df))} bot config generálva: {cfg_dir}/")


# ────────────────────────────────────────────────────────────────
# HTML report
# ────────────────────────────────────────────────────────────────

def _format_price(p) -> str:
    """Adaptív ár-formázás: kis árak több tizedessel."""
    try:
        p = float(p)
    except (TypeError, ValueError):
        return "—"
    if p == 0:
        return "0"
    if p < 0.001:
        return f"{p:.8f}".rstrip("0").rstrip(".")
    if p < 1:
        return f"{p:.6f}".rstrip("0").rstrip(".")
    if p < 100:
        return f"{p:.4f}"
    return f"{p:.2f}"


def _hurst_marker(h: float) -> str:
    if h < 0.4:
        return "🟢 mean-rev"
    if h > 0.6:
        return "🔴 trending"
    return "🟡 random"


def _minnot_marker(mn: float) -> str:
    if mn <= 1.0:
        return "🟢"
    if mn <= 5.0:
        return "🟡"
    return "🔴"


def _out_range_marker(p: float) -> str:
    if p <= 10:
        return "🟢"
    if p <= 30:
        return "🟡"
    return "🔴"


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    """Egyszerű markdown tábla — minden cella string."""
    head = "| " + " | ".join(headers) + " |"
    sep = "|" + "|".join("---" for _ in headers) + "|"
    body = "\n".join("| " + " | ".join(r) + " |" for r in rows)
    return f"{head}\n{sep}\n{body}"


def generate_md_report(df: pd.DataFrame, cfg: dict, out_path: Path) -> None:
    """Markdown riport — VPS-en (böngésző nélkül) is olvasható."""
    top_n = int(cfg["top_n"])
    top = df.head(top_n).reset_index(drop=True)
    bot = cfg["bot"]
    quote = cfg["quote_asset"]
    lookback = int(cfg["lookback_days"])

    has_hurst = "hurst_exponent" in top.columns
    has_anchor = "anchor_median_profit_per_day" in top.columns and (
        top["anchor_median_profit_per_day"].fillna(0).abs().sum() > 0
    )
    has_vp = "vp_anchor_1_price" in top.columns and (
        top["vp_anchor_1_price"].fillna(0).abs().sum() > 0
    )

    parts: list[str] = []
    parts.append("# 🎯 Grid Pair Finder Report\n")
    parts.append(
        f"**Quote:** `{quote}` | **Lookback:** {lookback} nap | "
        f"**Symbol-ok:** {len(df)} (likviditás ≥ {cfg['min_volume_24h_quote']:.0f})  \n"
        f"**Tőke:** {bot['capital_quote']} {quote} | "
        f"**target_profit_pct:** {bot['target_profit_pct']*100:.2f}% | "
        f"**fee:** {bot['fee_buy']*100:.3f}%/{bot['fee_sell']*100:.3f}% | "
        f"**buffer:** {bot['base_buffer_pct']*100:.0f}% | "
        f"**max_grid_levels:** {bot['max_grid_levels']}\n"
    )
    parts.append(
        "> ⚠ **FONTOS:** Ez NEM backteszt. A *becsült profit/nap* egy DURVA HEURISZTIKA — "
        "feltételezi hogy a napi átlagos ár-mozgás (ATR) hányszor megy keresztül a step-en, "
        "és minden szint párhuzamosan termel. A valódi profit függ a mozgás jellegétől, "
        "order book likviditástól, slippage-től. **A számok a relatív rangsoroláshoz használhatók, "
        "NEM abszolút garancia.**\n"
    )

    # ── 1. TOP N tábla ──────────────────────────────────────────────
    parts.append(f"\n## 📊 Top {top_n} jelölt\n")
    headers = ["#", "Symbol", "K", "Step%", "Vol(d)%", "ATR%", "Range%",
               "Trend(R²)", "Hurst", "Vol24h(k)", "MinNot", "Cycle/d",
               "Profit/d", "Days→100", "Score"]
    rows = []
    for i, row in top.iterrows():
        d100 = row.get("days_to_100_usdc", None)
        d100s = f"{int(d100)}" if d100 and not (isinstance(d100, float) and math.isnan(d100)) else "∞"
        hurst_v = row.get("hurst_exponent", 0.5) if has_hurst else 0.5
        hurst_cell = f"{hurst_v:.2f} {_hurst_marker(hurst_v)}" if has_hurst else "–"
        rows.append([
            str(i + 1),
            f"`{row['symbol']}`",
            str(int(row["K"])),
            f"{row['step_pct']:.2f}",
            f"{row['realized_vol_daily']*100:.2f}",
            f"{row['atr_pct']:.2f}",
            f"{row['range_pct_lookback']:.1f}",
            f"{row['trend_strength']:.2f}",
            hurst_cell,
            f"{row['volume_24h_quote']/1000:.0f}",
            f"{row['min_notional']:.2f} {_minnot_marker(row['min_notional'])}",
            f"{row['est_cycles_per_day']:.1f}",
            f"{row['est_profit_per_day']:.3f}",
            d100s,
            f"{row['score']:.2f}",
        ])
    parts.append(_md_table(headers, rows))
    parts.append(
        "\n> **Színkód:** Hurst — 🟢 <0.4 mean-rev (jó) · 🟡 0.4–0.6 random · 🔴 >0.6 trending (rossz) | "
        "MinNot — 🟢 ≤1 · 🟡 ≤5 · 🔴 >5\n"
    )

    # ── 2. Multi-anchor részletek ───────────────────────────────────
    if has_anchor:
        parts.append("\n## 🎯 Multi-anchor szimuláció (top 10)\n")
        parts.append(
            "Volume Profile (POC) alapú anchor jelöltek, mindegyikre per-szint crossings.  \n"
            "`cycles = floor(crossings/2)` per szint — pontosabb mint a globális `ATR/step` heurisztika.\n"
        )
        anc_n = min(10, len(top))
        atop = top.head(anc_n)
        ah = ["#", "Symbol", "Median P/d", "Min P/d", "Max P/d", "P/d Spread",
              "Out-Range%", "Recency"]
        ar = []
        for i, r in atop.iterrows():
            spread = float(r["anchor_max_profit_per_day"] - r["anchor_min_profit_per_day"])
            out_pct = float(r["out_of_range_pct"])
            rec = float(r.get("recency_weight", 1.0))
            rec_mark = "🔴" if rec < 0.7 else ("🟡" if rec < 0.9 else "🟢")
            ar.append([
                str(i + 1),
                f"`{r['symbol']}`",
                f"{r['anchor_median_profit_per_day']:.4f}",
                f"{r['anchor_min_profit_per_day']:.4f}",
                f"{r['anchor_max_profit_per_day']:.4f}",
                f"{spread:.4f}",
                f"{out_pct:.1f} {_out_range_marker(out_pct)}",
                f"{rec:.2f} {rec_mark}",
            ])
        parts.append(_md_table(ah, ar))
        parts.append(
            "\n> **Out-Range** — 🟢 ≤10% · 🟡 ≤30% · 🔴 >30% (range-ből kifutó pár) | "
            "**Recency** — 🔴 <0.7 csillapodó vol · 🟡 <0.9 · 🟢 ≥0.9 stabil/emelkedő\n"
        )

    # ── 3. Volume Profile (POC) anchor jelöltek ─────────────────────
    if has_vp:
        parts.append("\n## 📍 Volume Profile / POC anchor jelöltek (top 10)\n")
        parts.append(
            "*Best anchor* = a multi-anchor szim legmagasabb profit/d-eredménye. "
            "*POC #1-3* = top 3 volume-density csúcs (typical price hisztogram, "
            "volume-mal súlyozva).\n"
        )
        vp_n = min(10, len(top))
        vptop = top.head(vp_n)
        vh = ["#", "Symbol", "Current", "Best Anchor", "Δ% (best vs cur)",
              "POC #1", "Share #1", "POC #2", "Share #2", "POC #3"]
        vr = []
        for i, r in vptop.iterrows():
            cur = float(r["current_price"])
            best = float(r["best_anchor_price"])
            diff = (best - cur) / cur * 100 if cur > 0 and best > 0 else 0.0
            vr.append([
                str(i + 1),
                f"`{r['symbol']}`",
                _format_price(cur),
                _format_price(best),
                f"{diff:+.2f}%",
                _format_price(r["vp_anchor_1_price"]),
                f"{r['vp_anchor_1_share']*100:.1f}%",
                _format_price(r["vp_anchor_2_price"]),
                f"{r['vp_anchor_2_share']*100:.1f}%",
                _format_price(r["vp_anchor_3_price"]),
            ])
        parts.append(_md_table(vh, vr))

    # ── 4. Sensitivity heatmap (mint MD tábla) ──────────────────────
    target_pcts = [0.001, 0.002, 0.005, 0.01, 0.02, 0.04]
    sens_n = min(15, len(top))
    parts.append(f"\n## 🔥 Érzékenység: profit/nap különböző target_profit_pct mellett (top {sens_n})\n")
    sh = ["Symbol"] + [f"{tp*100:.1f}%" for tp in target_pcts]
    sr = []
    for _, srow in top.head(sens_n).iterrows():
        cells = [f"`{srow['symbol']}`"]
        for tp in target_pcts:
            cfg_alt = dict(bot)
            cfg_alt["target_profit_pct"] = tp
            sim = simulate_bot(
                {"atr_pct": float(srow["atr_pct"])},
                {"min_notional": float(srow["min_notional"])},
                cfg_alt,
            )
            cells.append(f"{sim['est_profit_per_day']:.3f}")
        sr.append(cells)
    parts.append(_md_table(sh, sr))

    # ── 5. Konkrét számítási példa a #1-re ──────────────────────────
    if len(df) > 0:
        parts.append("\n## 🧮 Konkrét számítási példa — #1 jelölt\n")
        parts.append(_build_example_md(df.iloc[0], cfg))

    # ── 6. Glosszárium ──────────────────────────────────────────────
    parts.append("\n## 📖 Oszlopok magyarázata\n")
    parts.append(_build_glossary_md(cfg))

    parts.append(
        "\n---\n"
        "*Offline újra-szimuláció (ha config változott — nem fetcheli újra):*  \n"
        "`./run.sh config.yaml --from-csv`\n\n"
        f"*Adatforrás: Binance public REST API · {cfg['kline_interval']} OHLCV × {lookback} nap.*\n"
    )

    out_path.write_text("\n".join(parts), encoding="utf-8")


def _build_example_md(top_row: pd.Series, cfg: dict) -> str:
    bot = cfg["bot"]
    sym = top_row["symbol"]
    min_not = float(top_row["min_notional"])
    capital = float(bot["capital_quote"])
    reserve = float(bot["quote_reserve_pct"])
    max_levels = int(bot["max_grid_levels"])
    fb = float(bot["fee_buy"])
    fs = float(bot["fee_sell"])
    buffer = float(bot["base_buffer_pct"])
    target = float(bot["target_profit_pct"])

    available = capital * (1 - reserve)
    per_level_auto = available / max_levels
    min_safe = min_not * 1.1
    V = max(per_level_auto, min_safe)
    bps = V * (1 + buffer) / (1 - fb)
    cost_pair = V + bps
    K = int(available // cost_pair)
    K = min(K, max_levels // 2)
    step = (1 + target) / ((1 - fb) * (1 - fs)) - 1
    profit_cycle = V * target
    atr = float(top_row["atr_pct"])
    cycles = atr / 100 / step if step > 0 else 0
    profit_day = profit_cycle * cycles * K

    return (
        f"**Pár:** `{sym}` | **min_notional:** {min_not:.4f} {cfg['quote_asset']} | "
        f"**{cfg['lookback_days']} napi ATR:** {atr:.2f}%\n\n"
        f"1. **Per-szint érték (V)**  \n"
        f"   `V = max(min_notional × 1.1, capital / max_grid_levels)`  \n"
        f"   `V = max({min_not:.4f} × 1.1, {capital} / {max_levels}) = max({min_safe:.4f}, {per_level_auto:.4f})`  \n"
        f"   **V = {V:.4f} {cfg['quote_asset']}**\n\n"
        f"2. **Bootstrap / sell szint**  \n"
        f"   `bootstrap_per_sell = V × (1 + buffer) / (1 - fee_buy)`  \n"
        f"   `= {V:.4f} × {1+buffer:.4f} / {1-fb:.5f}` → **{bps:.4f} {cfg['quote_asset']}**\n\n"
        f"3. **Pár-költség (1 BUY + 1 SELL)**  \n"
        f"   `cost_pair = V + bootstrap_per_sell = {V:.4f} + {bps:.4f}` → **{cost_pair:.4f} {cfg['quote_asset']}**\n\n"
        f"4. **Hány szimmetrikus pár (K)**  \n"
        f"   `available = {capital} × (1 - {reserve}) = {available:.2f}`  \n"
        f"   `K = floor({available:.2f} / {cost_pair:.4f})` → **K = {K}** ({K} BUY + {K} SELL = {2*K} szint)\n\n"
        f"5. **Step %**  \n"
        f"   `step = (1 + target) / ((1 - fee_buy) × (1 - fee_sell)) − 1`  \n"
        f"   `= (1 + {target}) / ((1 - {fb}) × (1 - {fs})) − 1` → **{step*100:.4f}%**\n\n"
        f"6. **Profit / cycle**  \n"
        f"   `profit_per_cycle = V × target = {V:.4f} × {target}` → **{profit_cycle:.4f} {cfg['quote_asset']}**\n\n"
        f"7. **Becsült cycle / nap (heuristic)**  \n"
        f"   `cycles/d ≈ ATR% / step% = {atr:.2f}% / {step*100:.4f}%` → **{cycles:.2f}**\n\n"
        f"8. **Becsült profit / nap**  \n"
        f"   `est_profit/d = profit_per_cycle × cycles/d × K = {profit_cycle:.4f} × {cycles:.2f} × {K}`  \n"
        f"   → **{profit_day:.4f} {cfg['quote_asset']}** (K szint párhuzamosan termel — optimista)\n\n"
        f"9. **Days → 100 {cfg['quote_asset']}**  \n"
        f"   `100 / {profit_day:.4f}` → **{100/profit_day:.0f} nap**\n\n"
        f"> ⚠ Ez egy *elméleti* profit potenciál. A valódi cycle szám függ attól, hogy a {cfg['lookback_days']} "
        f"napi ATR-mozgás **folyamatos oszcilláció** volt-e (jó grid-nek) vagy **egyirányú trend** (rossz). "
        f"Ezért a Trend (R²) is bele van számítva a final score-ba.\n"
    )


def _build_glossary_md(cfg: dict) -> str:
    lookback = int(cfg["lookback_days"])
    atr_period = min(14, lookback)
    quote = cfg["quote_asset"]
    rows = [
        ("Symbol", f"Binance symbol pl. `SOLUSDC`. baseAsset+quoteAsset."),
        ("K", "Hány BUY és hány SELL szint férne be a tőkébe (szimmetrikus, k_buy=k_sell=K). "
              "`K = floor(available / (V + bootstrap_per_sell))` ahol "
              "`V = max(min_notional × 1.1, capital / max_grid_levels)`. K<3 → score büntetve (×0.3)."),
        ("Step%", "Két szomszédos szint %-os ár-távolsága. "
                  "`step = (1 + target_profit_pct) / ((1 - fee_buy) × (1 - fee_sell)) − 1`."),
        ("Vol(d)%", f"Realizált napi volatilitás %-ban — {lookback} napi log-return-ek szórása."),
        ("ATR%", f"Average True Range {atr_period}d átlag, %-ban. "
                  "`ATR% = mean(TR) / mean_close × 100`."),
        ("Range%", f"`(max - min) / mean × 100` az elmúlt {lookback} napra."),
        ("Trend(R²)", f"Lineáris regresszió R² a {lookback} napi close-ra. 0=range (jó), 1=trend (rossz)."),
        ("Hurst", "R/S analysis (Mandelbrot). <0.4 mean-reverting (jó, ×1.3 bónusz), "
                  "0.4–0.6 random walk, >0.6 trending (rossz, ×0.6 büntetés)."),
        ("Vol24h(k)", f"Elmúlt 24h forgalom {quote}-ben, ezerben."),
        ("MinNot", f"Binance minNotional filter: legkisebb {quote}-értékű order. Meghatározza V alsó korlátját."),
        ("Cycle/d", "Becsült cycle/nap: `ATR% / step%`. Durva közelítés."),
        ("Profit/d", f"`profit_per_cycle × cycles/d × K`. Optimista (K szint párhuzamosan termel)."),
        ("Days→100", f"`100 / Profit/d` — hány nap kell 100 {quote} profithoz."),
        ("Score", "`EPD_anchor × (1 − 0.5×Trend) × min(Vol24h/100k, 5) × K_factor × out_penalty × recency × hurst_factor`. "
                   "EPD_anchor = multi-anchor median profit/d ha van, különben az ATR-alapú becslés."),
        ("Anchor median P/d", "Multi-anchor szimuláció — 5 POC-anchor-pozícióban szimulálva, median profit/nap. "
                                "A spread (max-min) mutatja a stabilitást."),
        ("Out-Range%", "A multi-anchor szim során hány %-a a candle-oknak volt teljesen KÍVÜL a grid sávján. "
                        ">30% → score büntetés."),
        ("Recency", "`ATR_recent_7d / ATR_full_lookback`, 0.3..2.0 clipped. "
                     "<0.7 csillapodó (büntetés), >1.3 emelkedő vol (bónusz)."),
        ("POC anchor", "Point of Control — Volume Profile legmagasabb density-pontja "
                        "(typical price hisztogram, volume-súlyozva)."),
    ]
    return _md_table(["Oszlop", "Magyarázat"], [[f"**{n}**", d] for n, d in rows])


def generate_html_report(df: pd.DataFrame, cfg: dict, out_path: Path) -> None:
    top_n = int(cfg["top_n"])
    top = df.head(top_n)

    # 1. Top N tábla
    # MinNotional színezés: zöld ha ≤1, sárga ha ≤5, narancs ha >5 (sűrűbb grid lehetséges-e)
    def _minnot_color(mn: float) -> str:
        if mn <= 1.0: return "#d4edda"   # zöld — sűrűbb grid lehet
        if mn <= 5.0: return "#fff3cd"   # sárga — standard
        return "#f8d7da"                 # piros — drága szintek

    def _hurst_color(h: float) -> str:
        if h < 0.4: return "#d4edda"   # zöld — mean-reverting (jó)
        if h > 0.6: return "#f8d7da"   # piros — trending (rossz)
        return "#fff3cd"               # sárga — random walk

    minnot_colors = [_minnot_color(m) for m in top["min_notional"]]
    hurst_colors = [_hurst_color(h) for h in top.get("hurst_exponent", pd.Series([0.5]*len(top)))]
    row_fills = ["#ecf0f1" if i % 2 == 0 else "white" for i in range(len(top))]
    has_hurst = "hurst_exponent" in top.columns

    table_fig = go.Figure(data=[go.Table(
        columnwidth=[28, 95, 35, 50, 55, 50, 55, 45, 50, 70, 70, 55, 65, 65, 55],
        header=dict(
            values=["#", "Symbol", "K", "Step%", "Vol(d)%", "ATR%", "Range%",
                    "Trend(R²)", "<b>Hurst</b>", "Vol24h(k)", "<b>MinNot USDC</b>",
                    "Cycle/d", "Profit/d", "Days→100", "Score"],
            fill_color="#2c3e50", font=dict(color="white", size=12), align="left",
        ),
        cells=dict(
            values=[
                list(range(1, len(top) + 1)),
                top["symbol"],
                top["K"],
                top["step_pct"].round(2),
                (top["realized_vol_daily"] * 100).round(2),
                top["atr_pct"].round(2),
                top["range_pct_lookback"].round(1),
                top["trend_strength"].round(2),
                top["hurst_exponent"].round(2) if has_hurst else ["–"]*len(top),
                (top["volume_24h_quote"] / 1000).round(0),
                top["min_notional"].round(2),
                top["est_cycles_per_day"].round(1),
                top["est_profit_per_day"].round(3),
                top["days_to_100_usdc"].round(0).fillna("∞") if "days_to_100_usdc" in top else "",
                top["score"].round(2),
            ],
            fill_color=[
                row_fills, row_fills, row_fills, row_fills, row_fills,
                row_fills, row_fills, row_fills,
                hurst_colors,    # <-- Hurst színezve
                row_fills,
                minnot_colors,   # <-- min_notional színezve
                row_fills, row_fills, row_fills, row_fills,
            ],
            align="left", font=dict(size=11),
        ),
    )])
    table_fig.update_layout(
        title=f"Top {top_n} {cfg['quote_asset']} grid jelölt (score szerint) — "
              f"<b>Hurst</b> <span style='background:#d4edda'>&lt;0.4 mean-rev</span> "
              f"<span style='background:#fff3cd'>0.4-0.6 random</span> "
              f"<span style='background:#f8d7da'>&gt;0.6 trending</span> | "
              f"<b>MinNot</b> <span style='background:#d4edda'>≤1</span> "
              f"<span style='background:#fff3cd'>≤5</span> "
              f"<span style='background:#f8d7da'>&gt;5</span>",
        height=min(900, 60 + 28 * len(top)),
    )

    # 2. Scatter: vol vs trend
    scatter = go.Figure()
    scatter.add_trace(go.Scatter(
        x=df["realized_vol_daily"] * 100,
        y=df["trend_strength"],
        mode="markers",
        marker=dict(
            size=np.clip(df["score"] * 0.5 + 6, 6, 30),
            color=df["est_profit_per_day"],
            colorscale="Viridis",
            showscale=True,
            colorbar=dict(title="Profit/nap"),
            line=dict(width=0.5, color="white"),
        ),
        text=[f"{s}<br>K={k}, step={st:.2f}%<br>score={sc:.2f}<br>profit/d={p:.3f}"
              for s, k, st, sc, p in zip(df["symbol"], df["K"], df["step_pct"],
                                          df["score"], df["est_profit_per_day"])],
        hovertemplate="%{text}<extra></extra>",
    ))
    scatter.update_layout(
        title="Volatilitás vs Trend Strength (méret=score, szín=profit/nap)",
        xaxis_title="Napi realizált volatilitás (%)",
        yaxis_title="Trend strength (R²) — 0=range, 1=trend",
        height=600,
    )

    # 3. Top N bar: profit/nap
    bar_n = min(20, len(top))
    bar_fig = go.Figure(go.Bar(
        x=top["symbol"].head(bar_n),
        y=top["est_profit_per_day"].head(bar_n),
        text=top["est_profit_per_day"].head(bar_n).round(3),
        textposition="outside",
        marker_color=top["score"].head(bar_n),
        marker=dict(colorscale="Viridis", showscale=True, colorbar=dict(title="Score")),
    ))
    bar_fig.update_layout(
        title=f"Top {bar_n} — becsült profit/nap ({cfg['quote_asset']})",
        xaxis_title="Symbol",
        yaxis_title=f"Becsült profit / nap ({cfg['quote_asset']})",
        height=500,
    )

    # 3.b Multi-anchor szimuláció — bar + errorbar (csak ha vannak anchor adatok)
    has_anchor = "anchor_median_profit_per_day" in top.columns and (
        top["anchor_median_profit_per_day"].fillna(0).abs().sum() > 0
    )
    if has_anchor:
        anc_n = min(15, len(top))
        anc_top = top.head(anc_n)
        median = anc_top["anchor_median_profit_per_day"]
        mn = anc_top["anchor_min_profit_per_day"]
        mx = anc_top["anchor_max_profit_per_day"]
        out_pct = anc_top["out_of_range_pct"]
        # Színkód: ha out_of_range > 30 → piros figyelmeztetés
        bar_colors = ["#e74c3c" if op > 30 else "#3498db" for op in out_pct]
        anchor_bar = go.Figure()
        anchor_bar.add_trace(go.Bar(
            x=anc_top["symbol"], y=median,
            error_y=dict(
                type="data",
                array=(mx - median).clip(lower=0),
                arrayminus=(median - mn).clip(lower=0),
                visible=True, color="#34495e", thickness=1.5,
            ),
            marker_color=bar_colors,
            text=[f"{v:.3f}" for v in median],
            textposition="outside",
            customdata=list(zip(out_pct, anc_top.get("recency_weight", [1.0]*len(anc_top)))),
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Median profit/d: %{y:.4f}<br>"
                "Out of range: %{customdata[0]:.1f}%<br>"
                "Recency weight: %{customdata[1]:.2f}<extra></extra>"
            ),
        ))
        anchor_bar.update_layout(
            title=f"Multi-anchor szimuláció (top {anc_n}) — median profit/nap, "
                  f"hibasáv = min/max különböző anchor-okból. "
                  f"<span style='color:#e74c3c'>Piros = >30% out-of-range</span>",
            xaxis_title="Symbol",
            yaxis_title=f"Profit / nap ({cfg['quote_asset']})",
            height=520,
        )
        anchor_bar_html = anchor_bar.to_html(full_html=False, include_plotlyjs=False)

        # Multi-anchor tábla: top 10 részletes
        anc_table_n = min(10, len(top))
        atop = top.head(anc_table_n)
        out_colors = [
            "#f8d7da" if v > 30 else ("#fff3cd" if v > 10 else "#d4edda")
            for v in atop["out_of_range_pct"]
        ]
        rec_colors = [
            "#f8d7da" if v < 0.7 else ("#fff3cd" if v < 0.9 else "#d4edda")
            for v in atop.get("recency_weight", [1.0]*len(atop))
        ]
        anchor_table = go.Figure(data=[go.Table(
            columnwidth=[35, 90, 70, 65, 65, 65, 70, 70],
            header=dict(
                values=["#", "Symbol", "Median P/d", "Min P/d", "Max P/d",
                        "P/d Spread", "Out-Range%", "Recency"],
                fill_color="#2c3e50", font=dict(color="white", size=12), align="left",
            ),
            cells=dict(
                values=[
                    list(range(1, anc_table_n + 1)),
                    atop["symbol"],
                    atop["anchor_median_profit_per_day"].round(4),
                    atop["anchor_min_profit_per_day"].round(4),
                    atop["anchor_max_profit_per_day"].round(4),
                    (atop["anchor_max_profit_per_day"] - atop["anchor_min_profit_per_day"]).round(4),
                    atop["out_of_range_pct"].round(1),
                    atop.get("recency_weight", pd.Series([1.0]*anc_table_n)).round(2),
                ],
                fill_color=[
                    ["white"]*anc_table_n, ["white"]*anc_table_n,
                    ["white"]*anc_table_n, ["white"]*anc_table_n, ["white"]*anc_table_n,
                    ["white"]*anc_table_n,
                    out_colors, rec_colors,
                ],
                align="left", font=dict(size=11),
            ),
        )])
        anchor_table.update_layout(
            title=f"Multi-anchor részletek (top {anc_table_n}) — "
                  f"<span style='background:#d4edda'>zöld out%≤10</span> "
                  f"<span style='background:#fff3cd'>sárga ≤30</span> "
                  f"<span style='background:#f8d7da'>piros >30</span>; "
                  f"recency: <0.7 csillapodó, >0.9 stabil",
            height=min(700, 60 + 28 * anc_table_n),
        )
        anchor_table_html = anchor_table.to_html(full_html=False, include_plotlyjs=False)
    else:
        anchor_bar_html = "<p><i>Nincs anchor szimuláció adat (régi CSV-ből futtatva offline módban).</i></p>"
        anchor_table_html = ""

    # 3.c Volume Profile (POC) anchor jelöltek táblázat
    has_vp = "vp_anchor_1_price" in top.columns and (top["vp_anchor_1_price"].fillna(0).abs().sum() > 0)
    if has_vp:
        vp_n = min(10, len(top))
        vp_top_df = top.head(vp_n)
        # POC vs current price diff %
        def _diff_pct(poc, cur):
            if cur <= 0 or poc <= 0:
                return 0.0
            return (poc - cur) / cur * 100
        diff_1 = [_diff_pct(p, c) for p, c in zip(vp_top_df["vp_anchor_1_price"], vp_top_df["current_price"])]
        diff_best = [_diff_pct(p, c) for p, c in zip(vp_top_df["best_anchor_price"], vp_top_df["current_price"])]
        vp_table = go.Figure(data=[go.Table(
            columnwidth=[35, 90, 80, 80, 70, 80, 70, 80, 70, 90],
            header=dict(
                values=["#", "Symbol", "Current Price", "Best Anchor",
                        "Δ% (best vs cur)", "POC #1", "Share #1",
                        "POC #2", "Share #2", "POC #3"],
                fill_color="#2c3e50", font=dict(color="white", size=12), align="left",
            ),
            cells=dict(
                values=[
                    list(range(1, vp_n + 1)),
                    vp_top_df["symbol"],
                    [_format_price(p) for p in vp_top_df["current_price"]],
                    [_format_price(p) for p in vp_top_df["best_anchor_price"]],
                    [f"{d:+.2f}%" for d in diff_best],
                    [_format_price(p) for p in vp_top_df["vp_anchor_1_price"]],
                    [f"{s*100:.1f}%" for s in vp_top_df["vp_anchor_1_share"]],
                    [_format_price(p) for p in vp_top_df["vp_anchor_2_price"]],
                    [f"{s*100:.1f}%" for s in vp_top_df["vp_anchor_2_share"]],
                    [_format_price(p) for p in vp_top_df["vp_anchor_3_price"]],
                ],
                align="left", font=dict(size=11),
            ),
        )])
        vp_table.update_layout(
            title=f"Volume Profile / POC anchor jelöltek (top {vp_n}) — "
                  f"<i>Best anchor</i> = a multi-anchor szim legmagasabb profit/d eredménye. "
                  f"<i>POC #1-3</i> = top 3 volume-density csúcs.",
            height=min(700, 60 + 28 * vp_n),
        )
        vp_table_html = vp_table.to_html(full_html=False, include_plotlyjs=False)
    else:
        vp_table_html = ""

    # 4. Sensitivity heatmap — különböző target_profit_pct
    target_pcts = [0.001, 0.002, 0.005, 0.01, 0.02, 0.04]
    sens_n = min(15, len(top))
    sens_top = top.head(sens_n)
    heatmap_data = []
    for sym_row in sens_top.itertuples():
        row = []
        for tp in target_pcts:
            cfg_alt = dict(cfg["bot"])
            cfg_alt["target_profit_pct"] = tp
            sim = simulate_bot(
                {"atr_pct": sym_row.atr_pct},
                {"min_notional": sym_row.min_notional},
                cfg_alt,
            )
            row.append(sim["est_profit_per_day"])
        heatmap_data.append(row)
    heatmap_fig = go.Figure(go.Heatmap(
        z=heatmap_data,
        x=[f"{tp*100:.1f}%" for tp in target_pcts],
        y=sens_top["symbol"].tolist(),
        colorscale="Viridis",
        text=[[f"{v:.3f}" for v in row] for row in heatmap_data],
        texttemplate="%{text}",
        colorbar=dict(title="Profit/d"),
    ))
    heatmap_fig.update_layout(
        title=f"Érzékenység: profit/nap különböző target_profit_pct mellett (top {sens_n})",
        xaxis_title="target_profit_pct",
        yaxis_title="Symbol",
        height=max(400, 28 * sens_n + 100),
    )

    # Glosszárium HTML (lookback-érzékeny)
    glossary_html = _build_glossary_html(cfg)

    # Konkrét számítási példa a top1 párra
    example_html = _build_example_html(df.iloc[0], cfg) if len(df) > 0 else ""

    # Összefűzés
    bot = cfg["bot"]
    html = f"""<!DOCTYPE html>
<html lang="hu">
<head>
<meta charset="utf-8">
<title>Grid Pair Finder Report</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         margin: 20px; max-width: 1400px; color: #2c3e50; }}
  h1 {{ color: #2c3e50; }}
  h2 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 6px; margin-top: 40px; }}
  .meta {{ background: #ecf0f1; padding: 12px 18px; border-radius: 6px;
           margin-bottom: 20px; font-size: 14px; line-height: 1.6; }}
  .meta b {{ color: #2c3e50; }}
  .section {{ margin-top: 30px; }}
  .glossary {{ background: #fefefe; border: 1px solid #bdc3c7; border-radius: 6px;
                padding: 16px 22px; margin-top: 20px; }}
  .glossary table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
  .glossary th {{ background: #34495e; color: white; padding: 8px 10px; text-align: left; }}
  .glossary td {{ padding: 8px 10px; border-bottom: 1px solid #ecf0f1; vertical-align: top; }}
  .glossary tr:hover td {{ background: #f4f6f7; }}
  .glossary code {{ background: #ecf0f1; padding: 1px 5px; border-radius: 3px;
                    font-family: "SF Mono", Menlo, monospace; font-size: 12px; }}
  .example {{ background: #eef9ff; border-left: 4px solid #3498db; padding: 16px 22px;
              margin-top: 20px; font-size: 14px; line-height: 1.7; }}
  .example .step {{ margin: 10px 0; }}
  .example .label {{ display: inline-block; min-width: 220px; color: #34495e; font-weight: 600; }}
  .example code {{ background: #d6e8ff; padding: 2px 6px; border-radius: 3px;
                   font-family: "SF Mono", Menlo, monospace; font-size: 13px; }}
  details {{ margin: 12px 0; }}
  summary {{ cursor: pointer; font-weight: 600; padding: 8px 12px; background: #ecf0f1;
              border-radius: 4px; }}
  summary:hover {{ background: #d6dbdf; }}
  .warn {{ background: #fff5e0; border-left: 4px solid #f39c12; padding: 14px 18px;
           margin-top: 16px; font-size: 13px; }}
  .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #bdc3c7;
             color: #7f8c8d; font-size: 12px; }}
</style>
</head>
<body>
<h1>🎯 Grid Pair Finder</h1>
<div class="meta">
  <b>Quote:</b> {cfg['quote_asset']} &nbsp; | &nbsp;
  <b>Lookback:</b> {cfg['lookback_days']} nap &nbsp; | &nbsp;
  <b>Symbol-ok:</b> {len(df)} (likviditás filter ≥ {cfg['min_volume_24h_quote']:.0f})<br>
  <b>Tőke:</b> {bot['capital_quote']} {cfg['quote_asset']} &nbsp; | &nbsp;
  <b>target_profit_pct:</b> {bot['target_profit_pct']*100:.2f}% &nbsp; | &nbsp;
  <b>fee:</b> {bot['fee_buy']*100:.3f}% / {bot['fee_sell']*100:.3f}% &nbsp; | &nbsp;
  <b>buffer:</b> {bot['base_buffer_pct']*100:.0f}% &nbsp; | &nbsp;
  <b>max_grid_levels:</b> {bot['max_grid_levels']}
</div>

<div class="warn">
  ⚠ <b>FONTOS:</b> Ez NEM backteszt. A "becsült profit/nap" egy DURVA HEURISZTIKA — feltételezi
  hogy a napi átlagos ár-mozgás (ATR) hányszor megy keresztül a step-en, és minden szint párhuzamosan termel.
  A valódi profit függ a mozgás jellegétől (squeeze vs trend), order book likviditásától, slippage-től.
  <b>A számok a relatív rangsoroláshoz használhatók, NEM abszolút garancia.</b>
</div>

<h2>📊 Top {top_n} jelölt</h2>
<div class="section">{table_fig.to_html(full_html=False, include_plotlyjs="cdn")}</div>

<h2>📖 Oszlopok magyarázata</h2>
{glossary_html}

<h2>🧮 Konkrét számítási példa — #1 jelölt</h2>
{example_html}

<h2>📈 Vizualizációk</h2>
<div class="section">{scatter.to_html(full_html=False, include_plotlyjs=False)}</div>
<div class="section">{bar_fig.to_html(full_html=False, include_plotlyjs=False)}</div>

<h2>🎯 Multi-anchor szimuláció (v3 — Volume Profile + per-szint crossings)</h2>
<p><b>Anchor jelöltek:</b> Volume Profile / POC alapján (top {cfg.get('anchor_simulations', 5)} legmagasabb forgalom-density csúcs).
<b>Cycle becslés:</b> minden grid szintre megszámoljuk hányszor metszett az ár (low ≤ szint ≤ high), majd <code>cycles = floor(crossings/2)</code> per szint. Ez sokkal pontosabb mint a v2 globális heurisztikája.
<b>Out of range</b> = hány candle volt teljesen kívül a grid sávján. <i>Mat. alap: empirikus eloszlás módusza (POC) + szint-átkelési statisztika.</i></p>
<div class="section">{anchor_bar_html}</div>
<div class="section">{anchor_table_html}</div>
<div class="section">{vp_table_html}</div>

<h2>🔥 Érzékenység vizsgálat</h2>
<p>Hogyan változik a becsült profit/nap különböző <code>target_profit_pct</code> érték mellett
(ugyanaz a tőke, ugyanaz a fee). Sárgább = nagyobb profit. <b>Figyeld meg:</b> a túl alacsony target
azért hozhat kevesebb profitot, mert <code>K</code> ugyanannyi marad de a profit/cycle kicsi;
a túl magas pedig azért, mert ritkább a cycle (kevesebb fill). Általában van egy optimum.</p>
<div class="section">{heatmap_fig.to_html(full_html=False, include_plotlyjs=False)}</div>

<div class="footer">
  <p><b>Offline újra-szimuláció:</b> ha módosítod a <code>config.yaml</code>-ban a bot paramétereket
  (capital, target_profit_pct, fee, buffer), futtasd: <code>python finder.py config.yaml --from-csv</code>
  — ez nem fetcheli újra a Binance-t (gyors), csak a meglévő <code>data.csv</code>-ből újra szimulál.</p>
  <p><b>Adat forrás:</b> Binance public REST API (no auth). 24h ticker + napi (1d) OHLCV gyertyák × {cfg['lookback_days']} nap.</p>
</div>
</body>
</html>
"""
    out_path.write_text(html, encoding="utf-8")


def _build_glossary_html(cfg: dict) -> str:
    """Részletes oszlop-magyarázat táblázat HTML-ben (lookback-érzékeny)."""
    lookback = int(cfg["lookback_days"])
    atr_period = min(14, lookback)
    quote = cfg["quote_asset"]
    rows = [
        ("#", "Rangsor pozíció (score szerint csökkenő)."),
        ("Symbol", "Binance symbol pl. <code>SOLUSDC</code>. <b>baseAsset</b>+<b>quoteAsset</b>."),
        ("K", "Hány BUY és hány SELL szint férne be a tőkébe (szimmetrikus, <b>k_buy = k_sell = K</b>). "
              "Képlet: <code>K = floor(available / (V + bootstrap_per_sell))</code> ahol "
              "<code>V = max(min_notional × 1.1, capital / max_grid_levels)</code> és "
              "<code>bootstrap_per_sell = V × (1 + buffer) / (1 - fee_buy)</code>. "
              "Ha K&lt;3 → score büntetve (0.3×)."),
        ("Step%", "Két szomszédos szint közötti %-os ár-távolság. "
                  "Képlet: <code>step = (1 + target_profit_pct) / ((1 - fee_buy) × (1 - fee_sell)) − 1</code>. "
                  "Csak a config <b>target_profit_pct</b>-től és <b>fee</b>-től függ, NEM a symbol-tól."),
        ("Vol(d)%", f"Realizált <b>napi volatilitás</b> %-ban. Az elmúlt <b>{lookback} napi</b> log-return-ek szórása. "
                    "Magas = ingadozó piac, jó grid-nek."),
        ("ATR%", f"<b>Average True Range</b> {atr_period} napos átlag, %-ban. A napi átlagos ár-mozgás amplitúdója. "
                  "Képlet: <code>TR = max(high-low, |high-close_prev|, |low-close_prev|)</code>, "
                  f"majd <code>ATR = mean(TR_{atr_period})</code>, és <code>ATR% = ATR / mean_close × 100</code>."),
        ("Range%", f"Az elmúlt <b>{lookback} nap</b> teljes <b>(max - min) / mean × 100</b> ár-tartománya. "
                   "Magas = nagyot mozdult a periódusban (lehet trend vagy nagy swing)."),
        ("Trend", f"Trend strength <b>R²</b>: lineáris regresszió a {lookback} napos close árakra. "
                   "<b>0 = tisztán range</b> (jó grid-nek), <b>1 = tisztán trend</b> (rossz grid-nek). "
                   "Score-ban: <code>(1 - 0.5 × trend)</code> szorzó."),
        ("Vol24h(k)", f"Elmúlt 24h forgalom kvótaeszközben ({quote}), ezerben. "
                       "Likviditás indikátor. <50k → előszűrve."),
        ("MinNot", f"Binance <b>minNotional</b> filter: az adott páron a legkisebb {quote} értékű order. "
                    f"5 {quote} nagy alt-coinokon, 1 {quote} kisebb token-eken. "
                    "Ez határozza meg a <b>V</b> alsó korlátját."),
        ("Cycle/d", "Becsült <b>cycle/nap</b>: <code>ATR% / step%</code>. "
                     "Heuristic: a napi ár-mozgás hányszor megy keresztül a step-en oda-vissza. "
                     "<b>Durva közelítés</b> — valódi piacon a mozgás nem egyenletes."),
        ("Profit/d", f"Becsült <b>profit/nap</b> {quote}-ben: <code>profit_per_cycle × cycle/d × K</code>. "
                      "Ahol <code>profit_per_cycle = V × target_profit_pct</code>. "
                      "Feltételezi hogy <b>K szint párhuzamosan termel</b> (optimista)."),
        ("Days→100", f"Hány nap kell <b>100 {quote} profit</b> eléréséhez: <code>100 / Profit/d</code>. "
                      "Ha túl magas → kis-tőkés bot strukturálisan nem éri el rövid távon."),
        ("Score", "Kombinált rangsor (v2): <code>EPD_anchor × (1 − 0.5×Trend) × min(Vol24h/100k, 5) × K_factor × out_penalty × recency</code>. "
                   "Ahol <code>EPD_anchor</code> = multi-anchor median profit/d (ha van), különben az ATR-alapú becslés."),
        ("Anchor median P/d", "Multi-anchor szimuláció eredménye: a múlt OHLCV-jéből 5 különböző anchor-pozícióban szimulálva, a median profit/nap. "
                                "A min/max tartja a szóródást — minél kisebb a szórás, annál stabilabb."),
        ("Out of range %", "A multi-anchor szimuláció során hány %-a a candle-oknak volt teljesen KÍVÜL a grid sávján "
                            "(ár leszakadt vagy kifelé szállt). Magas érték → range-ből kifutó pár, rossz grid. "
                            "&gt;30% → score büntetés."),
        ("Recency weight", "<code>ATR_recent_7d / ATR_full_lookback</code> arány, 0.3..2.0 clipped. "
                            "<b>1.0</b> = stabil; <b>&lt;0.7</b> = csillapodó vol (büntetés); "
                            "<b>&gt;1.3</b> = növekvő vol (bónusz)."),
        ("Hurst exponent", "Mandelbrot-féle <b>R/S analysis</b> (rescaled-range). Értelmezés: "
                            "<b>H&lt;0.5</b> = mean-reverting (anti-persistent, JÓ grid-nek), "
                            "<b>H≈0.5</b> = random walk, <b>H&gt;0.5</b> = trending (persistent, ROSSZ grid-nek). "
                            "Score: H&lt;0.4 → ×1.3 bónusz, H&gt;0.6 → ×0.6 büntetés. "
                            "Forrás: Mandelbrot &amp; Wallis (1969), Hurst (1951)."),
        ("POC anchor", "<b>Point of Control</b> — Volume Profile legmagasabb density-pontja. "
                        "A typical price (H+L+C)/3 hisztogramja, volume-mal súlyozva. Top 3 local maxima = "
                        "azok az árszintek, ahol az ár történetileg a legtöbbet \"polcolt\". "
                        "Mat. alap: empirikus eloszlás módusza."),
        ("Crossings (cycles)", "Per-szint <b>crossings count</b>: hányszor metszette át az ár az adott "
                                "grid szintet (low ≤ level ≤ high). <code>cycles = floor(crossings / 2)</code> "
                                "(egy le-fel ciklus = legalább 2 metszés). "
                                "Pontosabb mint a globális <code>ATR/step</code> heurisztika."),
    ]
    rows_html = "".join(
        f"<tr><td><b>{name}</b></td><td>{desc}</td></tr>" for name, desc in rows
    )
    return f"""
<details open>
  <summary>Mit jelent melyik oszlop? (kattints az összecsukáshoz)</summary>
  <div class="glossary">
    <table>
      <thead><tr><th style="width:120px">Oszlop</th><th>Magyarázat + képlet</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
</details>
"""


def _build_example_html(top_row: pd.Series, cfg: dict) -> str:
    """Konkrét számítási példa a #1 párra — minden lépés."""
    bot = cfg["bot"]
    sym = top_row["symbol"]
    min_not = float(top_row["min_notional"])
    capital = float(bot["capital_quote"])
    reserve = float(bot["quote_reserve_pct"])
    max_levels = int(bot["max_grid_levels"])
    fb = float(bot["fee_buy"])
    fs = float(bot["fee_sell"])
    buffer = float(bot["base_buffer_pct"])
    target = float(bot["target_profit_pct"])

    available = capital * (1 - reserve)
    per_level_auto = available / max_levels
    min_safe = min_not * 1.1
    V = max(per_level_auto, min_safe)
    bps = V * (1 + buffer) / (1 - fb)
    cost_pair = V + bps
    K = int(available // cost_pair)
    K = min(K, max_levels // 2)

    step = (1 + target) / ((1 - fb) * (1 - fs)) - 1
    profit_cycle = V * target
    atr = float(top_row["atr_pct"])
    cycles = atr / 100 / step if step > 0 else 0
    profit_day = profit_cycle * cycles * K

    return f"""
<div class="example">
<p><b>Pár:</b> <code>{sym}</code> &nbsp;|&nbsp; <b>min_notional:</b> {min_not:.4f} USDC
&nbsp;|&nbsp; <b>{cfg['lookback_days']} napi ATR:</b> {atr:.2f}%</p>

<div class="step"><span class="label">1. Per-szint érték (V):</span>
<code>V = max(min_notional × 1.1, capital / max_grid_levels)</code><br>
<code>V = max({min_not:.4f} × 1.1, {capital} / {max_levels}) = max({min_safe:.4f}, {per_level_auto:.4f}) = <b>{V:.4f} USDC</b></code>
</div>

<div class="step"><span class="label">2. Bootstrap / sell szint:</span>
<code>bootstrap_per_sell = V × (1 + buffer) / (1 - fee_buy)</code><br>
<code>= {V:.4f} × {1+buffer:.4f} / {1-fb:.5f} = <b>{bps:.4f} USDC</b></code> (egy sell szint indítási költsége)
</div>

<div class="step"><span class="label">3. Pár-költség (1 BUY + 1 SELL):</span>
<code>cost_pair = V + bootstrap_per_sell = {V:.4f} + {bps:.4f} = <b>{cost_pair:.4f} USDC</b></code>
</div>

<div class="step"><span class="label">4. Hány szimmetrikus pár (K):</span>
<code>available = {capital} × (1 - {reserve}) = {available:.2f}</code><br>
<code>K = floor({available:.2f} / {cost_pair:.4f}) = <b>{K}</b></code>
({K} BUY + {K} SELL = {2*K} szint összesen)
</div>

<div class="step"><span class="label">5. Step %:</span>
<code>step = (1 + target_pct) / ((1 - fee_buy) × (1 - fee_sell)) − 1</code><br>
<code>= (1 + {target}) / ((1 - {fb}) × (1 - {fs})) − 1 = <b>{step*100:.4f}%</b></code>
</div>

<div class="step"><span class="label">6. Profit / cycle:</span>
<code>profit_per_cycle = V × target_pct = {V:.4f} × {target} = <b>{profit_cycle:.4f} USDC</b></code>
(egy lezárt BUY-SELL pár nettó profitja, fee után)
</div>

<div class="step"><span class="label">7. Becsült cycle / nap (heuristic):</span>
<code>cycles/d ≈ ATR% / step% = {atr:.2f}% / {step*100:.4f}% = <b>{cycles:.2f}</b></code><br>
<i>(Durva közelítés — feltételezi hogy a napi átlagos ár-mozgás ennyi step-en megy keresztül)</i>
</div>

<div class="step"><span class="label">8. Becsült profit / nap:</span>
<code>est_profit/d = profit_per_cycle × cycles/d × K = {profit_cycle:.4f} × {cycles:.2f} × {K} = <b>{profit_day:.4f} USDC</b></code><br>
<i>(K szint párhuzamosan termel — optimista feltételezés)</i>
</div>

<div class="step"><span class="label">9. Days → 100 USDC:</span>
<code>100 / {profit_day:.4f} = <b>{100/profit_day:.0f} nap</b></code>
ha a piaci volatilitás állandó marad
</div>

<p style="margin-top: 16px; color: #c0392b;"><b>Kritikus észrevétel:</b> ez egy <i>elméleti</i> profit potenciál.
A valódi cycle szám függ attól, hogy a {cfg['lookback_days']} napi ATR-mozgás <b>folyamatos oszcilláció</b> volt-e
(jó grid-nek) vagy <b>egyirányú trend</b> (rossz grid-nek). Ezért a Trend (R²) érték is bele van számítva
a final score-ba.</p>
</div>
"""


if __name__ == "__main__":
    args = sys.argv[1:]
    from_csv = "--from-csv" in args
    args = [a for a in args if not a.startswith("--")]
    cfg_path = args[0] if args else "config.yaml"
    main(cfg_path, from_csv=from_csv)
