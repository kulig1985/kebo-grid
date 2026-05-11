"""
Grid Pair Finder — USDC párok elemzése grid bot szempontból.

Standalone script. Bot-tól független. Csak public Binance REST API-t használ.
NEM backteszt — csak leíró stat + a config bot paramétereivel elméleti szimuláció.

Használat:
    python finder.py config.yaml
"""

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
USER_AGENT = "kebo-grid-finder/1.0"


# ────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg.setdefault("quote_asset", "USDC")
    cfg.setdefault("lookback_days", 30)
    cfg.setdefault("exclude_symbols", [])
    cfg.setdefault("min_volume_24h_quote", 50000)
    cfg.setdefault("top_n", 30)
    cfg.setdefault("output_dir", "output")
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


def compute_metrics(klines: list, ticker_24h: dict) -> dict:
    """Lookback napos OHLCV → leíró metrikák."""
    df = pd.DataFrame(klines, columns=KLINE_COLS)
    for c in ("open", "high", "low", "close", "volume", "quote_volume"):
        df[c] = df[c].astype(float)

    if len(df) < 5:
        raise ValueError(f"Túl kevés kline ({len(df)})")

    # Log returns + realized vol
    df["log_ret"] = np.log(df["close"] / df["close"].shift(1))
    realized_vol_daily = df["log_ret"].std()
    if math.isnan(realized_vol_daily):
        realized_vol_daily = 0.0

    # ATR (14)
    df["tr"] = np.maximum.reduce([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs(),
    ])
    atr_period = min(14, len(df))
    atr = df["tr"].rolling(atr_period).mean().iloc[-1]
    if math.isnan(atr):
        atr = df["tr"].mean()
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
        "realized_vol_annual": float(realized_vol_daily * math.sqrt(365)),
        "atr_pct": float(atr_pct),
        "range_pct_lookback": float(range_pct),
        "trend_strength": float(trend_strength),
        "swing_count": swing_count,
        "volume_24h_quote": float(ticker_24h.get("quoteVolume", 0)),
        "current_price": float(df["close"].iloc[-1]),
    }


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
# Score
# ────────────────────────────────────────────────────────────────

def grid_score(metrics: dict, sim: dict) -> float:
    """Kombinált rangsor — magas score = jó grid jelölt."""
    epd = sim["est_profit_per_day"]
    ranging = 1 - 0.5 * metrics["trend_strength"]
    liq = min(metrics["volume_24h_quote"] / 100_000, 5.0)
    k_factor = 1.0 if sim["K"] >= 3 else 0.3
    return float(epd * ranging * liq * k_factor)


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
            score = grid_score(metrics, sim)
            results.append({
                "symbol": row["symbol"],
                "base_asset": filters["base_asset"],
                **{k: v for k, v in filters.items() if k != "base_asset"},
                **metrics,
                **sim,
                "score": score,
            })
        df = pd.DataFrame(results).sort_values("score", ascending=False).reset_index(drop=True)
        # Új CSV (mert szimuláció új értékekkel jött)
        df.to_csv(csv_path, index=False)
        print(f"[+] {len(df)} symbol újraszámolva")
        generate_html_report(df, cfg, html_path)
        print(f"[+] HTML report: {html_path}")
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

        # 4. per-symbol klines + metrics + simulate
        print(f"[3/4] Per-symbol kline + metrika + szimuláció ({len(symbols)} symbol)...")
        results = []
        for i, s in enumerate(symbols, 1):
            sym = s["symbol"]
            print(f"      [{i}/{len(symbols)}] {sym:20s}", end="\r", flush=True)
            try:
                klines = fetch_klines(client, sym, "1d", lookback)
                if len(klines) < min(lookback - 2, 5):
                    continue
                filters = parse_symbol_filters(s)
                metrics = compute_metrics(klines, tickers[sym])
                sim = simulate_bot(metrics, filters, cfg["bot"])
                score = grid_score(metrics, sim)
                results.append({
                    "symbol": sym,
                    "base_asset": filters["base_asset"],
                    **{k: v for k, v in filters.items() if k != "base_asset"},
                    **metrics,
                    **sim,
                    "score": score,
                })
                # gyenge rate-limit elkerülés
                time.sleep(0.05)
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

def generate_html_report(df: pd.DataFrame, cfg: dict, out_path: Path) -> None:
    top_n = int(cfg["top_n"])
    top = df.head(top_n)

    # 1. Top N tábla
    # MinNotional színezés: zöld ha ≤1, sárga ha ≤5, narancs ha >5 (sűrűbb grid lehetséges-e)
    def _minnot_color(mn: float) -> str:
        if mn <= 1.0: return "#d4edda"   # zöld — sűrűbb grid lehet
        if mn <= 5.0: return "#fff3cd"   # sárga — standard
        return "#f8d7da"                 # piros — drága szintek

    minnot_colors = [_minnot_color(m) for m in top["min_notional"]]
    row_fills = ["#ecf0f1" if i % 2 == 0 else "white" for i in range(len(top))]

    table_fig = go.Figure(data=[go.Table(
        columnwidth=[28, 95, 35, 50, 55, 50, 55, 45, 70, 70, 55, 65, 65, 55],
        header=dict(
            values=["#", "Symbol", "K", "Step%", "Vol(d)%", "ATR%", "Range%",
                    "Trend", "Vol24h(k)", "<b>MinNot USDC</b>", "Cycle/d", "Profit/d", "Days→100", "Score"],
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
                (top["volume_24h_quote"] / 1000).round(0),
                top["min_notional"].round(2),
                top["est_cycles_per_day"].round(1),
                top["est_profit_per_day"].round(3),
                top["days_to_100_usdc"].round(0).fillna("∞") if "days_to_100_usdc" in top else "",
                top["score"].round(2),
            ],
            fill_color=[
                row_fills, row_fills, row_fills, row_fills, row_fills,
                row_fills, row_fills, row_fills, row_fills,
                minnot_colors,  # <-- min_notional színezve
                row_fills, row_fills, row_fills, row_fills,
            ],
            align="left", font=dict(size=11),
        ),
    )])
    table_fig.update_layout(
        title=f"Top {top_n} {cfg['quote_asset']} grid jelölt (score szerint) — <b>MinNot</b> oszlop "
              f"<span style='background:#d4edda'>zöld≤1</span> "
              f"<span style='background:#fff3cd'>sárga≤5</span> "
              f"<span style='background:#f8d7da'>piros>5</span>",
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
        ("Score", "Kombinált rangsor: <code>Profit/d × (1 − 0.5 × Trend) × min(Vol24h/100k, 5) × K_factor</code>. "
                   "K_factor = 1 ha K≥3, különben 0.3."),
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
