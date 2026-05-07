"""Konfiguráció betöltés és validálás."""
from decimal import Decimal
from typing import Literal, Optional
import os
import yaml
from pydantic import BaseModel, model_validator


class ExchangeConfig(BaseModel):
    env: Literal["mainnet", "testnet"] = "testnet"
    ws_api_url_mainnet: str = "wss://ws-api.binance.com:443/ws-api/v3"
    ws_api_url_testnet: str = "wss://ws-api.testnet.binance.vision/ws-api/v3"
    user_stream_url_mainnet: str = "wss://stream.binance.com:9443/ws"
    user_stream_url_testnet: str = "wss://stream.testnet.binance.vision:9443/ws"
    rest_url_mainnet: str = "https://api.binance.com"
    rest_url_testnet: str = "https://testnet.binance.vision"
    api_key_env: str = "BINANCE_API_KEY"
    secret_key_env: str = "BINANCE_SECRET_KEY"
    recv_window_ms: int = 10000  # 10s – a szerver idő szinkronizálás miatt növelve

    @property
    def ws_api_url(self) -> str:
        return self.ws_api_url_testnet if self.env == "testnet" else self.ws_api_url_mainnet

    @property
    def user_stream_url(self) -> str:
        return self.user_stream_url_testnet if self.env == "testnet" else self.user_stream_url_mainnet

    @property
    def rest_url(self) -> str:
        return self.rest_url_testnet if self.env == "testnet" else self.rest_url_mainnet

    @property
    def api_key(self) -> str:
        return os.environ.get(self.api_key_env, "")

    @property
    def secret_key(self) -> str:
        return os.environ.get(self.secret_key_env, "")


class BotConfig(BaseModel):
    symbol: str = "SOLUSDT"
    base_asset: str = "SOL"
    quote_asset: str = "USDT"

    total_capital_quote: Decimal = Decimal("50")
    # A bot által kezelt teljes tőke (USDT). Ez az egyetlen kötelező tőke-paraméter.
    # Ebből számítja a rendszer, hogy mennyi jut egy grid vonalra.

    order_quote_value: Optional[Decimal] = None
    # Egy grid vonal USDT értéke. Ha nem adod meg, a rendszer automatikusan kiszámolja:
    #   order_quote_value = (total_capital_quote × (1 - quote_reserve_pct)) / max_grid_levels
    # Ha megadod: validálja, hogy belefér a tőkébe és legalább 2 szint létrejöhet.

    target_profit_pct: Optional[Decimal] = None
    # Minimum NETTÓ profit EGY BUY-SELL CIKLUSON — az order_quote_value (egy grid vonal USDT
    # értéke) %-ában. NEM a total_capital-ra vetítve!
    #
    # Képlet: r = (1 + π) / ((1 - fee_buy) × (1 - fee_sell))
    # Profit / cycle ≈ order_quote_value × target_profit_pct
    #
    # PÉLDA: order_quote_value=5.5 USDT, target_profit_pct=0.005 (0.5%)
    #        → ciklus profit ≈ 5.5 × 0.005 = 0.0275 USDT (~2.75 cent / matched pair)
    #
    # ⚠ HASZNÁLD EZT VAGY a target_net_profit_per_cycle_quote-ot — egyszerre csak EGYIKET.

    target_net_profit_per_cycle_quote: Decimal = Decimal("0.02")
    # Minimum NETTÓ profit egy buy-sell körön — USDT-ben (abszolút). Default: 0.02 USDT/cycle.
    # Pl. 0.02 = 2 cent / ciklus minden szintre azonos.
    #
    # Képlet: r = (1 + fee_buy + profit_usdt / order_quote_value) / (1 - fee_sell)
    # Minden szinten ugyanaz az USDT profit, de a %-os hozam szintenként eltérő (lentebb nagyobb %).
    #
    # Ha target_profit_pct meg van adva, ez a mező ignorált.

    grid_type: Literal["geometric", "arithmetic"] = "geometric"
    inventory_mode: Literal["prebalanced", "quote_only_bootstrap", "use_existing_balances"] = "prebalanced"
    buy_allocation_ratio: Decimal = Decimal("0.5")
    quote_reserve_pct: Decimal = Decimal("0.02")
    base_reserve_pct: Decimal = Decimal("0.02")
    order_type: Literal["LIMIT_MAKER", "LIMIT"] = "LIMIT_MAKER"
    time_in_force: str = "GTC"
    max_grid_levels: int = 20
    max_grid_step_pct: Decimal = Decimal("0.05")
    # Sanity check: ha a számított lépés meghaladja ezt (5%), a bot HIBÁVAL leáll induláskor.
    # Védelem elgépelés ellen — pl. véletlenül 0.5 kerül 0.005 helyett a profit célba.

    buy_side_order_count: Optional[int] = None
    sell_side_order_count: Optional[int] = None
    external_intervention_policy: Literal["pause", "continue_reconcile", "emergency_stop"] = "pause"
    stop_policy: Literal["cancel_orders_only", "cancel_orders_and_optionally_liquidate_base"] = "cancel_orders_only"

    @model_validator(mode="after")
    def validate_target_profit(self) -> "BotConfig":
        """target_profit_pct VAGY target_net_profit_per_cycle_quote — pozitív érték."""
        pct = self.target_profit_pct
        quote = self.target_net_profit_per_cycle_quote
        if pct is not None and pct <= 0:
            raise ValueError("target_profit_pct pozitív kell legyen")
        if quote is not None and quote <= 0:
            raise ValueError("target_net_profit_per_cycle_quote pozitív kell legyen")
        return self

    @model_validator(mode="after")
    def validate_order_quote_value(self) -> "BotConfig":
        """
        Ha order_quote_value explicit meg van adva: validálja a konzisztenciát.
        Ha None: a motor számítja ki az exchangeInfo (min_notional) ismeretében.
        """
        if self.order_quote_value is None:
            # Helyes – engine.initialize() fogja beállítani a min_notional figyelembevételével
            return self

        available = self.total_capital_quote * (1 - self.quote_reserve_pct)
        if self.order_quote_value <= 0:
            raise ValueError("order_quote_value pozitív kell legyen")
        if self.order_quote_value > available:
            raise ValueError(
                f"order_quote_value ({self.order_quote_value}) meghaladja az elérhető "
                f"tőkét ({available} = {self.total_capital_quote} - {self.quote_reserve_pct*100}% tartalék)."
            )
        if self.order_quote_value > available / 2:
            raise ValueError(
                f"order_quote_value ({self.order_quote_value}) túl nagy – "
                f"legalább 2 szinthez kell elegendő tőke. Maximum: {available / 2}."
            )
        return self


class BootstrapConfig(BaseModel):
    """
    quote_only_bootstrap mód beállításai.
    Ha inventory_mode = quote_only_bootstrap: a bot először SOL-t vásárol,
    majd utána helyezi el a sell order-eket.
    """
    order_type: Literal["MARKET", "LIMIT", "LIMIT_MAKER"] = "MARKET"
    # MARKET: azonnali végrehajtás piaci áron (taker díj!)
    # LIMIT:  limit áron, offset-tel az anchor alá
    # LIMIT_MAKER: post-only, csak akkor tölt ha maker (lassabb de olcsóbb)

    quote_qty: Optional[Decimal] = None
    # Mennyi USDT-ért vásárol SOL-t a bootstrap lépésben.
    # None = automatikus számítás a sell oldali grid base-igényéből:
    #   required_base = sum(sell_level.qty)
    #   gross_base    = required_base × (1 + base_buffer_pct) / (1 - fee_buy)
    #   quote_qty     = gross_base × anchor_price
    # Csak annyit vesz, ami a sell grid orderekhez kell — nem keletkezik felesleges idle base.

    base_buffer_pct: Decimal = Decimal("0.05")
    # Biztonsági ráhagyás a kalkulált base-igény fölé (5% default).
    # Lefedi a market fill csúszást, fee-eltérést, kerekítést.

    limit_price_offset_pct: Decimal = Decimal("0.001")
    # LIMIT/LIMIT_MAKER bootstrap esetén: az anchor ár alá annyival
    # Pl. 0.001 = 0.1%-kal az aktuális ár alatt ad le LIMIT ordert


class FeeConfig(BaseModel):
    fee_mode: Literal["maker_assumed", "taker_assumed", "custom"] = "maker_assumed"
    maker_fee_buy: Decimal = Decimal("0.001")
    maker_fee_sell: Decimal = Decimal("0.001")
    taker_fee_buy: Decimal = Decimal("0.001")
    taker_fee_sell: Decimal = Decimal("0.001")

    @property
    def effective_buy_fee(self) -> Decimal:
        if self.fee_mode == "taker_assumed":
            return self.taker_fee_buy
        return self.maker_fee_buy

    @property
    def effective_sell_fee(self) -> Decimal:
        if self.fee_mode == "taker_assumed":
            return self.taker_fee_sell
        return self.maker_fee_sell


class AnchorConfig(BaseModel):
    source: Literal["manual", "best_bid_ask_mid", "last_trade"] = "best_bid_ask_mid"
    manual_price: Optional[Decimal] = None


class SafetyConfig(BaseModel):
    external_intervention_policy: Literal["pause", "continue_reconcile", "emergency_stop"] = "pause"
    max_user_stream_staleness_sec: int = 120
    max_trading_ws_staleness_sec: int = 30
    max_trading_ws_idle_force_close_sec: int = 300
    # 5 perc: ha a trading WS connected==True de ennyi ideje nincs üzenet,
    # a watchdog force close-olja → auto-reconnect. 3 egymás utáni → emergency stop.
    cancel_retry_interval_sec: int = 2
    cancel_retry_max: int = 5
    emergency_stop_on_db_queue_full: bool = True
    emergency_stop_on_balance_mismatch: bool = True
    sell_on_emergency_stop: bool = False
    shutdown_action: Literal["cancel_only", "cancel_and_sell", "force_exit"] = "cancel_only"
    # cancel_only      — minden ordert töröl, base bent marad
    # cancel_and_sell  — orderek + base MARKET sell, tiszta exit (REST fallback ha WS halott)
    # force_exit       — gyors kilépés, semmi cleanup (vész esetére)
    reconciliation_interval_sec: int = 60
    profit_report_interval_sec: int = 600


class DatabaseConfig(BaseModel):
    """
    DB kapcsolati adatok KIZÁRÓLAG az .env fájlból (env var-okból) jönnek:
      DB_HOST, DB_PORT, DB_NAME, DB_USER, DATABASE_PASSWORD

    YAML-ban csak a pool méreteket kell megadni.
    """
    pool_min_size: int = 1
    pool_max_size: int = 10
    writer_queue_max_size: int = 10000

    @property
    def host(self) -> str:
        val = os.environ.get("DB_HOST", "")
        if not val:
            raise RuntimeError("DB_HOST env var nincs beállítva! Állítsd be az .env fájlban.")
        return val

    @property
    def port(self) -> int:
        return int(os.environ.get("DB_PORT", "5432"))

    @property
    def name(self) -> str:
        return os.environ.get("DB_NAME", "kebo_db")

    @property
    def user(self) -> str:
        return os.environ.get("DB_USER", "kebo_grid")

    @property
    def password(self) -> str:
        val = os.environ.get("DATABASE_PASSWORD", "")
        if not val:
            raise RuntimeError("DATABASE_PASSWORD env var nincs beállítva! Állítsd be az .env fájlban.")
        return val

    @property
    def dsn(self) -> str:
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @property
    def dsn_asyncpg(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class LoggingConfig(BaseModel):
    level: str = "INFO"
    format: str = "rich"  # "json" | "console" | "rich"


class ApiConfig(BaseModel):
    bot_engine_url: str = "http://bot-solusdc:8080"


class Settings(BaseModel):
    exchange: ExchangeConfig = ExchangeConfig()
    bot: BotConfig = BotConfig()
    bootstrap: BootstrapConfig = BootstrapConfig()
    fees: FeeConfig = FeeConfig()
    anchor: AnchorConfig = AnchorConfig()
    safety: SafetyConfig = SafetyConfig()
    database: DatabaseConfig = DatabaseConfig()
    logging: LoggingConfig = LoggingConfig()
    api: ApiConfig = ApiConfig()

    @model_validator(mode="after")
    def validate_settings(self) -> "Settings":
        if self.anchor.source == "manual" and self.anchor.manual_price is None:
            raise ValueError("anchor.manual_price szükséges, ha source=manual")
        if self.bot.inventory_mode != "quote_only_bootstrap" and self.bootstrap.quote_qty is not None:
            pass  # figyelmen kívül hagyjuk ha nem bootstrap mód
        return self


def load_config(config_file: str = "config.yaml") -> Settings:
    """YAML konfig betöltése, env var-okkal kiegészítve."""
    with open(config_file) as f:
        data = yaml.safe_load(f)
    return Settings(**data)
