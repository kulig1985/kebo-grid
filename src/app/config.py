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
    recv_window_ms: int = 5000

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
    order_quote_value: Decimal = Decimal("5")
    target_net_profit_per_cycle_quote: Decimal = Decimal("0.02")
    grid_type: Literal["geometric", "arithmetic"] = "geometric"
    inventory_mode: Literal["prebalanced", "quote_only_bootstrap", "use_existing_balances"] = "prebalanced"
    buy_allocation_ratio: Decimal = Decimal("0.5")
    quote_reserve_pct: Decimal = Decimal("0.02")
    base_reserve_pct: Decimal = Decimal("0.02")
    order_type: Literal["LIMIT_MAKER", "LIMIT"] = "LIMIT_MAKER"
    time_in_force: str = "GTC"
    max_grid_levels: int = 20
    min_grid_step_pct: Decimal = Decimal("0.0025")
    max_grid_step_pct: Decimal = Decimal("0.05")
    buy_side_order_count: Optional[int] = None
    sell_side_order_count: Optional[int] = None
    external_intervention_policy: Literal["pause", "continue_reconcile", "emergency_stop"] = "pause"
    stop_policy: Literal["cancel_orders_only", "cancel_orders_and_optionally_liquidate_base"] = "cancel_orders_only"


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
    source: Literal["manual", "best_bid_ask_mid", "last_trade"] = "manual"
    manual_price: Optional[Decimal] = None


class SafetyConfig(BaseModel):
    external_intervention_policy: Literal["pause", "continue_reconcile", "emergency_stop"] = "pause"
    max_user_stream_staleness_sec: int = 10
    max_trading_ws_staleness_sec: int = 10
    cancel_retry_interval_sec: int = 2
    cancel_retry_max: int = 5
    emergency_stop_on_db_queue_full: bool = True
    emergency_stop_on_balance_mismatch: bool = True
    sell_on_emergency_stop: bool = False
    reconciliation_interval_sec: int = 60


class DatabaseConfig(BaseModel):
    host: str = "localhost"
    port: int = 5432
    name: str = "kebo_db"
    user: str = "kebo_grid"
    password_env: str = "DATABASE_PASSWORD"
    pool_min_size: int = 1
    pool_max_size: int = 10
    writer_queue_max_size: int = 10000

    @property
    def dsn(self) -> str:
        password = os.environ.get(self.password_env, "")
        return f"postgresql+asyncpg://{self.user}:{password}@{self.host}:{self.port}/{self.name}"

    @property
    def dsn_asyncpg(self) -> str:
        """DSN asyncpg közvetlen használathoz (SQLAlchemy nélkül)."""
        password = os.environ.get(self.password_env, "")
        return f"postgresql://{self.user}:{password}@{self.host}:{self.port}/{self.name}"


class LoggingConfig(BaseModel):
    level: str = "INFO"
    json: bool = True


class Settings(BaseModel):
    exchange: ExchangeConfig = ExchangeConfig()
    bot: BotConfig = BotConfig()
    fees: FeeConfig = FeeConfig()
    anchor: AnchorConfig = AnchorConfig()
    safety: SafetyConfig = SafetyConfig()
    database: DatabaseConfig = DatabaseConfig()
    logging: LoggingConfig = LoggingConfig()

    @model_validator(mode="after")
    def validate_anchor(self) -> "Settings":
        if self.anchor.source == "manual" and self.anchor.manual_price is None:
            raise ValueError("anchor.manual_price szükséges, ha source=manual")
        return self


def load_config(config_file: str = "config.yaml") -> Settings:
    """YAML konfig betöltése, env var-okkal kiegészítve."""
    with open(config_file) as f:
        data = yaml.safe_load(f)
    return Settings(**data)
