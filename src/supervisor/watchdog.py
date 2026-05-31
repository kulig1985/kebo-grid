"""
Watchdog – rendszer egészségének monitorozása.

Triggerek:
- User stream staleness
- Trading WS staleness
- DB writer queue tele
- Ismétlődő order rejection
- Balance eltérés
"""
import asyncio
import time
from typing import Optional

from app.config import SafetyConfig
from app.log_setup import get_logger
from exchange.user_stream import UserDataStream
from exchange.ws_api import BinanceWsApi
from grid.engine import BotStatus, GridEngine
from persistence.writer import DbWriter
from supervisor.emergency import EmergencyStop

log = get_logger(__name__)

CHECK_INTERVAL = 5.0  # másodperc

# Binance WS limit-barát értékek:
# - 300 conn / 5 perc / IP → két force-close között legalább 60s
# - frissen csatlakozott connection-nek időt kell adni stabilizálódni
RECONNECT_WARMUP_SEC = 60       # új connection-nek ennyi mp-ig nem szabad force-close
FORCE_CLOSE_DEBOUNCE_SEC = 60   # két force-close közötti minimum idő
STABLE_RESET_SEC = 300          # ennyi mp folyamatos friss üzenet után reset a count


class Watchdog:
    def __init__(
        self,
        engine: GridEngine,
        ws_api: BinanceWsApi,
        user_stream: UserDataStream,
        db_writer: DbWriter,
        emergency: EmergencyStop,
        config: SafetyConfig,
    ):
        self.engine = engine
        self.ws_api = ws_api
        self.user_stream = user_stream
        self.db_writer = db_writer
        self.emergency = emergency
        self.config = config
        self._running = False
        self._rejection_count = 0
        self._ws_stale_warned = False
        self._stale_force_close_count = 0
        self._last_force_close_ts: float = 0.0
        self._last_stable_check_ts: float = 0.0
        # _last_msg_time amikor utoljára force-close-oltunk — csak akkor reset-eljük
        # a számlálót, ha az új connection-en VALÓDI új üzenet érkezett azóta.
        self._last_msg_time_at_force_close: float = 0.0

    async def run(self) -> None:
        self._running = True
        log.info("Watchdog elindult")
        while self._running:
            await asyncio.sleep(CHECK_INTERVAL)
            await self._check()

    async def stop(self) -> None:
        self._running = False

    async def _check(self) -> None:
        if self.engine.status in (BotStatus.EMERGENCY_STOPPING, BotStatus.EMERGENCY_STOPPED, BotStatus.STOPPED):
            return

        user_stream_age = self.user_stream.last_event_age_sec

        # User stream staleness – csak az első valódi esemény UTÁN ellenőriz
        if self.user_stream._first_event_received:
            if user_stream_age > self.config.max_user_stream_staleness_sec:
                await self.emergency.execute(
                    f"User stream stale: {user_stream_age:.1f}s > {self.config.max_user_stream_staleness_sec}s"
                )
                return

        # Trading WS: 5 perc stale (is_connected==True, de a Binance "fél-élő") → force reconnect.
        # Ha 3-szor egymás után kell, az emergency stop.
        ws_age = self.ws_api.last_msg_age_sec
        idle_limit = getattr(self.config, "max_trading_ws_idle_force_close_sec", 300)

        if not self.ws_api.is_connected:
            if not self._ws_stale_warned:
                log.warning("Trading WS disconnected — auto-reconnect várás")
                self._ws_stale_warned = True
        elif ws_age > idle_limit:
            now = time.monotonic()
            # Debounce: két force-close között legalább FORCE_CLOSE_DEBOUNCE_SEC mp
            if now - self._last_force_close_ts < FORCE_CLOSE_DEBOUNCE_SEC:
                return
            # Warm-up: ha a friss connection még nem volt elég ideig fent, ne számítson
            # force-close-nak (a Binance lezárhatja "1000 OK"-val tisztán is)
            if now - self.ws_api._connect_time < RECONNECT_WARMUP_SEC:
                log.debug(
                    "WS reconnect warm-up alatt — force-close kihagyva",
                    connect_age=f"{now - self.ws_api._connect_time:.0f}s",
                )
                return
            self._stale_force_close_count += 1
            self._last_force_close_ts = now
            self._last_stable_check_ts = now  # új force-close → stabilizálódási timer újraindul
            self._last_msg_time_at_force_close = self.ws_api._last_msg_time
            log.warning(
                "Trading WS stale — force reconnect",
                age_sec=f"{ws_age:.0f}s",
                attempt=self._stale_force_close_count,
            )
            await self.ws_api.force_reconnect("watchdog_stale")
            if self._stale_force_close_count >= 3:
                await self.emergency.execute(
                    f"Trading WS 3x stale ({idle_limit}s) — emergency stop"
                )
                return
        else:
            # Élő és friss üzenetek → warning törlése
            self._ws_stale_warned = False
            now = time.monotonic()
            # A stale-számláló csak akkor reset, ha a connection már elég ideje stabil.
            # Így a "1× stale → reconnect → 30s friss → megint stale" forgatókönyv
            # nem nullázza azonnal a számlálót, és tényleg eljut emergency-be ha kell.
            if self._stale_force_close_count > 0:
                # CSAK akkor számít stabilnak, ha az utolsó force-close ÓTA VALÓDI új
                # üzenet érkezett (különben csak a _connect() warm-up oszcillál).
                current_msg_time = self.ws_api._last_msg_time
                if current_msg_time <= self._last_msg_time_at_force_close:
                    # Nincs új üzenet az utolsó force-close óta — ne hamisan resetel.
                    return
                if self._last_stable_check_ts == 0:
                    self._last_stable_check_ts = now
                elif now - self._last_stable_check_ts > STABLE_RESET_SEC:
                    log.info(
                        "WS stabilan fut — stale számláló reset",
                        prev_count=self._stale_force_close_count,
                    )
                    self._stale_force_close_count = 0
                    self._last_stable_check_ts = 0.0

        # DB writer queue telítettség
        if self.config.emergency_stop_on_db_queue_full:
            if self.db_writer.queue_size > self.db_writer.max_queue_size * 0.9:
                await self.emergency.execute(
                    f"DB writer queue majdnem tele: {self.db_writer.queue_size}"
                )
                return

        # DB degraded állapot
        if self.db_writer.is_degraded:
            log.error("DB writer degraded – DB kapcsolat problémás")

    def record_rejection(self) -> None:
        """Order visszautasítás számlálása."""
        self._rejection_count += 1

    def reset_rejection_count(self) -> None:
        self._rejection_count = 0
