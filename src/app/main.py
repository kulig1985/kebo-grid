"""
Főprogram – összes async task indítása és koordinálása.

Task-ok:
1. trading_ws_writer_task
2. trading_ws_reader_task
3. user_stream_task
4. event_dispatcher_task
5. db_writer_task
6. reconciliation_task
7. watchdog_task
8. command_processor_task
9. api_server_task
10. broadcast_task (frontend WebSocket)
"""
import asyncio
import signal
import sys
import time
import uuid
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Optional

import uvicorn
from fastapi import FastAPI

from app.config import Settings, load_config
from app.log_setup import get_logger, setup_logging
from exchange.market_stream import MarketStream
from exchange.models import ExecutionReport, SymbolInfo
from exchange.user_stream import UserDataStream
from exchange.ws_api import BinanceWsApi, WsSendCommand
from grid.engine import GridEngine
from grid.state_machine import LocalOrderState, transition_from_execution_report
from persistence.db import close_db, init_db
from persistence.models import BotRun
from persistence.repositories import BotRunRepo, GridLevelRepo
from persistence.writer import DbEvent, DbWriter
from supervisor.emergency import EmergencyStop
from supervisor.reconciliation import Reconciliation
from supervisor.watchdog import Watchdog
from api.routes import init_routes, router as api_router
from api.live_events import broadcast_loop, set_broadcast_queue, websocket_events, publish_event

log = get_logger(__name__)

# Globális leállítás jelző
_shutdown = asyncio.Event()


def make_run_short_id(run_id: int) -> str:
    """6 karakteres base36 azonosító a bot run-hoz."""
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    n = run_id
    result = []
    for _ in range(6):
        result.append(chars[n % 36])
        n //= 36
    return "".join(reversed(result))


def parse_run_short_id(short: str) -> int:
    """short_id-ból visszaállítja a bot_run.id-t (make_run_short_id inverze)."""
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    n = 0
    for c in short:
        n = n * 36 + chars.index(c)
    return n


async def event_dispatcher(
    event_queue: asyncio.Queue,
    db_queue: asyncio.Queue[DbEvent],
    engine: GridEngine,
) -> None:
    """
    Esemény diszpécser – user data stream eseményeket fogad és irányít.

    DB írás SOHA nem itt, csak queue-ba tesz.
    """
    local_cancels: set[str] = set()

    log.info("Event dispatcher elindult")

    while True:
        try:
            data = await asyncio.wait_for(event_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        event_type = data.get("e")
        run_id = engine.bot_run_id or 0

        try:
            if event_type == "executionReport":
                await _handle_execution_report(data, db_queue, engine, run_id, local_cancels)

            elif event_type == "outboundAccountPosition":
                balances = data.get("B", [])
                engine.inventory.update_from_account_position(balances)
                for b in balances:
                    db_queue.put_nowait(DbEvent(
                        type="update_balance",
                        data={
                            "bot_run_id": run_id,
                            "asset": b["a"],
                            "free": Decimal(b["f"]),
                            "locked": Decimal(b["l"]),
                            "source": "user_stream",
                            "event_time": data.get("E"),
                        },
                    ))

            elif event_type == "balanceUpdate":
                asset = data["a"]
                delta = Decimal(data["d"])
                engine.inventory.update_from_balance_update(asset, delta)

            # Live broadcast
            publish_event({"type": event_type, "data": data})

        except Exception as e:
            log.error("Event dispatcher hiba", event_type=event_type, error=str(e))

        event_queue.task_done()


async def _handle_execution_report(
    data: dict,
    db_queue: asyncio.Queue,
    engine: GridEngine,
    bot_run_id: int,
    local_cancels: set[str],
) -> None:
    """executionReport feldolgozása."""
    try:
        report = ExecutionReport.from_dict(data)
    except Exception as e:
        log.error("ExecutionReport parse hiba", error=str(e), raw=data)
        return

    cid = report.client_order_id

    # Nem a bot által küldött order → csak naplózzuk, nem dolgozzuk fel
    if not cid.startswith("G-"):
        log.debug("Nem-bot order event kihagyva", cid=cid, status=report.order_status)
        db_queue.put_nowait(DbEvent(
            type="insert_execution_event",
            data={
                "bot_run_id": bot_run_id,
                "event_type": "executionReport",
                "execution_type": report.execution_type,
                "order_status": report.order_status,
                "client_order_id": cid,
                "exchange_order_id": report.order_id,
                "execution_id": report.execution_id,
                "trade_id": report.trade_id if report.trade_id >= 0 else None,
                "event_time": report.event_time,
                "transaction_time": report.transaction_time,
                "raw_json": data,
            },
        ))
        return

    has_local_cancel = cid in local_cancels
    # Emergency stop közben minden CANCELED a sajátunk
    if engine.status in ("EMERGENCY_STOPPING", "EMERGENCY_STOPPED") and report.execution_type == "CANCELED":
        has_local_cancel = True

    # Állapotgép átmenet
    current = LocalOrderState.SUBMITTED_UNKNOWN
    new_state, external = transition_from_execution_report(current, report, has_local_cancel)

    # DB queue: execution event (idempotens)
    db_queue.put_nowait(DbEvent(
        type="insert_execution_event",
        data={
            "bot_run_id": bot_run_id,
            "event_type": "executionReport",
            "execution_type": report.execution_type,
            "order_status": report.order_status,
            "client_order_id": cid,
            "exchange_order_id": report.order_id,
            "execution_id": report.execution_id,
            "trade_id": report.trade_id if report.trade_id >= 0 else None,
            "event_time": report.event_time,
            "transaction_time": report.transaction_time,
            "raw_json": data,
        },
    ))

    # DB queue: order frissítés
    db_queue.put_nowait(DbEvent(
        type="update_order_from_execution",
        data={
            "client_order_id": cid,
            "exchange_order_id": report.order_id,
            "status_exchange": report.order_status,
            "status_local": new_state.value,
            "executed_qty": report.cumulative_filled_qty,
            "cumulative_quote_qty": report.cumulative_quote_qty,
            "is_working": report.is_on_book,
            "reject_reason": report.reject_reason if report.reject_reason != "NONE" else None,
            "last_event_time": report.event_time,
            "created_exchange_time": report.order_creation_time,
        },
    ))

    # Fill mentés ha trade esemény
    if report.execution_type == "TRADE" and report.trade_id >= 0:
        db_queue.put_nowait(DbEvent(
            type="insert_fill",
            data={
                "bot_run_id": bot_run_id,
                "client_order_id": cid,
                "exchange_order_id": report.order_id,
                "trade_id": report.trade_id,
                "execution_id": report.execution_id,
                "symbol": report.symbol,
                "side": report.side,
                "price": report.last_executed_price,
                "quantity": report.last_executed_qty,
                "quote_quantity": report.last_quote_qty,
                "commission_amount": report.commission_amount,
                "commission_asset": report.commission_asset,
                "is_maker": report.is_maker,
                "transaction_time": report.transaction_time,
                "raw_event_json": data,
            },
        ))

    # Grid motor értesítése
    if report.order_status == "FILLED":
        await engine.on_execution_report(report)

    # Külső beavatkozás kezelése
    if external:
        await engine.on_external_cancel(report)
        db_queue.put_nowait(DbEvent(
            type="log_external_event",
            data={
                "bot_run_id": bot_run_id,
                "event_category": "EXTERNAL_CANCEL",
                "symbol": report.symbol,
                "client_order_id": cid,
                "exchange_order_id": report.order_id,
                "description": "Order törölve ismeretlen forrásból",
                "raw_json": data,
                "policy_action": engine.settings.safety.external_intervention_policy,
            },
        ))


async def command_processor(
    command_queue: asyncio.Queue,
    engine: GridEngine,
    emergency: EmergencyStop,
) -> None:
    """API parancsok feldolgozása."""
    while True:
        try:
            cmd = await asyncio.wait_for(command_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        log.info("Parancs fogadva", cmd=cmd.get("cmd"))
        match cmd.get("cmd"):
            case "pause":
                await engine.pause()
            case "resume":
                await engine.resume()
            case "stop":
                await engine.stop()
                _shutdown.set()
            case "emergency_stop":
                await emergency.execute(cmd.get("reason", "API parancs"))
            case _:
                log.warning("Ismeretlen parancs", cmd=cmd)

        command_queue.task_done()


async def run_migrations() -> None:
    """Alembic migrációk futtatása startup-kor – táblákat ez hozza létre."""
    from alembic.config import Config as AlembicConfig
    from alembic import command as alembic_command

    log.info("Adatbázis migrációk futtatása...")
    alembic_cfg = AlembicConfig("alembic.ini")
    loop = asyncio.get_event_loop()
    try:
        await asyncio.wait_for(
            loop.run_in_executor(None, lambda: alembic_command.upgrade(alembic_cfg, "head")),
            timeout=30.0,
        )
    except asyncio.TimeoutError:
        raise RuntimeError(
            "DB migráció timeout (30s)! Ellenőrizd a DB kapcsolatot.\n"
            "Docker konténerből NEM éred el a VPS külső IP-jét (hairpin NAT).\n"
            "Megoldás: lásd docker-compose.yml kommenteket (kebo-net hálózat)."
        )
    log.info("Adatbázis migrációk kész")


async def _try_recovery(
    engine: GridEngine,
    ws_api: BinanceWsApi,
    settings,
    bot_orders: list[dict],
    db_queue: asyncio.Queue,
) -> None:
    """Megpróbálja a korábbi run-t folytatni a bent ragadt orderekből."""
    from persistence.db import get_session

    first_cid = bot_orders[0]["clientOrderId"]
    parts = first_cid.split("-")
    if len(parts) < 2:
        log.warning("Érvénytelen bot order CID, recovery kihagyva", cid=first_cid)
        return

    run6 = parts[1]
    run_id = parse_run_short_id(run6)
    symbol = settings.bot.symbol
    log.info("Recovery jelölt", run_id=run_id, short_id=run6, open_bot_orders=len(bot_orders))

    async with get_session() as session:
        repo = BotRunRepo(session)
        bot_run = await repo.get(run_id)

        if bot_run is None:
            log.warning("Bot run nem található DB-ben, korábbi orderek törlése", run_id=run_id)
            ws_api.enqueue_cancel_all(symbol)
            return

        if bot_run.symbol != symbol:
            log.warning("Symbol mismatch, korábbi orderek törlése",
                        db_symbol=bot_run.symbol, config_symbol=symbol)
            ws_api.enqueue_cancel_all(symbol)
            return

        grid_level_repo = GridLevelRepo(session)
        grid_levels = await grid_level_repo.load_by_run(run_id)

        if not grid_levels:
            log.warning("Nincsenek grid szintek DB-ben, recovery nem lehetséges, orderek törlése")
            ws_api.enqueue_cancel_all(symbol)
            return

    # exchangeInfo lekérés (precision filterek kellenek)
    info_data = await ws_api.get_exchange_info(symbol)
    symbols = info_data.get("symbols", [])
    sym_data = next((s for s in symbols if s["symbol"] == symbol), None)
    if sym_data is None:
        log.error("Symbol nem található az exchangeInfo-ban", symbol=symbol)
        ws_api.enqueue_cancel_all(symbol)
        return
    symbol_info = SymbolInfo.from_exchange_info(sym_data)

    # Recovery végrehajtás
    await engine.recover(
        bot_run_id=run_id,
        bot_run_short_id=run6,
        bot_run_record=bot_run,
        grid_levels_db=grid_levels,
        open_orders=bot_orders,
        symbol_info=symbol_info,
    )

    # Bot run status frissítés DB-ben
    db_queue.put_nowait(DbEvent(
        type="update_bot_status",
        data={"run_id": run_id, "status": "RUNNING"},
    ))
    log.info("Recovery sikeres, bot fut", run_id=run_id)


async def main() -> None:
    # Konfig betöltés
    config_file = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    settings = load_config(config_file)

    setup_logging(settings.logging.level, settings.logging.json_format)
    log.info("Kebo Grid Bot indul", config=config_file)

    # DB inicializálás + auto migráció
    # (Időszinkron a WS kapcsolat felépülése után történik a ws_api.writer_loop()-ban)
    init_db(settings.database)
    await run_migrations()

    # Queue-k
    ws_send_queue: asyncio.Queue[WsSendCommand] = asyncio.Queue(maxsize=1000)
    event_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)
    db_queue: asyncio.Queue[DbEvent] = asyncio.Queue(maxsize=settings.database.writer_queue_max_size)
    command_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    broadcast_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

    set_broadcast_queue(broadcast_queue)

    # Komponensek
    ws_api = BinanceWsApi(settings.exchange, ws_send_queue, db_queue)
    user_stream = UserDataStream(
        settings.exchange,
        event_queue,
        on_reconnect=lambda: asyncio.get_event_loop().create_task(
            log.ainfo("User stream reconnect – reconciliation szükséges")
        ),
        ws_api=ws_api,  # WS API fallback ha REST /api/v3/userDataStream nem elérhető
    )
    market_stream = MarketStream(settings.exchange, settings.bot.symbol)
    engine = GridEngine(settings, ws_api, db_queue, event_queue, market_stream=market_stream)
    db_writer = DbWriter(db_queue, settings.database.writer_queue_max_size)
    emergency = EmergencyStop(engine, ws_api, db_queue, settings.safety)
    reconciliation = Reconciliation(engine, ws_api, db_queue, settings.safety)
    watchdog = Watchdog(engine, ws_api, user_stream, db_writer, emergency, settings.safety)

    # FastAPI app
    app = FastAPI(title="Kebo Grid Bot API")
    app.include_router(api_router)

    @app.websocket("/api/events")
    async def ws_events(websocket):
        await websocket_events(websocket)

    init_routes(engine, ws_api, user_stream, emergency, command_queue)

    # SIGINT/SIGTERM handler
    def handle_signal():
        log.warning("Leállítási jel fogadva")
        _shutdown.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    # Uvicorn konfig
    uv_config = uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="warning")
    uv_server = uvicorn.Server(uv_config)

    # Minden task indítása
    async with asyncio.TaskGroup() as tg:
        tg.create_task(ws_api.writer_loop(), name="ws_writer")
        tg.create_task(ws_api.reader_loop(), name="ws_reader")
        tg.create_task(user_stream.start(), name="user_stream")
        tg.create_task(market_stream.start(), name="market_stream")  # anchor price-hoz
        tg.create_task(db_writer.run(), name="db_writer")
        tg.create_task(event_dispatcher(event_queue, db_queue, engine), name="event_dispatcher")
        tg.create_task(command_processor(command_queue, engine, emergency), name="cmd_processor")
        tg.create_task(reconciliation.run(), name="reconciliation")
        tg.create_task(watchdog.run(), name="watchdog")
        tg.create_task(broadcast_loop(), name="broadcast")
        tg.create_task(uv_server.serve(), name="api_server")

        # Inicializáció: ELŐBB connection, AZTÁN recovery check, AZTÁN döntés
        log.info("Várakozás WS API és user stream csatlakozásra...")
        try:
            await asyncio.wait_for(ws_api._connected.wait(), timeout=30.0)
            log.info("WS API csatlakozva, várakozás user stream-re...")
            await asyncio.wait_for(user_stream.connected.wait(), timeout=30.0)
            log.info("User stream csatlakozva")
        except asyncio.TimeoutError:
            raise RuntimeError("WS API vagy user stream nem csatlakozott 30s alatt!")

        # Szerver idő szinkronizálás – KÖTELEZŐ az első authenticated hívás ELŐTT
        try:
            result = await ws_api._query("time", {}, authenticated=False)
            server_ms = result["serverTime"]
            local_ms = int(time.time() * 1000)
            offset = server_ms - local_ms
            from exchange.signing import set_time_offset
            set_time_offset(offset)
            if abs(offset) > 1000:
                log.warning("Rendszeróra eltérés korrigálva", offset_ms=offset)
            else:
                log.info("Szerver idő szinkronizálva", offset_ms=offset)
        except Exception as e:
            log.warning("Szerver idő szinkron sikertelen, folytatás", error=str(e))

        # Recovery check: van-e bent ragadt order az exchange-en?
        symbol = settings.bot.symbol
        log.info("Open orders lekérdezés...", symbol=symbol)
        existing_orders = await ws_api.get_open_orders(symbol)
        bot_orders = [o for o in existing_orders if o.get("clientOrderId", "").startswith("G-")]

        if bot_orders:
            await _try_recovery(engine, ws_api, settings, bot_orders, db_queue)

        if engine.status == "INITIALIZING":
            # Friss indítás: bot_run létrehozás + engine.initialize()
            from persistence.db import get_session
            async with get_session() as session:
                repo = BotRunRepo(session)
                run = await repo.create({
                    "symbol": settings.bot.symbol,
                    "base_asset": settings.bot.base_asset,
                    "quote_asset": settings.bot.quote_asset,
                    "status": "INITIALIZING",
                    "config_json": settings.bot.model_dump(mode="json"),
                    "grid_type": settings.bot.grid_type,
                    "total_capital_quote": settings.bot.total_capital_quote,
                    "order_quote_value": settings.bot.order_quote_value,
                    "target_net_profit_quote": settings.bot.target_net_profit_per_cycle_quote,
                })
                run_id = run.id
            short_id = make_run_short_id(run_id)
            log.info("Friss indítás: bot run létrehozva", run_id=run_id, short_id=short_id)
            tg.create_task(engine.initialize(run_id, short_id), name="engine_init")

        # Várakozás leállítási jelre
        await _shutdown.wait()
        log.info("Leállítás...")

        # Graceful shutdown: előbb orderek törlése, aztán kapcsolatok bontása
        await watchdog.stop()
        await reconciliation.stop()

        if engine.status not in ("EMERGENCY_STOPPING", "EMERGENCY_STOPPED"):
            log.info("Nyitott orderek törlése leállítás előtt", symbol=symbol)
            ws_api.enqueue_cancel_all(symbol)
            engine.status = "STOPPED"

            # Várunk a cancelAll végrehajtására (max 5s)
            for _ in range(10):
                await asyncio.sleep(0.5)
                try:
                    open_orders = await asyncio.wait_for(
                        ws_api.get_open_orders(symbol), timeout=3.0
                    )
                    if not open_orders:
                        log.info("Minden order törölve")
                        break
                    log.info("Várakozás order törlésre", remaining=len(open_orders))
                except Exception:
                    break
            else:
                log.warning("Nem sikerült az összes ordert törölni leállítás előtt")

        if engine.bot_run_id:
            db_queue.put_nowait(DbEvent(
                type="update_bot_status",
                data={"run_id": engine.bot_run_id, "status": "STOPPED"},
            ))
            await asyncio.sleep(0.5)

        await user_stream.stop()
        await market_stream.stop()
        await ws_api.stop()
        await db_writer.stop()
        uv_server.should_exit = True

    await close_db()
    log.info("Bot leállva")


if __name__ == "__main__":
    asyncio.run(main())
