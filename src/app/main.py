"""
Főprogram – grid bot engine + minimális belső HTTP.

Az API külön service-ben fut (api_main.py).
A belső HTTP csak health check és parancs fogadásra szolgál.
"""
import asyncio
import signal
import sys
import time
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
from supervisor.profit_reporter import ProfitReporter
from supervisor.reconciliation import Reconciliation
from supervisor.watchdog import Watchdog

log = get_logger(__name__)

_shutdown = asyncio.Event()


def make_run_short_id(run_id: int) -> str:
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    n = run_id
    result = []
    for _ in range(6):
        result.append(chars[n % 36])
        n //= 36
    return "".join(reversed(result))


def parse_run_short_id(short: str) -> int:
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
    try:
        report = ExecutionReport.from_dict(data)
    except Exception as e:
        log.error("ExecutionReport parse hiba", error=str(e), raw=data)
        return

    cid = report.client_order_id

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
    if engine.status in ("EMERGENCY_STOPPING", "EMERGENCY_STOPPED") and report.execution_type == "CANCELED":
        has_local_cancel = True

    current = LocalOrderState.SUBMITTED_UNKNOWN
    new_state, external = transition_from_execution_report(current, report, has_local_cancel)

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

    if report.order_status == "FILLED":
        await engine.on_execution_report(report)

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

        from sqlalchemy import select, func
        from persistence.models import Order
        max_pair_seq = 0
        result = await session.execute(
            select(func.max(Order.pair_id)).where(
                Order.bot_run_id == run_id,
                Order.pair_id.isnot(None),
            )
        )
        max_pair_str = result.scalar_one_or_none()
        if max_pair_str and max_pair_str.startswith("P-"):
            try:
                max_pair_seq = int(max_pair_str[2:])
            except ValueError:
                max_pair_seq = 0

    info_data = await ws_api.get_exchange_info(symbol)
    symbols = info_data.get("symbols", [])
    sym_data = next((s for s in symbols if s["symbol"] == symbol), None)
    if sym_data is None:
        log.error("Symbol nem található az exchangeInfo-ban", symbol=symbol)
        ws_api.enqueue_cancel_all(symbol)
        return
    symbol_info = SymbolInfo.from_exchange_info(sym_data)

    await engine.recover(
        bot_run_id=run_id,
        bot_run_short_id=run6,
        bot_run_record=bot_run,
        grid_levels_db=grid_levels,
        open_orders=bot_orders,
        symbol_info=symbol_info,
        max_pair_seq=max_pair_seq,
    )

    db_queue.put_nowait(DbEvent(
        type="update_bot_status",
        data={"run_id": run_id, "status": "RUNNING"},
    ))
    log.info("Recovery sikeres, bot fut", run_id=run_id)


def _create_internal_app(
    engine: GridEngine,
    ws_api: BinanceWsApi,
    user_stream: UserDataStream,
    command_queue: asyncio.Queue,
) -> FastAPI:
    """Minimális belső HTTP — health + status + cmd."""
    app = FastAPI(title="Kebo Grid Internal", docs_url=None, redoc_url=None)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/internal/status")
    async def internal_status():
        plan = engine.grid_plan
        grid_info = {}
        if plan:
            grid_info = {
                "buy": plan.k_buy,
                "sell": plan.k_sell,
                "low_price": str(plan.grid_low_price) if plan.grid_low_price else None,
                "high_price": str(plan.grid_high_price) if plan.grid_high_price else None,
            }
        return {
            "bot_status": engine.status,
            "bot_run_id": engine.bot_run_id,
            "symbol": engine.settings.bot.symbol,
            "open_orders": len(engine._level_orders),
            "grid": grid_info,
            "uptime_sec": time.monotonic() - _boot_time,
            "ws": {
                "trading_connected": ws_api.is_connected,
                "trading_last_msg_age": ws_api.last_msg_age_sec,
                "user_stream_last_event_age": user_stream.last_event_age_sec,
                "reconnects_trading": ws_api._reconnect_count,
                "reconnects_user_stream": user_stream._reconnect_count,
            },
        }

    @app.post("/internal/cmd")
    async def internal_cmd(body: dict):
        command_queue.put_nowait(body)
        return {"accepted": True}

    return app


_boot_time = time.monotonic()


async def main() -> None:
    global _boot_time
    _boot_time = time.monotonic()

    config_file = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    settings = load_config(config_file)

    setup_logging(settings.logging.level, settings.logging.format)
    log.info("Kebo Grid Bot indul", config=config_file)

    init_db(settings.database)
    await run_migrations()

    # Queue-k
    ws_send_queue: asyncio.Queue[WsSendCommand] = asyncio.Queue(maxsize=1000)
    event_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)
    db_queue: asyncio.Queue[DbEvent] = asyncio.Queue(maxsize=settings.database.writer_queue_max_size)
    command_queue: asyncio.Queue = asyncio.Queue(maxsize=100)

    # Komponensek
    ws_api = BinanceWsApi(settings.exchange, ws_send_queue, db_queue)
    user_stream = UserDataStream(
        settings.exchange,
        event_queue,
        on_reconnect=lambda: asyncio.get_event_loop().create_task(
            log.ainfo("User stream reconnect – reconciliation szükséges")
        ),
        ws_api=ws_api,
    )
    market_stream = MarketStream(settings.exchange, settings.bot.symbol)
    engine = GridEngine(settings, ws_api, db_queue, event_queue, market_stream=market_stream)
    db_writer = DbWriter(db_queue, settings.database.writer_queue_max_size)
    emergency = EmergencyStop(engine, ws_api, db_queue, settings.safety)
    reconciliation = Reconciliation(engine, ws_api, db_queue, settings.safety)
    watchdog = Watchdog(engine, ws_api, user_stream, db_writer, emergency, settings.safety)
    profit_reporter = ProfitReporter(engine, settings.safety.profit_report_interval_sec)

    # Belső HTTP (health + status + cmd)
    internal_app = _create_internal_app(engine, ws_api, user_stream, command_queue)

    # SIGINT/SIGTERM handler
    def handle_signal():
        log.warning("Leállítási jel fogadva")
        _shutdown.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    uv_config = uvicorn.Config(internal_app, host="0.0.0.0", port=8080, log_level="warning")
    uv_server = uvicorn.Server(uv_config)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(ws_api.writer_loop(), name="ws_writer")
        tg.create_task(ws_api.reader_loop(), name="ws_reader")
        tg.create_task(user_stream.start(), name="user_stream")
        tg.create_task(market_stream.start(), name="market_stream")
        tg.create_task(db_writer.run(), name="db_writer")
        tg.create_task(event_dispatcher(event_queue, db_queue, engine), name="event_dispatcher")
        tg.create_task(command_processor(command_queue, engine, emergency), name="cmd_processor")
        tg.create_task(reconciliation.run(), name="reconciliation")
        tg.create_task(watchdog.run(), name="watchdog")
        tg.create_task(profit_reporter.run(), name="profit_reporter")
        tg.create_task(uv_server.serve(), name="internal_http")

        log.info("Várakozás WS API és user stream csatlakozásra...")
        try:
            await asyncio.wait_for(ws_api._connected.wait(), timeout=30.0)
            log.info("WS API csatlakozva, várakozás user stream-re...")
            await asyncio.wait_for(user_stream.connected.wait(), timeout=30.0)
            log.info("User stream csatlakozva")
        except asyncio.TimeoutError:
            raise RuntimeError("WS API vagy user stream nem csatlakozott 30s alatt!")

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

        symbol = settings.bot.symbol
        log.info("Open orders lekérdezés...", symbol=symbol)
        existing_orders = await ws_api.get_open_orders(symbol)
        bot_orders = [o for o in existing_orders if o.get("clientOrderId", "").startswith("G-")]

        if bot_orders:
            await _try_recovery(engine, ws_api, settings, bot_orders, db_queue)

        if engine.status == "INITIALIZING":
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

        await _shutdown.wait()
        log.info("Leállítás...")

        await watchdog.stop()
        await reconciliation.stop()
        await profit_reporter.stop()

        if engine.status not in ("EMERGENCY_STOPPING", "EMERGENCY_STOPPED"):
            try:
                pre_cancel = await asyncio.wait_for(ws_api.get_open_orders(symbol), timeout=3.0)
                bot_orders_open = [o for o in pre_cancel if o.get("clientOrderId", "").startswith("G-")]
                ext_orders_open = [o for o in pre_cancel if not o.get("clientOrderId", "").startswith("G-")]
                log.info("Leállítás: nyitott orderek törlése",
                         symbol=symbol,
                         bot_orders=len(bot_orders_open),
                         external_orders=len(ext_orders_open),
                         total=len(pre_cancel))
                for o in bot_orders_open:
                    log.debug("Törlendő bot order",
                              cid=o.get("clientOrderId"), side=o.get("side"),
                              price=o.get("price"), qty=o.get("origQty"))
            except Exception:
                log.info("Nyitott orderek törlése leállítás előtt", symbol=symbol)

            ws_api.enqueue_cancel_all(symbol)
            engine.status = "STOPPED"

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

            if settings.safety.sell_on_emergency_stop and engine.symbol_info:
                try:
                    account = await ws_api.get_account()
                    base_asset = settings.bot.base_asset
                    for b in account.get("balances", []):
                        if b["a"] == base_asset:
                            free = Decimal(b["f"])
                            if free > Decimal("0"):
                                from exchange.precision import round_down_to_step
                                qty = round_down_to_step(free, engine.symbol_info.lot_size.step_size)
                                if qty > 0:
                                    cid = f"GSELL-{engine.bot_run_id or 0}"
                                    ws_api.enqueue_market_sell(symbol, qty, cid)
                                    log.info("Graceful shutdown base sell", asset=base_asset, qty=str(qty))
                                    await asyncio.sleep(2.0)
                            break
                except Exception as e:
                    log.error("Graceful shutdown base sell hiba", error=str(e))

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
