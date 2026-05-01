"""
FastAPI endpoint-ok – read/control API.

Fontos: POST parancs endpoint-ok command queue-ba tesznek,
NEM futtatnak blokkoló kereskedési műveleteket inline!
"""
import time
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from api.schemas import (
    BalanceResponse, BotRunResponse, BotStatusResponse,
    CommandResponse, FillResponse, HealthResponse, OrderResponse,
    PnlResponse, StartBotRequest, WsStatusResponse,
)
from persistence.db import get_session
from persistence.models import Balance, BotRun, Fill, Order

router = APIRouter()

# Ezeket a main.py tölti fel startup-kor
_engine_ref = None
_ws_api_ref = None
_user_stream_ref = None
_emergency_ref = None
_command_queue_ref = None
_start_time = time.monotonic()


def get_engine():
    if _engine_ref is None:
        raise HTTPException(503, "Bot motor nem elérhető")
    return _engine_ref


@router.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="ok",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/status", response_model=BotStatusResponse)
async def status():
    engine = get_engine()
    plan = engine.grid_plan
    return BotStatusResponse(
        status=engine.status,
        bot_run_id=engine.bot_run_id,
        symbol=engine.settings.bot.symbol,
        uptime_sec=time.monotonic() - _start_time,
        open_orders=len(engine._level_orders),
        grid_levels={
            "buy": plan.k_buy if plan else 0,
            "sell": plan.k_sell if plan else 0,
        },
    )


@router.get("/ws/status", response_model=WsStatusResponse)
async def ws_status():
    ws = _ws_api_ref
    us = _user_stream_ref
    return WsStatusResponse(
        trading_ws_connected=ws.is_connected if ws else False,
        user_stream_connected=True,  # nincs közvetlen is_connected a stream-en
        trading_ws_last_msg_age_sec=ws.last_msg_age_sec if ws else -1,
        user_stream_last_event_age_sec=us.last_event_age_sec if us else -1,
        reconnect_counts={
            "trading_ws": ws._reconnect_count if ws else 0,
            "user_stream": us._reconnect_count if us else 0,
        },
    )


@router.get("/bot/runs", response_model=list[BotRunResponse])
async def list_runs():
    async with get_session() as session:
        result = await session.execute(select(BotRun).order_by(BotRun.id.desc()).limit(50))
        runs = result.scalars().all()
    return [BotRunResponse.model_validate(r) for r in runs]


@router.get("/bot/runs/{run_id}", response_model=BotRunResponse)
async def get_run(run_id: int):
    async with get_session() as session:
        run = await session.get(BotRun, run_id)
    if not run:
        raise HTTPException(404, "Bot run nem található")
    return BotRunResponse.model_validate(run)


@router.get("/bot/runs/{run_id}/config")
async def get_run_config(run_id: int):
    async with get_session() as session:
        run = await session.get(BotRun, run_id)
    if not run:
        raise HTTPException(404, "Bot run nem található")
    return run.config_json


@router.get("/bot/runs/{run_id}/orders", response_model=list[OrderResponse])
async def get_orders(run_id: int, status: Optional[str] = None):
    async with get_session() as session:
        query = select(Order).where(Order.bot_run_id == run_id)
        if status:
            query = query.where(Order.status_local == status)
        result = await session.execute(query.order_by(Order.id.desc()).limit(200))
        orders = result.scalars().all()
    return [OrderResponse.model_validate(o) for o in orders]


@router.get("/bot/runs/{run_id}/orders/{client_order_id}", response_model=OrderResponse)
async def get_order(run_id: int, client_order_id: str):
    async with get_session() as session:
        result = await session.execute(
            select(Order).where(
                Order.bot_run_id == run_id,
                Order.client_order_id == client_order_id,
            )
        )
        order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(404, "Order nem található")
    return OrderResponse.model_validate(order)


@router.get("/bot/runs/{run_id}/fills", response_model=list[FillResponse])
async def get_fills(run_id: int):
    async with get_session() as session:
        result = await session.execute(
            select(Fill).where(Fill.bot_run_id == run_id).order_by(Fill.id.desc()).limit(500)
        )
        fills = result.scalars().all()
    return [FillResponse.model_validate(f) for f in fills]


@router.get("/bot/runs/{run_id}/balances", response_model=list[BalanceResponse])
async def get_balances(run_id: int):
    async with get_session() as session:
        result = await session.execute(
            select(Balance).where(Balance.bot_run_id == run_id).order_by(Balance.id.desc()).limit(100)
        )
        balances = result.scalars().all()
    return [BalanceResponse.model_validate(b) for b in balances]


@router.get("/bot/runs/{run_id}/pnl", response_model=PnlResponse)
async def get_pnl(run_id: int):
    engine = get_engine()
    if engine.bot_run_id != run_id:
        raise HTTPException(404, "PnL csak az aktív bot run-ra elérhető")
    summary = engine.pnl.summary()
    return PnlResponse(
        total_realized_quote=summary["total_realized_quote"],
        completed_cycles=summary["completed_cycles"],
        avg_profit_per_cycle=summary["avg_profit_per_cycle"],
    )


# --- Control endpoint-ok – queue-ba tesznek, NEM blokkolnak ---

@router.post("/bot/start", response_model=CommandResponse)
async def start_bot(request: StartBotRequest):
    if _command_queue_ref is None:
        raise HTTPException(503, "Command queue nem elérhető")
    _command_queue_ref.put_nowait({"cmd": "start", "data": request.model_dump()})
    return CommandResponse(accepted=True, message="Start parancs fogadva")


@router.post("/bot/pause", response_model=CommandResponse)
async def pause_bot():
    if _command_queue_ref is None:
        raise HTTPException(503, "Command queue nem elérhető")
    _command_queue_ref.put_nowait({"cmd": "pause"})
    return CommandResponse(accepted=True, message="Pause parancs fogadva")


@router.post("/bot/resume", response_model=CommandResponse)
async def resume_bot():
    if _command_queue_ref is None:
        raise HTTPException(503, "Command queue nem elérhető")
    _command_queue_ref.put_nowait({"cmd": "resume"})
    return CommandResponse(accepted=True, message="Resume parancs fogadva")


@router.post("/bot/stop", response_model=CommandResponse)
async def stop_bot():
    if _command_queue_ref is None:
        raise HTTPException(503, "Command queue nem elérhető")
    _command_queue_ref.put_nowait({"cmd": "stop"})
    return CommandResponse(accepted=True, message="Stop parancs fogadva")


@router.post("/bot/emergency-stop", response_model=CommandResponse)
async def emergency_stop():
    """Vészleállítás – azonnal visszatér, a végrehajtás async."""
    if _command_queue_ref is None:
        raise HTTPException(503, "Command queue nem elérhető")
    _command_queue_ref.put_nowait({"cmd": "emergency_stop", "reason": "API kérelem"})
    return CommandResponse(accepted=True, message="Vészleállítás parancs fogadva")


def init_routes(engine, ws_api, user_stream, emergency, command_queue):
    """Globális referenciák beállítása startup-kor."""
    global _engine_ref, _ws_api_ref, _user_stream_ref, _emergency_ref, _command_queue_ref
    _engine_ref = engine
    _ws_api_ref = ws_api
    _user_stream_ref = user_stream
    _emergency_ref = emergency
    _command_queue_ref = command_queue
