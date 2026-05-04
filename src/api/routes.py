"""
FastAPI endpoint-ok – standalone API service.

A bot engine-hez HTTP-n keresztül kommunikál (belső API).
DB lekérdezések közvetlenül az adatbázisból.
"""
from decimal import Decimal
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from api.schemas import (
    BalanceResponse, BotRunResponse, BotStatusResponse,
    CommandResponse, FillResponse, HealthResponse, OrderResponse,
    PnlResponse, StartBotRequest, WsStatusResponse,
)
from grid.pnl import HalfMatch, compute_half_match_profit
from persistence.db import get_session
from persistence.models import Balance, BotRun, Fill, Order

router = APIRouter()

_bot_engine_url: str = ""


def init_routes(bot_engine_url: str) -> None:
    global _bot_engine_url
    _bot_engine_url = bot_engine_url


async def _engine_get(path: str) -> dict:
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(f"{_bot_engine_url}{path}")
        resp.raise_for_status()
        return resp.json()


async def _engine_post(path: str, json_body: dict = None) -> dict:
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.post(f"{_bot_engine_url}{path}", json=json_body)
        resp.raise_for_status()
        return resp.json()


@router.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="ok",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/status", response_model=BotStatusResponse)
async def status():
    try:
        data = await _engine_get("/internal/status")
        return BotStatusResponse(
            status=data["bot_status"],
            bot_run_id=data.get("bot_run_id"),
            symbol=data["symbol"],
            uptime_sec=data.get("uptime_sec"),
            open_orders=data.get("open_orders", 0),
            grid_levels=data.get("grid", {}),
        )
    except Exception:
        async with get_session() as session:
            result = await session.execute(
                select(BotRun).order_by(BotRun.id.desc()).limit(1)
            )
            run = result.scalar_one_or_none()
        return BotStatusResponse(
            status=run.status if run else "UNKNOWN",
            bot_run_id=run.id if run else None,
            symbol=run.symbol if run else "",
            uptime_sec=None,
            open_orders=0,
            grid_levels={},
        )


@router.get("/ws/status", response_model=WsStatusResponse)
async def ws_status():
    try:
        data = await _engine_get("/internal/status")
        ws = data.get("ws", {})
        return WsStatusResponse(
            trading_ws_connected=ws.get("trading_connected", False),
            user_stream_connected=True,
            trading_ws_last_msg_age_sec=ws.get("trading_last_msg_age", -1),
            user_stream_last_event_age_sec=ws.get("user_stream_last_event_age", -1),
            reconnect_counts={
                "trading_ws": ws.get("reconnects_trading", 0),
                "user_stream": ws.get("reconnects_user_stream", 0),
            },
        )
    except Exception:
        return WsStatusResponse(
            trading_ws_connected=False,
            user_stream_connected=False,
            trading_ws_last_msg_age_sec=-1,
            user_stream_last_event_age_sec=-1,
            reconnect_counts={"trading_ws": 0, "user_stream": 0},
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
    async with get_session() as session:
        result = await session.execute(
            select(Fill).where(Fill.bot_run_id == run_id).order_by(Fill.id)
        )
        fills_db = result.scalars().all()

    if not fills_db:
        return PnlResponse(
            grid_profit=Decimal("0"),
            total_buy_fees=Decimal("0"),
            total_sell_fees=Decimal("0"),
            completed_matches=0,
            unmatched_buy_qty=Decimal("0"),
            unmatched_sell_qty=Decimal("0"),
            total_pnl=Decimal("0"),
        )

    half_matches = [
        HalfMatch(
            fill_id=f.id,
            side=f.side,
            price=f.price,
            quantity=f.quantity,
            quote_qty=f.quote_quantity,
            commission=f.commission_amount,
            commission_asset=f.commission_asset or "",
            remaining=f.quantity,
        )
        for f in fills_db
    ]

    report = compute_half_match_profit(half_matches)

    return PnlResponse(
        grid_profit=report.grid_profit,
        total_buy_fees=report.total_buy_fees,
        total_sell_fees=report.total_sell_fees,
        completed_matches=report.completed_matches,
        unmatched_buy_qty=report.unmatched_buy_qty,
        unmatched_sell_qty=report.unmatched_sell_qty,
        total_pnl=report.total_pnl,
    )


# --- Control endpoint-ok – HTTP forward a bot engine-hez ---

@router.post("/bot/start", response_model=CommandResponse)
async def start_bot(request: StartBotRequest):
    try:
        await _engine_post("/internal/cmd", {"cmd": "start", "data": request.model_dump()})
        return CommandResponse(accepted=True, message="Start parancs fogadva")
    except Exception as e:
        raise HTTPException(502, f"Bot engine nem elérhető: {e}")


@router.post("/bot/pause", response_model=CommandResponse)
async def pause_bot():
    try:
        await _engine_post("/internal/cmd", {"cmd": "pause"})
        return CommandResponse(accepted=True, message="Pause parancs fogadva")
    except Exception as e:
        raise HTTPException(502, f"Bot engine nem elérhető: {e}")


@router.post("/bot/resume", response_model=CommandResponse)
async def resume_bot():
    try:
        await _engine_post("/internal/cmd", {"cmd": "resume"})
        return CommandResponse(accepted=True, message="Resume parancs fogadva")
    except Exception as e:
        raise HTTPException(502, f"Bot engine nem elérhető: {e}")


@router.post("/bot/stop", response_model=CommandResponse)
async def stop_bot():
    try:
        await _engine_post("/internal/cmd", {"cmd": "stop"})
        return CommandResponse(accepted=True, message="Stop parancs fogadva")
    except Exception as e:
        raise HTTPException(502, f"Bot engine nem elérhető: {e}")


@router.post("/bot/emergency-stop", response_model=CommandResponse)
async def emergency_stop():
    try:
        await _engine_post("/internal/cmd", {"cmd": "emergency_stop", "reason": "API kérelem"})
        return CommandResponse(accepted=True, message="Vészleállítás parancs fogadva")
    except Exception as e:
        raise HTTPException(502, f"Bot engine nem elérhető: {e}")
