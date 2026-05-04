"""Kebo Grid API — önálló service, a bot engine-től független."""
import asyncio
import sys

import uvicorn
from fastapi import FastAPI, WebSocket

from app.config import load_config
from app.log_setup import get_logger, setup_logging
from api.routes import init_routes, router as api_router
from api.live_events import set_broadcast_queue, broadcast_loop, websocket_events
from persistence.db import close_db, init_db

log = get_logger(__name__)


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
        raise RuntimeError("DB migráció timeout (30s)!")
    log.info("Adatbázis migrációk kész")


async def main() -> None:
    config_file = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    settings = load_config(config_file)

    setup_logging(settings.logging.level, settings.logging.format)
    log.info("Kebo Grid API indul", config=config_file)

    init_db(settings.database)
    await run_migrations()

    app = FastAPI(title="Kebo Grid Bot API")
    app.include_router(api_router)

    broadcast_queue = asyncio.Queue(maxsize=1000)
    set_broadcast_queue(broadcast_queue)

    @app.websocket("/api/events")
    async def ws_events(websocket: WebSocket):
        await websocket_events(websocket)

    init_routes(bot_engine_url=settings.api.bot_engine_url)
    log.info("Bot engine URL", url=settings.api.bot_engine_url)

    uv_config = uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="warning")
    server = uvicorn.Server(uv_config)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(broadcast_loop(), name="broadcast")
        tg.create_task(server.serve(), name="api_http")

    await close_db()
    log.info("API leállva")


if __name__ == "__main__":
    asyncio.run(main())
