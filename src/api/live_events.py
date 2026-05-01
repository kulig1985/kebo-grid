"""WebSocket /api/events – live esemény stream a frontend számára."""
import asyncio
import json
from typing import Optional

from fastapi import WebSocket, WebSocketDisconnect

from app.logging import get_logger

log = get_logger(__name__)

# Aktív WebSocket kapcsolatok
_clients: set[WebSocket] = set()
_broadcast_queue: Optional[asyncio.Queue] = None


def set_broadcast_queue(queue: asyncio.Queue) -> None:
    global _broadcast_queue
    _broadcast_queue = queue


async def broadcast_loop() -> None:
    """Broadcast task: queue-ból olvas és minden kliensnek elküldi."""
    if _broadcast_queue is None:
        return
    while True:
        try:
            event = await _broadcast_queue.get()
            message = json.dumps(event)
            disconnected = set()
            for client in _clients.copy():
                try:
                    await client.send_text(message)
                except Exception:
                    disconnected.add(client)
            _clients.difference_update(disconnected)
            _broadcast_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error("Broadcast hiba", error=str(e))


async def websocket_events(websocket: WebSocket) -> None:
    """WebSocket /api/events endpoint handler."""
    await websocket.accept()
    _clients.add(websocket)
    log.info("Frontend WebSocket csatlakozva", total_clients=len(_clients))
    try:
        # Keepalive – vár amíg a kliens bontja a kapcsolatot
        while True:
            data = await websocket.receive_text()
            # ping-pong a frontend-től
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        pass
    finally:
        _clients.discard(websocket)
        log.info("Frontend WebSocket lecsatlakozott", total_clients=len(_clients))


def publish_event(event: dict) -> None:
    """Esemény küldése az összes csatlakozott frontendnek."""
    if _broadcast_queue is not None:
        _broadcast_queue.put_nowait(event)
