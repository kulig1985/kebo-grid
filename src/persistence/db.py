"""Async adatbázis engine és session factory."""
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.ext.asyncio import AsyncEngine
from app.config import DatabaseConfig


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def init_db(config: DatabaseConfig) -> None:
    global _engine, _session_factory
    _engine = create_async_engine(
        config.dsn,
        pool_size=config.pool_max_size,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False,
    )
    _session_factory = async_sessionmaker(
        _engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )


async def close_db() -> None:
    global _engine
    if _engine:
        await _engine.dispose()
        _engine = None


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    if _session_factory is None:
        raise RuntimeError("DB nincs inicializálva. Hívd meg az init_db()-t előbb.")
    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("DB engine nincs inicializálva.")
    return _engine
