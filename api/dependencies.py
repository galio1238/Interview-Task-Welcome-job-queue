from collections.abc import AsyncGenerator

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from db.session import AsyncSessionLocal
from rqueue.client import get_redis


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


def get_redis_client() -> aioredis.Redis:
    return get_redis()
