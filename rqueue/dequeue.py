import uuid

import redis.asyncio as aioredis

PRIORITY_QUEUES = ["queue:high", "queue:medium", "queue:low"]


async def dequeue(r: aioredis.Redis, timeout: int = 5) -> uuid.UUID | None:
    result = await r.brpop(PRIORITY_QUEUES, timeout=timeout)
    if result is None:
        return None
    _key, job_id_str = result
    return uuid.UUID(job_id_str)
