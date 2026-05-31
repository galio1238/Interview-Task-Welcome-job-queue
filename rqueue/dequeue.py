import uuid

import redis.asyncio as aioredis

from rqueue.enqueue import AGING_LOW_KEY, AGING_MEDIUM_KEY

PRIORITY_QUEUES = ["queue:high", "queue:medium", "queue:low"]

_QUEUE_TO_AGING = {
    "queue:low": AGING_LOW_KEY,
    "queue:medium": AGING_MEDIUM_KEY,
}


async def dequeue(r: aioredis.Redis, timeout: int = 5) -> uuid.UUID | None:
    result = await r.brpop(PRIORITY_QUEUES, timeout=timeout)
    if result is None:
        return None
    key, job_id_str = result
    key_str = key if isinstance(key, str) else key.decode()
    aging_key = _QUEUE_TO_AGING.get(key_str)
    if aging_key:
        await r.zrem(aging_key, job_id_str)
    return uuid.UUID(job_id_str)
