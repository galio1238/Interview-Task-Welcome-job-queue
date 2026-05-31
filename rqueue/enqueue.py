import uuid
from datetime import datetime, timezone

import redis.asyncio as aioredis

from db.models import Priority

QUEUE_KEY = "queue:{priority}"
DELAYED_KEY = "queue:delayed"
AGING_LOW_KEY = "queue:aging:low"
AGING_MEDIUM_KEY = "queue:aging:medium"


def _queue_key(priority: Priority) -> str:
    return QUEUE_KEY.format(priority=priority.value)


async def enqueue(
    r: aioredis.Redis,
    job_id: uuid.UUID,
    priority: Priority,
    run_at: datetime | None = None,
) -> None:
    job_id_str = str(job_id)

    if run_at is not None:
        score = run_at.astimezone(timezone.utc).timestamp()
        # Encode priority so the scheduler Lua script can route to the right queue.
        value = f"{priority.value}:{job_id_str}"
        await r.zadd(DELAYED_KEY, {value: score})
    else:
        now = datetime.now(tz=timezone.utc).timestamp()
        await r.lpush(_queue_key(priority), job_id_str)
        if priority == Priority.LOW:
            await r.zadd(AGING_LOW_KEY, {job_id_str: now})
        elif priority == Priority.MEDIUM:
            await r.zadd(AGING_MEDIUM_KEY, {job_id_str: now})
