import json
import uuid
from datetime import datetime

import redis.asyncio as aioredis

from db.models import Priority

ASSIGNMENTS_KEY = "processing:assignments"

# Atomically remove the job from the assignments hash and push it to its queue.
# This ensures a re-queued job is never lost between the HDEL and LPUSH steps.
_REQUEUE_SCRIPT = """
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('LPUSH', KEYS[2], ARGV[1])
return 1
"""


async def claim_assignment(
    r: aioredis.Redis,
    job_id: uuid.UUID,
    worker_pid: int,
    priority: Priority,
    started_at: datetime,
) -> None:
    data = json.dumps({
        "job_id": str(job_id),
        "worker_pid": worker_pid,
        "started_at": started_at.timestamp(),
        "priority": priority.value,
    })
    await r.hset(ASSIGNMENTS_KEY, str(job_id), data)


async def release_assignment(r: aioredis.Redis, job_id: uuid.UUID) -> None:
    await r.hdel(ASSIGNMENTS_KEY, str(job_id))


async def requeue_assignment(
    r: aioredis.Redis,
    job_id: uuid.UUID,
    queue_key: str,
) -> None:
    requeue = r.register_script(_REQUEUE_SCRIPT)
    await requeue(keys=[ASSIGNMENTS_KEY, queue_key], args=[str(job_id)])
