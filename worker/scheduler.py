import asyncio
import json
import logging
import os
import signal
import time
import uuid

from sqlalchemy import select

from config import settings
from db.models import Job, JobStatus
from db.session import AsyncSessionLocal
from rqueue.assignments import ASSIGNMENTS_KEY, requeue_assignment
from rqueue.client import get_redis
from worker.logging_config import setup_logging

logger = logging.getLogger(__name__)

_DELAYED_KEY = "queue:delayed"
_QUEUE_HIGH = "queue:high"
_QUEUE_MEDIUM = "queue:medium"
_QUEUE_LOW = "queue:low"

_PRIORITY_TO_QUEUE = {
    "high": _QUEUE_HIGH,
    "medium": _QUEUE_MEDIUM,
    "low": _QUEUE_LOW,
}

# Atomically promote all due jobs from the delayed sorted set into their
# priority queues. Returns the number of jobs promoted.
_PROMOTE_SCRIPT = """
local items = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
local count = 0
for _, item in ipairs(items) do
    redis.call('ZREM', KEYS[1], item)
    local sep = string.find(item, ':')
    if sep then
        local priority = string.sub(item, 1, sep - 1)
        local job_id   = string.sub(item, sep + 1)
        if priority == 'high' then
            redis.call('LPUSH', KEYS[2], job_id)
        elseif priority == 'medium' then
            redis.call('LPUSH', KEYS[3], job_id)
        else
            redis.call('LPUSH', KEYS[4], job_id)
        end
    else
        -- Legacy / unknown format: fall back to medium queue.
        redis.call('LPUSH', KEYS[3], item)
    end
    count = count + 1
end
return count
"""

_shutdown_requested = False


def _handle_sigterm(signum: int, frame: object) -> None:
    global _shutdown_requested
    _shutdown_requested = True


def _is_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, OSError):
        return False


async def _promote_delayed_loop(r) -> None:
    promote = r.register_script(_PROMOTE_SCRIPT)
    logger.info("scheduler.started", extra={"event": "scheduler.started", "worker_pid": os.getpid()})

    while not _shutdown_requested:
        now = time.time()
        promoted = await promote(
            keys=[_DELAYED_KEY, _QUEUE_HIGH, _QUEUE_MEDIUM, _QUEUE_LOW],
            args=[now],
        )
        if promoted:
            logger.info(
                "scheduler.promoted",
                extra={"event": "scheduler.promoted", "count": promoted, "worker_pid": os.getpid()},
            )
        await asyncio.sleep(settings.scheduler_interval)


async def _check_liveness_once(r) -> None:
    assignments = await r.hgetall(ASSIGNMENTS_KEY)
    if not assignments:
        return

    for job_id_bytes, data_bytes in assignments.items():
        job_id_str = job_id_bytes if isinstance(job_id_bytes, str) else job_id_bytes.decode()
        try:
            data = json.loads(data_bytes)
        except (ValueError, TypeError):
            logger.warning(
                "scheduler.liveness.bad_entry",
                extra={"event": "scheduler.liveness.bad_entry", "job_id": job_id_str},
            )
            continue

        worker_pid = data.get("worker_pid")
        priority_str = data.get("priority", "medium")

        if worker_pid is None or _is_pid_alive(worker_pid):
            continue

        # Worker is dead — atomically remove from assignments and push back to queue.
        queue_key = _PRIORITY_TO_QUEUE.get(priority_str, _QUEUE_MEDIUM)
        job_id = uuid.UUID(job_id_str)
        await requeue_assignment(r, job_id, queue_key)

        # Reset the job in the DB so the next worker picks it up cleanly.
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(Job).where(Job.id == job_id))
            job = result.scalar_one_or_none()
            if job is not None and job.status == JobStatus.PROCESSING:
                job.status = JobStatus.PENDING
                job.started_at = None
                job.worker_pid = None
                await session.commit()

        logger.warning(
            "scheduler.liveness.requeued",
            extra={
                "event": "scheduler.liveness.requeued",
                "job_id": job_id_str,
                "dead_worker_pid": worker_pid,
                "queue": queue_key,
            },
        )


async def _liveness_check_loop(r) -> None:
    while not _shutdown_requested:
        await asyncio.sleep(settings.worker_liveness_interval)
        await _check_liveness_once(r)


async def _run_scheduler() -> None:
    r = get_redis()
    await asyncio.gather(
        _promote_delayed_loop(r),
        _liveness_check_loop(r),
    )
    logger.info("scheduler.shutdown", extra={"event": "scheduler.shutdown", "worker_pid": os.getpid()})


def main() -> None:
    setup_logging(settings.log_level)
    signal.signal(signal.SIGTERM, _handle_sigterm)
    asyncio.run(_run_scheduler())


if __name__ == "__main__":
    main()
