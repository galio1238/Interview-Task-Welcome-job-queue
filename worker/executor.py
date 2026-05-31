import asyncio
import logging
import os
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import redis.asyncio as aioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from db.models import Job, JobStatus
from db.session import AsyncSessionLocal
from jobs.registry import REGISTRY
from rqueue.assignments import release_assignment
from rqueue.enqueue import enqueue

logger = logging.getLogger(__name__)

_DEAD_LETTER_KEY = "queue:dead_letter"

_thread_pool = ThreadPoolExecutor()


def _make_progress_callback(
    job_id: uuid.UUID,
    loop: asyncio.AbstractEventLoop,
) -> Callable[[float], None]:
    async def _persist(percent: float) -> None:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(Job).where(Job.id == job_id))
            job = result.scalar_one_or_none()
            if job is not None:
                job.progress = percent
                await session.commit()

    def callback(percent: float) -> None:
        asyncio.run_coroutine_threadsafe(_persist(percent), loop)

    return callback


async def execute_job(job: Job, session: AsyncSession, r: aioredis.Redis) -> None:
    handler_cls = REGISTRY.get(job.type)
    if handler_cls is None:
        await _fail_job(job, session, r, RuntimeError(f"Unknown job type: {job.type!r}"))
        return

    timeout = job.timeout_seconds or settings.job_default_timeout
    loop = asyncio.get_event_loop()
    on_progress = _make_progress_callback(job.id, loop)
    handler = handler_cls()

    logger.info(
        "job.started",
        extra={
            "event": "job.started",
            "job_id": str(job.id),
            "job_type": job.type,
            "worker_pid": os.getpid(),
            "attempt": job.attempt_count,
            "priority": job.priority.value,
        },
    )

    try:
        result: dict[str, Any] = await asyncio.wait_for(
            loop.run_in_executor(_thread_pool, _run_sync, handler, job.payload, on_progress),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        await _fail_job(job, session, r, TimeoutError(f"Job exceeded timeout of {timeout}s"))
        return
    except Exception as exc:
        await _fail_job(job, session, r, exc)
        return

    now = datetime.now(tz=timezone.utc)
    job.status = JobStatus.COMPLETED
    job.result = result
    job.finished_at = now
    job.progress = 100.0
    await session.commit()
    await release_assignment(r, job.id)

    logger.info(
        "job.done",
        extra={
            "event": "job.done",
            "job_id": str(job.id),
            "job_type": job.type,
            "worker_pid": os.getpid(),
        },
    )


def _run_sync(handler: Any, payload: dict, on_progress: Callable) -> dict:
    return handler.run(payload, on_progress=on_progress)


async def _fail_job(job: Job, session: AsyncSession, r: aioredis.Redis, exc: Exception) -> None:
    error_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    now = datetime.now(tz=timezone.utc)

    job.error = error_text

    if job.attempt_count < job.max_retries:
        delay_seconds = settings.retry_backoff_base * (2 ** job.attempt_count)
        job.attempt_count += 1
        job.status = JobStatus.SCHEDULED
        job.started_at = None
        job.finished_at = None
        await session.commit()

        run_at = now + timedelta(seconds=delay_seconds)
        await enqueue(r, job.id, job.priority, run_at=run_at)
        await release_assignment(r, job.id)

        logger.warning(
            "job.retry_scheduled",
            extra={
                "event": "job.retry_scheduled",
                "job_id": str(job.id),
                "job_type": job.type,
                "worker_pid": os.getpid(),
                "attempt": job.attempt_count,
                "delay_seconds": delay_seconds,
                "error": str(exc),
            },
        )
    else:
        job.attempt_count += 1
        job.status = JobStatus.FAILED
        job.finished_at = now
        await session.commit()

        await r.zadd(_DEAD_LETTER_KEY, {str(job.id): now.timestamp()})
        await release_assignment(r, job.id)

        logger.error(
            "job.dead_lettered",
            extra={
                "event": "job.dead_lettered",
                "job_id": str(job.id),
                "job_type": job.type,
                "worker_pid": os.getpid(),
                "attempt": job.attempt_count,
                "error": str(exc),
            },
        )
