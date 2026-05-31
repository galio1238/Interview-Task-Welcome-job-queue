import asyncio
import logging
import os
import signal
import uuid

from sqlalchemy import select

from config import settings
from db.models import Job, JobStatus
from db.session import AsyncSessionLocal
from jobs.registry import load_all
from rqueue.client import get_redis
from rqueue.dequeue import dequeue
from worker.executor import execute_job
from worker.logging_config import setup_logging

logger = logging.getLogger(__name__)

_shutdown_requested = False


def _handle_sigterm(signum: int, frame: object) -> None:
    global _shutdown_requested
    _shutdown_requested = True
    logger.info("worker.shutdown_requested", extra={"event": "worker.shutdown_requested", "worker_pid": os.getpid()})


async def _run_worker() -> None:
    global _shutdown_requested

    r = get_redis()
    load_all()

    logger.info("worker.started", extra={"event": "worker.started", "worker_pid": os.getpid()})

    while not _shutdown_requested:
        job_id: uuid.UUID | None = await dequeue(r, timeout=5)
        if job_id is None:
            continue

        async with AsyncSessionLocal() as session:
            result = await session.execute(select(Job).where(Job.id == job_id))
            job = result.scalar_one_or_none()

            if job is None:
                logger.warning(
                    "job.not_found",
                    extra={"event": "job.not_found", "job_id": str(job_id), "worker_pid": os.getpid()},
                )
                continue

            if job.status == JobStatus.CANCELLED:
                logger.info(
                    "job.cancelled",
                    extra={"event": "job.cancelled", "job_id": str(job_id), "worker_pid": os.getpid()},
                )
                continue

            from datetime import datetime, timezone
            job.status = JobStatus.PROCESSING
            job.started_at = datetime.now(tz=timezone.utc)
            job.worker_pid = os.getpid()
            await session.commit()

            await execute_job(job, session, r)

    logger.info("worker.shutdown", extra={"event": "worker.shutdown", "worker_pid": os.getpid()})


def main() -> None:
    setup_logging(settings.log_level)
    signal.signal(signal.SIGTERM, _handle_sigterm)
    asyncio.run(_run_worker())


if __name__ == "__main__":
    main()
