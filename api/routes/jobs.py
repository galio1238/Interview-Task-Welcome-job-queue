import uuid
from datetime import datetime, timedelta, timezone

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db, get_redis_client
from api.schemas import JobCreate, JobLogResponse, JobResponse, ProgressUpdate
from db.models import Job, JobLog, JobStatus, LogLevel, Priority
from rqueue.enqueue import enqueue

router = APIRouter(prefix="/jobs", tags=["jobs"])

_TERMINAL = {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}
_IDEMPOTENCY_TTL = timedelta(hours=24)


@router.post("", status_code=201, response_model=JobResponse)
async def create_job(
    body: JobCreate,
    response: Response,
    db: AsyncSession = Depends(get_db),
    r: aioredis.Redis = Depends(get_redis_client),
):
    if body.idempotency_key:
        cutoff = datetime.now(tz=timezone.utc) - _IDEMPOTENCY_TTL
        result = await db.execute(
            select(Job).where(
                Job.type == body.type,
                Job.idempotency_key == body.idempotency_key,
                Job.created_at >= cutoff,
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            response.status_code = status.HTTP_200_OK
            return existing

    job_status = JobStatus.SCHEDULED if body.run_at else JobStatus.PENDING
    job = Job(
        type=body.type,
        payload=body.payload,
        priority=body.priority,
        status=job_status,
        idempotency_key=body.idempotency_key,
        timeout_seconds=body.timeout_seconds,
        max_retries=body.max_retries,
        run_at=body.run_at,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    await enqueue(r, job.id, job.priority, job.run_at)

    return job


@router.get("", response_model=list[JobResponse])
async def list_jobs(
    job_status: JobStatus | None = None,
    priority: Priority | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(Job)
    if job_status is not None:
        query = query.where(Job.status == job_status)
    if priority is not None:
        query = query.where(Job.priority == priority)
    result = await db.execute(query)
    return result.scalars().all()


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.delete("/{job_id}", status_code=204)
async def cancel_job(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status == JobStatus.PROCESSING:
        raise HTTPException(status_code=409, detail="Cannot cancel a running job")
    if job.status in _TERMINAL:
        raise HTTPException(status_code=409, detail=f"Job is already {job.status.value}")
    job.status = JobStatus.CANCELLED
    await db.commit()


@router.post("/{job_id}/retry", response_model=JobResponse)
async def retry_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    r: aioredis.Redis = Depends(get_redis_client),
):
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobStatus.FAILED:
        raise HTTPException(status_code=409, detail="Only failed jobs can be retried")

    job.status = JobStatus.PENDING
    job.attempt_count = 0
    job.error = None
    job.result = None
    job.started_at = None
    job.finished_at = None
    job.progress = None
    await db.commit()
    await db.refresh(job)

    await enqueue(r, job.id, job.priority)

    return job


@router.get("/{job_id}/logs", response_model=list[JobLogResponse])
async def get_job_logs(
    job_id: uuid.UUID,
    level: LogLevel | None = None,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Job).where(Job.id == job_id))
    if result.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Job not found")
    query = select(JobLog).where(JobLog.job_id == job_id).order_by(JobLog.created_at)
    if level is not None:
        query = query.where(JobLog.level == level)
    result = await db.execute(query)
    return result.scalars().all()


@router.patch("/{job_id}/progress", response_model=JobResponse)
async def update_progress(
    job_id: uuid.UUID,
    body: ProgressUpdate,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobStatus.PROCESSING:
        raise HTTPException(status_code=409, detail="Progress can only be updated on running jobs")
    job.progress = body.progress
    await db.commit()
    await db.refresh(job)
    return job
