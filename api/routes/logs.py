import uuid

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db
from api.schemas import JobLogResponse
from db.models import JobLog, LogLevel

router = APIRouter(prefix="/logs", tags=["logs"])


@router.get("", response_model=list[JobLogResponse])
async def list_logs(
    job_id: uuid.UUID | None = None,
    level: LogLevel | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(JobLog).order_by(JobLog.created_at.desc())
    if job_id is not None:
        query = query.where(JobLog.job_id == job_id)
    if level is not None:
        query = query.where(JobLog.level == level)
    result = await db.execute(query)
    return result.scalars().all()
