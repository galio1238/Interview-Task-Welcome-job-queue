import redis.asyncio as aioredis
from fastapi import APIRouter, Depends
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db, get_redis_client
from db.models import Job

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(
    db: AsyncSession = Depends(get_db),
    r: aioredis.Redis = Depends(get_redis_client),
):
    try:
        await db.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception:
        db_status = "error"

    try:
        await r.ping()
        redis_status = "ok"
    except Exception:
        redis_status = "error"

    queues = {
        "high": await r.llen("queue:high"),
        "medium": await r.llen("queue:medium"),
        "low": await r.llen("queue:low"),
        "delayed": await r.zcard("queue:delayed"),
        "dead_letter": await r.zcard("queue:dead_letter"),
    }

    rows = await db.execute(select(Job.status, func.count()).group_by(Job.status))
    jobs = {row[0].value: row[1] for row in rows}

    overall = "ok" if db_status == "ok" and redis_status == "ok" else "degraded"

    return {
        "status": overall,
        "database": db_status,
        "redis": redis_status,
        "queues": queues,
        "jobs": jobs,
    }
