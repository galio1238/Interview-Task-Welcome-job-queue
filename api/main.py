from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import health, jobs, logs
from db.models import Base
from db.session import engine
from rqueue.client import close_redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await close_redis()


app = FastAPI(title="Job Queue API", version="0.1.0", lifespan=lifespan)

app.include_router(jobs.router)
app.include_router(logs.router)
app.include_router(health.router)
