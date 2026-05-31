import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from db.models import JobStatus, LogLevel, Priority


class JobCreate(BaseModel):
    type: str
    payload: dict[str, Any] = {}
    priority: Priority = Priority.MEDIUM
    run_at: datetime | None = None
    idempotency_key: str | None = None
    timeout_seconds: int | None = None
    max_retries: int = 3


class ProgressUpdate(BaseModel):
    progress: float


class JobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    type: str
    payload: dict[str, Any]
    priority: Priority
    status: JobStatus
    idempotency_key: str | None
    attempt_count: int
    max_retries: int
    timeout_seconds: int | None
    run_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    worker_pid: int | None
    result: dict[str, Any] | None
    error: str | None
    progress: float | None
    created_at: datetime
    updated_at: datetime


class JobLogResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    job_id: uuid.UUID
    level: LogLevel
    message: str
    meta: dict[str, Any] | None
    created_at: datetime
