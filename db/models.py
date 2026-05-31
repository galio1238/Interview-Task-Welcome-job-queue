import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Float, Index, Integer, SmallInteger, String, Text
from sqlalchemy import Enum as SAEnum
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Priority(str, enum.Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class JobStatus(str, enum.Enum):
    SCHEDULED = "scheduled"
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        Index(
            "uq_jobs_type_idempotency_key",
            "type",
            "idempotency_key",
            unique=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        Index("ix_jobs_status", "status"),
        Index(
            "ix_jobs_run_at_scheduled",
            "run_at",
            postgresql_where=text("status = 'scheduled'::job_status"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    priority: Mapped[Priority] = mapped_column(
        SAEnum(Priority, name="job_priority", values_callable=lambda x: [e.value for e in x]),
        nullable=False,
        default=Priority.MEDIUM,
    )
    status: Mapped[JobStatus] = mapped_column(
        SAEnum(JobStatus, name="job_status", values_callable=lambda x: [e.value for e in x]),
        nullable=False,
        default=JobStatus.PENDING,
    )
    idempotency_key: Mapped[str | None] = mapped_column(
        String(255), nullable=True, default=None
    )
    attempt_count: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=3)
    timeout_seconds: Mapped[int | None] = mapped_column(
        Integer, nullable=True, default=None
    )
    run_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )
    worker_pid: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    result: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True, default=None
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)
    progress: Mapped[float | None] = mapped_column(Float, nullable=True, default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
