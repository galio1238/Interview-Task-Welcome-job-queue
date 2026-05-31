import uuid
from unittest.mock import AsyncMock

import pytest

from db.models import Job, JobStatus, Priority


@pytest.fixture
def make_job():
    """Return a factory for in-memory Job objects (no DB required)."""

    def _factory(
        attempt_count: int = 0,
        max_retries: int = 3,
        status: JobStatus = JobStatus.PROCESSING,
        priority: Priority = Priority.MEDIUM,
        job_type: str = "test_job",
    ) -> Job:
        return Job(
            id=uuid.uuid4(),
            type=job_type,
            payload={},
            priority=priority,
            status=status,
            attempt_count=attempt_count,
            max_retries=max_retries,
        )

    return _factory


@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.commit = AsyncMock()
    return session


@pytest.fixture
def mock_redis():
    r = AsyncMock()
    r.zadd = AsyncMock()
    r.lpush = AsyncMock()
    return r
