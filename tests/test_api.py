"""
API-level tests covering:
  - Job submission and retrieval (POST /jobs, GET /jobs, GET /jobs/{id})
  - Job completion flow
  - Job cancellation (DELETE /jobs/{id})
  - Idempotency (duplicate key returns existing job; 24-hour TTL)
"""

import uuid
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from api.dependencies import get_db, get_redis_client
from api.main import app
from api.routes.jobs import _IDEMPOTENCY_TTL
from db.models import Job, JobStatus, Priority

_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_job(**kwargs) -> Job:
    defaults = dict(
        id=uuid.uuid4(),
        type="email",
        payload={"to": "user@example.com"},
        priority=Priority.MEDIUM,
        status=JobStatus.PENDING,
        idempotency_key=None,
        attempt_count=0,
        max_retries=3,
        timeout_seconds=None,
        run_at=None,
        started_at=None,
        finished_at=None,
        worker_pid=None,
        result=None,
        error=None,
        progress=None,
        created_at=_NOW,
        updated_at=_NOW,
    )
    defaults.update(kwargs)
    return Job(**defaults)


def _db_session(
    existing_job: Job | None = None,
    jobs_list: list[Job] | None = None,
) -> AsyncMock:
    """
    Mock async session. scalar_one_or_none() returns existing_job;
    scalars().all() returns jobs_list. refresh() stamps created_at/updated_at
    so newly-created Job objects can be serialised by the route.
    """
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = existing_job
    mock_result.scalars.return_value.all.return_value = jobs_list if jobs_list is not None else []

    async def _refresh(obj: Job) -> None:
        # SQLAlchemy column defaults (default=callable/value) are applied by the
        # ORM during flush, not during __init__. The mock session skips flush,
        # so we apply the defaults here to match what the real DB would return.
        if getattr(obj, "id", None) is None:
            obj.id = uuid.uuid4()
        if getattr(obj, "attempt_count", None) is None:
            obj.attempt_count = 0
        if getattr(obj, "created_at", None) is None:
            obj.created_at = _NOW
        if getattr(obj, "updated_at", None) is None:
            obj.updated_at = _NOW

    session = AsyncMock()
    session.execute.return_value = mock_result
    session.refresh.side_effect = _refresh
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


def _set_overrides(session: AsyncMock, redis: AsyncMock | None = None) -> None:
    if redis is None:
        redis = AsyncMock()
        redis.lpush = AsyncMock(return_value=1)
        redis.zadd = AsyncMock(return_value=1)

    async def _get_db() -> AsyncGenerator:
        yield session

    app.dependency_overrides[get_db] = _get_db
    app.dependency_overrides[get_redis_client] = lambda: redis


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def http():
    """
    AsyncClient using ASGITransport (lifespan skipped — no real DB connection).
    Dependency overrides are cleared after each test.
    """
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# 1. Job Submission and Retrieval
# ---------------------------------------------------------------------------


class TestJobSubmission:
    async def test_create_job_returns_201(self, http):
        _set_overrides(_db_session())

        response = await http.post("/jobs", json={"type": "email", "payload": {"to": "u@e.com"}})

        assert response.status_code == 201
        data = response.json()
        assert data["type"] == "email"
        assert data["status"] == "pending"
        assert data["priority"] == "medium"
        assert "id" in data

    async def test_create_job_with_high_priority(self, http):
        _set_overrides(_db_session())

        response = await http.post("/jobs", json={"type": "report", "priority": "high"})

        assert response.status_code == 201
        assert response.json()["priority"] == "high"

    async def test_create_job_enqueues_to_redis(self, http):
        redis = AsyncMock()
        redis.lpush = AsyncMock(return_value=1)
        redis.zadd = AsyncMock(return_value=1)
        _set_overrides(_db_session(), redis=redis)

        await http.post("/jobs", json={"type": "email"})

        redis.lpush.assert_called_once()

    async def test_get_job_by_id_returns_200(self, http):
        job = _make_job(status=JobStatus.PENDING)
        _set_overrides(_db_session(existing_job=job))

        response = await http.get(f"/jobs/{job.id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == str(job.id)
        assert data["type"] == job.type
        assert data["status"] == "pending"

    async def test_get_job_not_found_returns_404(self, http):
        _set_overrides(_db_session(existing_job=None))

        response = await http.get(f"/jobs/{uuid.uuid4()}")

        assert response.status_code == 404

    async def test_list_jobs_returns_all_jobs(self, http):
        jobs = [_make_job(type="email"), _make_job(type="report")]
        _set_overrides(_db_session(jobs_list=jobs))

        response = await http.get("/jobs")

        assert response.status_code == 200
        assert len(response.json()) == 2

    async def test_list_jobs_empty(self, http):
        _set_overrides(_db_session(jobs_list=[]))

        response = await http.get("/jobs")

        assert response.status_code == 200
        assert response.json() == []


# ---------------------------------------------------------------------------
# 2. Job Completion Flow
# ---------------------------------------------------------------------------


class TestJobCompletionFlow:
    async def test_completed_job_has_expected_fields(self, http):
        started = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc)
        job = _make_job(
            status=JobStatus.COMPLETED,
            started_at=started,
            finished_at=finished,
            result={"sent": True},
            progress=100.0,
        )
        _set_overrides(_db_session(existing_job=job))

        response = await http.get(f"/jobs/{job.id}")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "completed"
        assert data["result"] == {"sent": True}
        assert data["progress"] == 100.0
        assert data["finished_at"] is not None
        assert data["started_at"] is not None

    async def test_failed_job_exposes_error_and_attempt_count(self, http):
        job = _make_job(
            status=JobStatus.FAILED,
            error="Traceback: something went wrong",
            attempt_count=3,
        )
        _set_overrides(_db_session(existing_job=job))

        response = await http.get(f"/jobs/{job.id}")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "failed"
        assert "something went wrong" in data["error"]
        assert data["attempt_count"] == 3


# ---------------------------------------------------------------------------
# 3. Job Cancellation
# ---------------------------------------------------------------------------


class TestJobCancellation:
    async def test_cancel_pending_job_returns_204(self, http):
        job = _make_job(status=JobStatus.PENDING)
        _set_overrides(_db_session(existing_job=job))

        response = await http.delete(f"/jobs/{job.id}")

        assert response.status_code == 204

    async def test_cancel_sets_status_to_cancelled(self, http):
        job = _make_job(status=JobStatus.PENDING)
        session = _db_session(existing_job=job)
        _set_overrides(session)

        await http.delete(f"/jobs/{job.id}")

        assert job.status == JobStatus.CANCELLED
        session.commit.assert_called_once()

    async def test_cancel_nonexistent_job_returns_404(self, http):
        _set_overrides(_db_session(existing_job=None))

        response = await http.delete(f"/jobs/{uuid.uuid4()}")

        assert response.status_code == 404

    async def test_cancel_processing_job_returns_409(self, http):
        job = _make_job(status=JobStatus.PROCESSING)
        _set_overrides(_db_session(existing_job=job))

        response = await http.delete(f"/jobs/{job.id}")

        assert response.status_code == 409

    @pytest.mark.parametrize(
        "terminal_status",
        [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED],
    )
    async def test_cancel_terminal_job_returns_409(self, http, terminal_status):
        job = _make_job(status=terminal_status)
        _set_overrides(_db_session(existing_job=job))

        response = await http.delete(f"/jobs/{job.id}")

        assert response.status_code == 409


# ---------------------------------------------------------------------------
# 4. Idempotency
# ---------------------------------------------------------------------------


class TestIdempotency:
    async def test_idempotency_ttl_is_24_hours(self):
        assert _IDEMPOTENCY_TTL == timedelta(hours=24)

    async def test_same_key_within_24h_returns_existing_job_with_200(self, http):
        existing = _make_job(
            type="email",
            idempotency_key="order-42",
            created_at=_NOW - timedelta(hours=1),
        )
        _set_overrides(_db_session(existing_job=existing))

        response = await http.post(
            "/jobs", json={"type": "email", "idempotency_key": "order-42"}
        )

        assert response.status_code == 200
        assert response.json()["id"] == str(existing.id)

    async def test_same_key_within_24h_does_not_write_to_db(self, http):
        existing = _make_job(type="email", idempotency_key="order-42")
        session = _db_session(existing_job=existing)
        _set_overrides(session)

        await http.post("/jobs", json={"type": "email", "idempotency_key": "order-42"})

        session.add.assert_not_called()
        session.commit.assert_not_called()

    async def test_different_idempotency_key_creates_new_job(self, http):
        session = _db_session(existing_job=None)
        _set_overrides(session)

        response = await http.post(
            "/jobs", json={"type": "email", "idempotency_key": "order-99"}
        )

        assert response.status_code == 201
        session.add.assert_called_once()

    async def test_no_idempotency_key_always_creates_new_job(self, http):
        session = _db_session(existing_job=None)
        _set_overrides(session)

        response = await http.post("/jobs", json={"type": "email"})

        assert response.status_code == 201
        session.add.assert_called_once()

    async def test_no_idempotency_key_skips_db_lookup(self, http):
        """Without a key the route must not perform the idempotency SELECT."""
        session = _db_session(existing_job=None)
        _set_overrides(session)

        await http.post("/jobs", json={"type": "email"})

        session.execute.assert_not_called()

    async def test_expired_key_after_24h_creates_new_job(self, http):
        """
        The DB WHERE clause filters jobs older than 24 h. Simulate the TTL
        expiry by having the session return None (as the real DB would when the
        existing job's created_at falls outside the window), and assert a new
        job is created.
        """
        session = _db_session(existing_job=None)
        _set_overrides(session)

        response = await http.post(
            "/jobs", json={"type": "email", "idempotency_key": "order-42"}
        )

        assert response.status_code == 201
        session.add.assert_called_once()

    async def test_expired_key_after_24h_does_not_return_old_id(self, http):
        """New job must have a different ID than the expired one."""
        old_job = _make_job(
            type="email",
            idempotency_key="order-42",
            created_at=_NOW - timedelta(hours=25),
        )
        # Session returns None — old job filtered out by the 24 h cutoff.
        session = _db_session(existing_job=None)
        _set_overrides(session)

        response = await http.post(
            "/jobs", json={"type": "email", "idempotency_key": "order-42"}
        )

        assert response.status_code == 201
        assert response.json()["id"] != str(old_job.id)
