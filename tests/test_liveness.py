"""
Tests for worker liveness tracking (processing:assignments).

When a worker dies mid-job the scheduler's _check_liveness_once must:
  1. Detect the dead PID stored in the processing:assignments hash
  2. Atomically remove the assignment entry and push the job back to its priority queue
  3. Reset the job in the DB to PENDING (cleared started_at / worker_pid)

Tests use fakeredis[lua] so the _REQUEUE_SCRIPT Lua runs against a real
interpreter. Install with: pip install fakeredis[lua]
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

try:
    import fakeredis

    HAS_FAKEREDIS = True
except ImportError:
    HAS_FAKEREDIS = False

from db.models import Job, JobStatus, Priority
from rqueue.assignments import ASSIGNMENTS_KEY, claim_assignment
from worker.scheduler import _check_liveness_once

_QUEUE_HIGH = "queue:high"
_QUEUE_MEDIUM = "queue:medium"
_QUEUE_LOW = "queue:low"

requires_lua = pytest.mark.skipif(
    not HAS_FAKEREDIS,
    reason="fakeredis[lua] not installed — run: pip install fakeredis[lua]",
)

# A PID high enough to be virtually guaranteed dead on any CI box.
_DEAD_PID = 999_999


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
async def fake_redis():
    r = fakeredis.FakeAsyncRedis(decode_responses=True)
    yield r
    await r.aclose()


def _make_processing_job(priority: Priority = Priority.HIGH) -> Job:
    return Job(
        id=uuid.uuid4(),
        type="test_job",
        payload={},
        priority=priority,
        status=JobStatus.PROCESSING,
        attempt_count=0,
        max_retries=3,
        worker_pid=_DEAD_PID,
        started_at=datetime.now(tz=timezone.utc),
    )


def _db_ctx(job: Job | None):
    """
    Return (ctx_manager, mock_session) where ctx_manager patches
    AsyncSessionLocal() to yield a session that returns *job* on execute.

    scalar_one_or_none() is called without await in the scheduler, so it
    must be a plain MagicMock, not an AsyncMock.
    """
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = job

    mock_session = AsyncMock()
    mock_session.execute.return_value = mock_result
    mock_session.commit = AsyncMock()

    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_session
    mock_ctx.__aexit__.return_value = False

    return mock_ctx, mock_session


# ---------------------------------------------------------------------------
# Core recovery behaviour
# ---------------------------------------------------------------------------


class TestDeadWorkerRecovery:
    """When a worker PID is dead, its job must be fully recovered."""

    @requires_lua
    async def test_job_pushed_back_to_priority_queue(self, fake_redis):
        job = _make_processing_job(Priority.HIGH)
        await claim_assignment(fake_redis, job.id, _DEAD_PID, Priority.HIGH, job.started_at)

        ctx, _ = _db_ctx(job)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=False):
                await _check_liveness_once(fake_redis)

        assert str(job.id) in await fake_redis.lrange(_QUEUE_HIGH, 0, -1)

    @requires_lua
    async def test_assignment_removed_from_hash(self, fake_redis):
        job = _make_processing_job(Priority.MEDIUM)
        await claim_assignment(fake_redis, job.id, _DEAD_PID, Priority.MEDIUM, job.started_at)

        ctx, _ = _db_ctx(job)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=False):
                await _check_liveness_once(fake_redis)

        assert not await fake_redis.hexists(ASSIGNMENTS_KEY, str(job.id))

    @requires_lua
    async def test_db_status_reset_to_pending(self, fake_redis):
        job = _make_processing_job(Priority.LOW)
        await claim_assignment(fake_redis, job.id, _DEAD_PID, Priority.LOW, job.started_at)

        ctx, mock_session = _db_ctx(job)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=False):
                await _check_liveness_once(fake_redis)

        assert job.status == JobStatus.PENDING

    @requires_lua
    async def test_db_started_at_cleared(self, fake_redis):
        job = _make_processing_job()
        await claim_assignment(fake_redis, job.id, _DEAD_PID, Priority.HIGH, job.started_at)

        ctx, _ = _db_ctx(job)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=False):
                await _check_liveness_once(fake_redis)

        assert job.started_at is None

    @requires_lua
    async def test_db_worker_pid_cleared(self, fake_redis):
        job = _make_processing_job()
        await claim_assignment(fake_redis, job.id, _DEAD_PID, Priority.HIGH, job.started_at)

        ctx, _ = _db_ctx(job)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=False):
                await _check_liveness_once(fake_redis)

        assert job.worker_pid is None

    @requires_lua
    async def test_db_commit_called(self, fake_redis):
        job = _make_processing_job()
        await claim_assignment(fake_redis, job.id, _DEAD_PID, Priority.HIGH, job.started_at)

        ctx, mock_session = _db_ctx(job)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=False):
                await _check_liveness_once(fake_redis)

        mock_session.commit.assert_called_once()


# ---------------------------------------------------------------------------
# Priority routing
# ---------------------------------------------------------------------------


class TestPriorityRouting:
    """Re-queued job must land in the queue matching its original priority."""

    @requires_lua
    @pytest.mark.parametrize(
        "priority, expected_queue",
        [
            (Priority.HIGH, _QUEUE_HIGH),
            (Priority.MEDIUM, _QUEUE_MEDIUM),
            (Priority.LOW, _QUEUE_LOW),
        ],
    )
    async def test_job_requeued_to_correct_queue(self, fake_redis, priority, expected_queue):
        job = _make_processing_job(priority)
        await claim_assignment(fake_redis, job.id, _DEAD_PID, priority, job.started_at)

        ctx, _ = _db_ctx(job)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=False):
                await _check_liveness_once(fake_redis)

        assert str(job.id) in await fake_redis.lrange(expected_queue, 0, -1)

    @requires_lua
    @pytest.mark.parametrize(
        "priority, wrong_queues",
        [
            (Priority.HIGH, [_QUEUE_MEDIUM, _QUEUE_LOW]),
            (Priority.MEDIUM, [_QUEUE_HIGH, _QUEUE_LOW]),
            (Priority.LOW, [_QUEUE_HIGH, _QUEUE_MEDIUM]),
        ],
    )
    async def test_job_not_in_wrong_queues(self, fake_redis, priority, wrong_queues):
        job = _make_processing_job(priority)
        await claim_assignment(fake_redis, job.id, _DEAD_PID, priority, job.started_at)

        ctx, _ = _db_ctx(job)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=False):
                await _check_liveness_once(fake_redis)

        for queue in wrong_queues:
            assert str(job.id) not in await fake_redis.lrange(queue, 0, -1)


# ---------------------------------------------------------------------------
# Alive worker — no action taken
# ---------------------------------------------------------------------------


class TestAliveWorkerNotTouched:
    """Jobs owned by a live worker must not be requeued or modified."""

    @requires_lua
    async def test_alive_worker_job_stays_in_assignments(self, fake_redis):
        job = _make_processing_job()
        await claim_assignment(fake_redis, job.id, _DEAD_PID, Priority.HIGH, job.started_at)

        ctx, _ = _db_ctx(job)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=True):
                await _check_liveness_once(fake_redis)

        assert await fake_redis.hexists(ASSIGNMENTS_KEY, str(job.id))

    @requires_lua
    async def test_alive_worker_job_not_pushed_to_queue(self, fake_redis):
        job = _make_processing_job()
        await claim_assignment(fake_redis, job.id, _DEAD_PID, Priority.HIGH, job.started_at)

        ctx, _ = _db_ctx(job)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=True):
                await _check_liveness_once(fake_redis)

        assert str(job.id) not in await fake_redis.lrange(_QUEUE_HIGH, 0, -1)

    @requires_lua
    async def test_alive_worker_db_not_touched(self, fake_redis):
        job = _make_processing_job()
        await claim_assignment(fake_redis, job.id, _DEAD_PID, Priority.HIGH, job.started_at)

        ctx, mock_session = _db_ctx(job)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=True):
                await _check_liveness_once(fake_redis)

        mock_session.commit.assert_not_called()


# ---------------------------------------------------------------------------
# Multiple orphaned jobs
# ---------------------------------------------------------------------------


class TestMultipleOrphanedJobs:
    """All jobs from dead workers in a single pass must be recovered."""

    @requires_lua
    async def test_all_dead_jobs_removed_from_assignments(self, fake_redis):
        jobs = [_make_processing_job(Priority.MEDIUM) for _ in range(3)]
        for job in jobs:
            await claim_assignment(fake_redis, job.id, _DEAD_PID, Priority.MEDIUM, job.started_at)

        # Return None from DB — enough to verify Redis behaviour independently.
        ctx, _ = _db_ctx(None)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=False):
                await _check_liveness_once(fake_redis)

        for job in jobs:
            assert not await fake_redis.hexists(ASSIGNMENTS_KEY, str(job.id))

    @requires_lua
    async def test_all_dead_jobs_appear_in_queue(self, fake_redis):
        jobs = [_make_processing_job(Priority.MEDIUM) for _ in range(3)]
        for job in jobs:
            await claim_assignment(fake_redis, job.id, _DEAD_PID, Priority.MEDIUM, job.started_at)

        ctx, _ = _db_ctx(None)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=False):
                await _check_liveness_once(fake_redis)

        queue_items = await fake_redis.lrange(_QUEUE_MEDIUM, 0, -1)
        for job in jobs:
            assert str(job.id) in queue_items

    @requires_lua
    async def test_empty_assignments_is_a_noop(self, fake_redis):
        """_check_liveness_once on an empty hash must not raise."""
        ctx, mock_session = _db_ctx(None)
        with patch("worker.scheduler.AsyncSessionLocal", return_value=ctx):
            with patch("worker.scheduler._is_pid_alive", return_value=False):
                await _check_liveness_once(fake_redis)

        mock_session.commit.assert_not_called()
