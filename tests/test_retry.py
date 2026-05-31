"""
Tests for retry-with-exponential-backoff logic in worker/executor.py.

Focus:
  - delays follow base * 2^attempt for attempts 0, 1, 2
  - after max_retries exhausted: status = FAILED, job added to dead letter
  - after max_retries exhausted: enqueue() is NOT called (job won't be picked up again)
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from db.models import JobStatus
from worker.executor import _DEAD_LETTER_KEY, _fail_job

# Use a 1-second base so the test runs fast and delays stay sub-second distinguishable.
_TEST_BACKOFF_BASE = 1


@pytest.fixture(autouse=True)
def patch_settings():
    """Override settings for all tests in this module."""
    with patch("worker.executor.settings") as cfg:
        cfg.retry_backoff_base = _TEST_BACKOFF_BASE
        cfg.job_default_timeout = 300
        yield cfg


class TestExponentialBackoffDelays:
    """Retry delays must double on each successive attempt."""

    @pytest.mark.parametrize("attempt", [0, 1, 2])
    async def test_delay_is_base_times_two_to_the_attempt(
        self, attempt, make_job, mock_session, mock_redis
    ):
        job = make_job(attempt_count=attempt, max_retries=3)
        captured_run_at = []

        async def fake_enqueue(r, job_id, priority, run_at=None):
            captured_run_at.append(run_at)

        t_before = datetime.now(tz=timezone.utc)
        with patch("worker.executor.enqueue", side_effect=fake_enqueue):
            await _fail_job(job, mock_session, mock_redis, RuntimeError("boom"))
        t_after = datetime.now(tz=timezone.utc)

        assert len(captured_run_at) == 1, "enqueue must be called once per retry"
        run_at = captured_run_at[0]

        expected_delay = _TEST_BACKOFF_BASE * (2**attempt)
        # run_at must fall within [t_before + expected_delay, t_after + expected_delay + slack]
        assert run_at >= t_before + timedelta(seconds=expected_delay), (
            f"run_at is too early for attempt {attempt}"
        )
        assert run_at <= t_after + timedelta(seconds=expected_delay + 0.1), (
            f"run_at is too late for attempt {attempt}"
        )

    async def test_delays_are_strictly_increasing(self, make_job, mock_session, mock_redis):
        """Each successive attempt must have a longer delay than the previous."""
        delays = []

        async def fake_enqueue(r, job_id, priority, run_at=None):
            delays.append(run_at)

        with patch("worker.executor.enqueue", side_effect=fake_enqueue):
            for attempt in range(3):
                job = make_job(attempt_count=attempt, max_retries=3)
                t = datetime.now(tz=timezone.utc)
                await _fail_job(job, mock_session, mock_redis, RuntimeError("fail"))
                # Normalise: store seconds-from-call rather than absolute timestamps
                delays[-1] = (delays[-1] - t).total_seconds()

        assert delays[0] < delays[1] < delays[2], (
            f"Delays must strictly increase: {delays}"
        )

    async def test_delay_ratio_is_two(self, make_job, mock_session, mock_redis):
        """Each delay must be exactly double the previous (ratio = 2)."""
        delays = []

        async def fake_enqueue(r, job_id, priority, run_at=None):
            delays.append(run_at)

        with patch("worker.executor.enqueue", side_effect=fake_enqueue):
            for attempt in range(3):
                job = make_job(attempt_count=attempt, max_retries=3)
                t = datetime.now(tz=timezone.utc)
                await _fail_job(job, mock_session, mock_redis, RuntimeError("fail"))
                delays[-1] = (delays[-1] - t).total_seconds()

        # Allow ±5 % tolerance for wall-clock drift
        assert abs(delays[1] / delays[0] - 2.0) < 0.05, (
            f"delay[1]/delay[0] should be ~2, got {delays[1] / delays[0]:.3f}"
        )
        assert abs(delays[2] / delays[1] - 2.0) < 0.05, (
            f"delay[2]/delay[1] should be ~2, got {delays[2] / delays[1]:.3f}"
        )


class TestMaxRetriesExhausted:
    """When attempt_count reaches max_retries the job must be permanently failed."""

    async def test_status_becomes_failed(self, make_job, mock_session, mock_redis):
        job = make_job(attempt_count=3, max_retries=3)

        with patch("worker.executor.enqueue", new_callable=AsyncMock) as mock_enqueue:
            await _fail_job(job, mock_session, mock_redis, RuntimeError("last"))

        assert job.status == JobStatus.FAILED
        mock_enqueue.assert_not_called()

    async def test_attempt_count_incremented_on_final_failure(
        self, make_job, mock_session, mock_redis
    ):
        job = make_job(attempt_count=3, max_retries=3)

        with patch("worker.executor.enqueue", new_callable=AsyncMock):
            await _fail_job(job, mock_session, mock_redis, RuntimeError("last"))

        assert job.attempt_count == 4

    async def test_job_added_to_dead_letter_queue(self, make_job, mock_session, mock_redis):
        job = make_job(attempt_count=3, max_retries=3)

        with patch("worker.executor.enqueue", new_callable=AsyncMock):
            await _fail_job(job, mock_session, mock_redis, RuntimeError("last"))

        mock_redis.zadd.assert_called_once()
        call_args = mock_redis.zadd.call_args
        # First positional arg must be the dead letter key
        assert call_args[0][0] == _DEAD_LETTER_KEY
        # The mapping must contain the job id
        mapping: dict = call_args[0][1]
        assert str(job.id) in mapping

    async def test_enqueue_not_called_after_max_retries(self, make_job, mock_session, mock_redis):
        """Exhausted job must never be put back into any priority queue."""
        job = make_job(attempt_count=3, max_retries=3)

        with patch("worker.executor.enqueue", new_callable=AsyncMock) as mock_enqueue:
            await _fail_job(job, mock_session, mock_redis, RuntimeError("last"))

        mock_enqueue.assert_not_called()

    async def test_finished_at_is_set(self, make_job, mock_session, mock_redis):
        job = make_job(attempt_count=3, max_retries=3)

        with patch("worker.executor.enqueue", new_callable=AsyncMock):
            await _fail_job(job, mock_session, mock_redis, RuntimeError("last"))

        assert job.finished_at is not None


class TestRetryStateDuringBackoff:
    """Before max retries: job must be re-queued, not permanently failed."""

    async def test_status_becomes_scheduled_not_failed(self, make_job, mock_session, mock_redis):
        job = make_job(attempt_count=0, max_retries=3)

        with patch("worker.executor.enqueue", new_callable=AsyncMock):
            await _fail_job(job, mock_session, mock_redis, RuntimeError("transient"))

        assert job.status == JobStatus.SCHEDULED

    async def test_attempt_count_incremented_on_retry(self, make_job, mock_session, mock_redis):
        job = make_job(attempt_count=1, max_retries=3)

        with patch("worker.executor.enqueue", new_callable=AsyncMock):
            await _fail_job(job, mock_session, mock_redis, RuntimeError("transient"))

        assert job.attempt_count == 2

    async def test_dead_letter_not_written_during_retry(self, make_job, mock_session, mock_redis):
        job = make_job(attempt_count=0, max_retries=3)

        with patch("worker.executor.enqueue", new_callable=AsyncMock):
            await _fail_job(job, mock_session, mock_redis, RuntimeError("transient"))

        mock_redis.zadd.assert_not_called()

    async def test_error_text_stored_on_each_attempt(self, make_job, mock_session, mock_redis):
        job = make_job(attempt_count=0, max_retries=3)
        exc = ValueError("specific error")

        with patch("worker.executor.enqueue", new_callable=AsyncMock):
            await _fail_job(job, mock_session, mock_redis, exc)

        assert job.error is not None
        assert "specific error" in job.error
