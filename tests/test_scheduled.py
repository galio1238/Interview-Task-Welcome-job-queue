"""
Tests for scheduled-job handling.

Two layers are covered:
  1. Enqueue — jobs with run_at are stored in queue:delayed with the correct
     score (Unix timestamp) and value format ({priority}:{job_id}).
  2. Lua promote script — the atomic Redis script that moves due jobs from
     queue:delayed into their priority queues.

The Lua tests use fakeredis[lua] so the script runs against a real Lua
interpreter, not a Python mock.  Install with: pip install fakeredis[lua]
"""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

try:
    import fakeredis

    HAS_FAKEREDIS = True
except ImportError:  # pragma: no cover
    HAS_FAKEREDIS = False

from db.models import Priority
from rqueue.enqueue import AGING_LOW_KEY, AGING_MEDIUM_KEY, DELAYED_KEY, enqueue
from worker.scheduler import _PROMOTE_SCRIPT

_QUEUE_HIGH = "queue:high"
_QUEUE_MEDIUM = "queue:medium"
_QUEUE_LOW = "queue:low"

requires_lua = pytest.mark.skipif(
    not HAS_FAKEREDIS,
    reason="fakeredis[lua] not installed — run: pip install fakeredis[lua]",
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
async def fake_redis():
    r = fakeredis.FakeAsyncRedis(decode_responses=True)
    yield r
    await r.aclose()


async def _promote(r, now_ts: float) -> int:
    """Run the Lua promote script against *r* as if the current time is *now_ts*."""
    return await r.eval(
        _PROMOTE_SCRIPT,
        6,                  # numkeys — must match the 6 KEYS the script uses
        DELAYED_KEY,        # KEYS[1]
        _QUEUE_HIGH,        # KEYS[2]
        _QUEUE_MEDIUM,      # KEYS[3]
        _QUEUE_LOW,         # KEYS[4]
        AGING_MEDIUM_KEY,   # KEYS[5]
        AGING_LOW_KEY,      # KEYS[6]
        str(now_ts),        # ARGV[1]
    )


# ---------------------------------------------------------------------------
# 1. Enqueue — storing scheduled jobs correctly
# ---------------------------------------------------------------------------


class TestEnqueueDelayed:
    """enqueue() with run_at must write to queue:delayed, never to a priority queue."""

    async def test_future_job_stored_in_delayed_sorted_set(self):
        r = AsyncMock()
        job_id = uuid.uuid4()
        run_at = datetime(2099, 6, 1, tzinfo=timezone.utc)

        await enqueue(r, job_id, Priority.MEDIUM, run_at=run_at)

        r.zadd.assert_called_once()
        assert r.zadd.call_args[0][0] == DELAYED_KEY
        r.lpush.assert_not_called()

    async def test_score_equals_run_at_unix_timestamp(self):
        r = AsyncMock()
        job_id = uuid.uuid4()
        run_at = datetime(2099, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

        await enqueue(r, job_id, Priority.HIGH, run_at=run_at)

        mapping: dict = r.zadd.call_args[0][1]
        score = next(iter(mapping.values()))
        assert score == pytest.approx(run_at.timestamp())

    @pytest.mark.parametrize("priority", [Priority.HIGH, Priority.MEDIUM, Priority.LOW])
    async def test_value_encodes_priority_and_job_id(self, priority):
        r = AsyncMock()
        job_id = uuid.uuid4()
        run_at = datetime(2099, 1, 1, tzinfo=timezone.utc)

        await enqueue(r, job_id, priority, run_at=run_at)

        mapping: dict = r.zadd.call_args[0][1]
        value = next(iter(mapping))
        prefix, encoded_id = value.split(":", 1)
        assert prefix == priority.value
        assert encoded_id == str(job_id)

    async def test_immediate_job_not_stored_in_delayed_set(self):
        r = AsyncMock()
        job_id = uuid.uuid4()

        await enqueue(r, job_id, Priority.HIGH, run_at=None)

        r.zadd.assert_not_called()
        r.lpush.assert_called_once()


# ---------------------------------------------------------------------------
# 2. Lua promote script
# ---------------------------------------------------------------------------


class TestLuaPromoteScript:
    """The Lua script must atomically move due jobs to the right priority queues."""

    @requires_lua
    @pytest.mark.parametrize(
        "priority, expected_queue",
        [
            ("high", _QUEUE_HIGH),
            ("medium", _QUEUE_MEDIUM),
            ("low", _QUEUE_LOW),
        ],
    )
    async def test_past_job_promoted_to_correct_priority_queue(
        self, fake_redis, priority, expected_queue
    ):
        job_id = str(uuid.uuid4())
        past_ts = 1_000_000.0

        await fake_redis.zadd(DELAYED_KEY, {f"{priority}:{job_id}": past_ts})
        promoted = await _promote(fake_redis, past_ts + 1)

        assert promoted == 1
        assert job_id in await fake_redis.lrange(expected_queue, 0, -1)

    @requires_lua
    async def test_future_job_not_promoted(self, fake_redis):
        job_id = str(uuid.uuid4())
        future_ts = 9_999_999_999.0
        now_ts = 1_000_000.0  # well before future_ts

        await fake_redis.zadd(DELAYED_KEY, {f"high:{job_id}": future_ts})
        promoted = await _promote(fake_redis, now_ts)

        assert promoted == 0
        assert await fake_redis.zscore(DELAYED_KEY, f"high:{job_id}") is not None

    @requires_lua
    async def test_only_due_jobs_promoted_from_mixed_set(self, fake_redis):
        past_id = str(uuid.uuid4())
        future_id = str(uuid.uuid4())
        now_ts = 2_000_000.0

        await fake_redis.zadd(
            DELAYED_KEY,
            {
                f"high:{past_id}": now_ts - 1,
                f"medium:{future_id}": now_ts + 3600,
            },
        )
        promoted = await _promote(fake_redis, now_ts)

        assert promoted == 1
        assert past_id in await fake_redis.lrange(_QUEUE_HIGH, 0, -1)
        # future job must still be in delayed
        assert await fake_redis.zscore(DELAYED_KEY, f"medium:{future_id}") is not None

    @requires_lua
    async def test_promoted_job_removed_from_delayed_set(self, fake_redis):
        job_id = str(uuid.uuid4())
        past_ts = 1_000_000.0

        await fake_redis.zadd(DELAYED_KEY, {f"low:{job_id}": past_ts})
        await _promote(fake_redis, past_ts + 1)

        assert await fake_redis.zscore(DELAYED_KEY, f"low:{job_id}") is None

    @requires_lua
    async def test_returns_count_of_promoted_jobs(self, fake_redis):
        now_ts = 5_000_000.0
        ids = [str(uuid.uuid4()) for _ in range(3)]

        for job_id in ids:
            await fake_redis.zadd(DELAYED_KEY, {f"medium:{job_id}": now_ts - 1})

        assert await _promote(fake_redis, now_ts) == 3

    @requires_lua
    async def test_multiple_priorities_promoted_in_single_pass(self, fake_redis):
        high_id, medium_id, low_id = (str(uuid.uuid4()) for _ in range(3))
        past_ts = 1_000_000.0

        await fake_redis.zadd(
            DELAYED_KEY,
            {
                f"high:{high_id}": past_ts,
                f"medium:{medium_id}": past_ts,
                f"low:{low_id}": past_ts,
            },
        )
        promoted = await _promote(fake_redis, past_ts + 1)

        assert promoted == 3
        assert high_id in await fake_redis.lrange(_QUEUE_HIGH, 0, -1)
        assert medium_id in await fake_redis.lrange(_QUEUE_MEDIUM, 0, -1)
        assert low_id in await fake_redis.lrange(_QUEUE_LOW, 0, -1)

    @requires_lua
    async def test_legacy_format_without_separator_falls_back_to_medium(self, fake_redis):
        """Items without a ':' separator must not be dropped — they fall back to medium."""
        raw_id = str(uuid.uuid4())
        past_ts = 1_000_000.0

        await fake_redis.zadd(DELAYED_KEY, {raw_id: past_ts})
        promoted = await _promote(fake_redis, past_ts + 1)

        assert promoted == 1
        assert raw_id in await fake_redis.lrange(_QUEUE_MEDIUM, 0, -1)

    @requires_lua
    async def test_zero_promoted_when_delayed_set_is_empty(self, fake_redis):
        assert await _promote(fake_redis, 9_999_999_999.0) == 0

    @requires_lua
    async def test_job_due_exactly_at_now_is_promoted(self, fake_redis):
        """ZRANGEBYSCORE bounds are inclusive — score == now must be promoted."""
        job_id = str(uuid.uuid4())
        exact_ts = 1_234_567_890.0

        await fake_redis.zadd(DELAYED_KEY, {f"high:{job_id}": exact_ts})
        promoted = await _promote(fake_redis, exact_ts)

        assert promoted == 1
        assert job_id in await fake_redis.lrange(_QUEUE_HIGH, 0, -1)


# ---------------------------------------------------------------------------
# 3. End-to-end: enqueue → promote → queue state
# ---------------------------------------------------------------------------


class TestScheduledJobNotDequeued:
    """A scheduled job must not appear in any priority queue until it is due."""

    @requires_lua
    async def test_future_job_stays_in_delayed_before_promotion(self, fake_redis):
        job_id = uuid.uuid4()
        run_at = datetime.now(tz=timezone.utc) + timedelta(hours=1)

        await enqueue(fake_redis, job_id, Priority.HIGH, run_at=run_at)

        # Promote at current time — job is still in the future
        await _promote(fake_redis, datetime.now(tz=timezone.utc).timestamp())

        # No priority queue should contain this job
        for queue in (_QUEUE_HIGH, _QUEUE_MEDIUM, _QUEUE_LOW):
            assert str(job_id) not in await fake_redis.lrange(queue, 0, -1), (
                f"Future job must not appear in {queue}"
            )

    @requires_lua
    async def test_past_job_lands_in_priority_queue_after_promotion(self, fake_redis):
        job_id = uuid.uuid4()
        run_at = datetime(2000, 1, 1, tzinfo=timezone.utc)  # far in the past

        await enqueue(fake_redis, job_id, Priority.MEDIUM, run_at=run_at)

        promoted = await _promote(fake_redis, datetime.now(tz=timezone.utc).timestamp())

        assert promoted == 1
        assert str(job_id) in await fake_redis.lrange(_QUEUE_MEDIUM, 0, -1)
        # Confirm removed from delayed
        assert await fake_redis.zcard(DELAYED_KEY) == 0

    @requires_lua
    async def test_promoted_job_respects_its_original_priority(self, fake_redis):
        """After promotion, high-priority job must be ahead of low-priority in dequeue order."""
        high_id = uuid.uuid4()
        low_id = uuid.uuid4()
        past = datetime(2000, 1, 1, tzinfo=timezone.utc)

        await enqueue(fake_redis, high_id, Priority.HIGH, run_at=past)
        await enqueue(fake_redis, low_id, Priority.LOW, run_at=past)

        await _promote(fake_redis, datetime.now(tz=timezone.utc).timestamp())

        # Verify queue membership directly (avoids BRPOP timeout concerns)
        assert str(high_id) in await fake_redis.lrange(_QUEUE_HIGH, 0, -1)
        assert str(low_id) in await fake_redis.lrange(_QUEUE_LOW, 0, -1)
        assert str(high_id) not in await fake_redis.lrange(_QUEUE_LOW, 0, -1)
        assert str(low_id) not in await fake_redis.lrange(_QUEUE_HIGH, 0, -1)
