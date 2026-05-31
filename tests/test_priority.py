"""
Tests for priority queue ordering.

The priority guarantee rests on two invariants:
  1. enqueue() routes each priority to its own Redis key (queue:high / medium / low).
  2. dequeue() calls BRPOP with those keys listed high → medium → low, so Redis
     naturally drains the highest non-empty queue first.

Tests are split into three classes:
  - TestEnqueueRouting   : correct key is written for each priority (and for delayed jobs)
  - TestDequeueOrdering  : BRPOP is called with keys in the right order
  - TestPriorityOrdering : end-to-end via FakeRedis — jobs dequeued in priority order
"""

import uuid
from collections import defaultdict
from datetime import datetime, timezone
from unittest.mock import AsyncMock, call, patch

import pytest

from db.models import Priority
from rqueue.dequeue import PRIORITY_QUEUES, dequeue
from rqueue.enqueue import DELAYED_KEY, enqueue


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeRedis:
    """
    In-memory simulation of the Redis list + sorted-set primitives used by
    enqueue() and dequeue().  Reproduces LPUSH + BRPOP FIFO semantics and
    BRPOP's key-ordering priority rule (first non-empty key wins).
    """

    def __init__(self):
        self._lists: dict[str, list[str]] = defaultdict(list)
        self._zsets: dict[str, dict[str, float]] = defaultdict(dict)

    async def lpush(self, key: str, *values: str) -> int:
        for value in values:
            self._lists[key].insert(0, str(value))
        return len(self._lists[key])

    async def zadd(self, key: str, mapping: dict) -> int:
        self._zsets[key].update({str(k): float(v) for k, v in mapping.items()})
        return len(mapping)

    async def brpop(self, keys, timeout: int = 0):
        for key in keys:
            if self._lists[key]:
                return (key, self._lists[key].pop())
        return None

    def queue_len(self, key: str) -> int:
        return len(self._lists[key])


# ---------------------------------------------------------------------------
# 1. Enqueue routing
# ---------------------------------------------------------------------------


class TestEnqueueRouting:
    """enqueue() must write to the correct Redis key for every priority."""

    @pytest.mark.parametrize(
        "priority, expected_key",
        [
            (Priority.HIGH, "queue:high"),
            (Priority.MEDIUM, "queue:medium"),
            (Priority.LOW, "queue:low"),
        ],
    )
    async def test_immediate_job_routed_to_priority_key(self, priority, expected_key):
        r = AsyncMock()
        job_id = uuid.uuid4()

        await enqueue(r, job_id, priority, run_at=None)

        r.lpush.assert_called_once_with(expected_key, str(job_id))
        r.zadd.assert_not_called()

    @pytest.mark.parametrize("priority", [Priority.HIGH, Priority.MEDIUM, Priority.LOW])
    async def test_delayed_job_goes_to_delayed_key_regardless_of_priority(self, priority):
        r = AsyncMock()
        job_id = uuid.uuid4()
        run_at = datetime(2099, 1, 1, tzinfo=timezone.utc)

        await enqueue(r, job_id, priority, run_at=run_at)

        r.zadd.assert_called_once()
        assert r.zadd.call_args[0][0] == DELAYED_KEY
        r.lpush.assert_not_called()

    async def test_delayed_job_encodes_priority_in_value(self):
        """Scheduler needs to know which queue to promote the job into."""
        r = AsyncMock()
        job_id = uuid.uuid4()
        run_at = datetime(2099, 1, 1, tzinfo=timezone.utc)

        await enqueue(r, job_id, Priority.HIGH, run_at=run_at)

        mapping: dict = r.zadd.call_args[0][1]
        value = next(iter(mapping))
        assert value.startswith("high:"), (
            f"Delayed job value must include priority prefix, got: {value!r}"
        )


# ---------------------------------------------------------------------------
# 2. Dequeue key ordering
# ---------------------------------------------------------------------------


class TestDequeueOrdering:
    """dequeue() must pass keys to BRPOP in high → medium → low order."""

    async def test_brpop_called_with_priority_keys_in_order(self):
        r = AsyncMock()
        r.brpop.return_value = ("queue:high", str(uuid.uuid4()))

        await dequeue(r, timeout=5)

        r.brpop.assert_called_once_with(PRIORITY_QUEUES, timeout=5)

    async def test_priority_queues_constant_is_high_medium_low(self):
        assert PRIORITY_QUEUES == ["queue:high", "queue:medium", "queue:low"]

    async def test_dequeue_returns_none_when_all_queues_empty(self):
        r = AsyncMock()
        r.brpop.return_value = None

        result = await dequeue(r, timeout=1)

        assert result is None

    async def test_dequeue_returns_uuid_from_brpop_result(self):
        job_id = uuid.uuid4()
        r = AsyncMock()
        r.brpop.return_value = ("queue:high", str(job_id))

        result = await dequeue(r, timeout=5)

        assert result == job_id


# ---------------------------------------------------------------------------
# 3. End-to-end priority ordering via FakeRedis
# ---------------------------------------------------------------------------


class TestPriorityOrdering:
    """
    Enqueue jobs out-of-priority order; dequeue them and assert the order
    matches high → medium → low.
    """

    async def test_high_dequeued_before_medium_before_low(self):
        r = FakeRedis()
        ids = {p: uuid.uuid4() for p in [Priority.LOW, Priority.HIGH, Priority.MEDIUM]}

        # Enqueue in a deliberately wrong order
        await enqueue(r, ids[Priority.LOW], Priority.LOW)
        await enqueue(r, ids[Priority.HIGH], Priority.HIGH)
        await enqueue(r, ids[Priority.MEDIUM], Priority.MEDIUM)

        first = await dequeue(r, timeout=0)
        second = await dequeue(r, timeout=0)
        third = await dequeue(r, timeout=0)

        assert first == ids[Priority.HIGH], "high must be dequeued first"
        assert second == ids[Priority.MEDIUM], "medium must be dequeued second"
        assert third == ids[Priority.LOW], "low must be dequeued third"

    async def test_multiple_high_priority_jobs_drained_before_lower(self):
        r = FakeRedis()
        high_1, high_2 = uuid.uuid4(), uuid.uuid4()
        low = uuid.uuid4()

        await enqueue(r, low, Priority.LOW)
        await enqueue(r, high_1, Priority.HIGH)
        await enqueue(r, high_2, Priority.HIGH)

        first = await dequeue(r, timeout=0)
        second = await dequeue(r, timeout=0)
        third = await dequeue(r, timeout=0)

        assert first in (high_1, high_2)
        assert second in (high_1, high_2)
        assert first != second
        assert third == low

    async def test_low_priority_processed_when_higher_queues_empty(self):
        r = FakeRedis()
        job_id = uuid.uuid4()

        await enqueue(r, job_id, Priority.LOW)

        result = await dequeue(r, timeout=0)

        assert result == job_id

    async def test_returns_none_after_all_jobs_consumed(self):
        r = FakeRedis()
        job_id = uuid.uuid4()

        await enqueue(r, job_id, Priority.MEDIUM)

        await dequeue(r, timeout=0)
        result = await dequeue(r, timeout=0)

        assert result is None
