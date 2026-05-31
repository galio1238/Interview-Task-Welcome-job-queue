"""
Tests for priority queue ordering and starvation prevention.

The priority guarantee rests on two invariants:
  1. enqueue() routes each priority to its own Redis key (queue:high / medium / low).
  2. dequeue() calls BRPOP with those keys listed high → medium → low, so Redis
     naturally drains the highest non-empty queue first.

Starvation prevention adds:
  3. enqueue() registers low/medium jobs in aging sorted sets so wait time is tracked.
  4. dequeue() removes a job from its aging set immediately after pickup.
  5. The elevation script atomically moves stale jobs to a higher-priority queue.

Tests are split into five classes:
  - TestEnqueueRouting        : correct key is written for each priority (and for delayed jobs)
  - TestDequeueOrdering       : BRPOP is called with keys in the right order
  - TestPriorityOrdering      : end-to-end via FakeRedis — jobs dequeued in priority order
  - TestEnqueueAging          : low/medium jobs are registered in the aging sorted sets
  - TestDequeueAgingCleanup   : dequeue removes the job from its aging set
  - TestElevation             : stale jobs are moved to higher-priority queues
"""

import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from unittest.mock import AsyncMock, call, patch

import pytest

from db.models import Priority
from rqueue.dequeue import PRIORITY_QUEUES, dequeue
from rqueue.enqueue import AGING_LOW_KEY, AGING_MEDIUM_KEY, DELAYED_KEY, enqueue


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

    async def zrem(self, key: str, *members: str) -> int:
        removed = 0
        for m in members:
            if str(m) in self._zsets[key]:
                del self._zsets[key][str(m)]
                removed += 1
        return removed

    async def brpop(self, keys, timeout: int = 0):
        for key in keys:
            if self._lists[key]:
                return (key, self._lists[key].pop())
        return None

    # Sync helpers used by _simulate_elevation and test assertions.

    def lrem(self, key: str, count: int, value: str) -> int:
        to_remove = str(value)
        removed = 0
        new_lst = []
        for item in self._lists[key]:
            if item == to_remove and (count == 0 or removed < count):
                removed += 1
            else:
                new_lst.append(item)
        self._lists[key] = new_lst
        return removed

    def zset_members(self, key: str) -> set[str]:
        return set(self._zsets[key].keys())

    def queue_len(self, key: str) -> int:
        return len(self._lists[key])


def _simulate_elevation(r: FakeRedis, now: float, low_thr: float, med_thr: float) -> int:
    """
    Python equivalent of _ELEVATION_SCRIPT for unit testing.

    Mirrors the Lua logic exactly:
      - stale low jobs → medium queue, aging clock reset to now
      - stale medium jobs → high queue, removed from aging
      - already-dequeued jobs (LREM returns 0) → cleaned from aging, not re-queued
    """
    count = 0

    stale_low = [jid for jid, score in r._zsets[AGING_LOW_KEY].items() if score <= now - low_thr]
    for jid in stale_low:
        n = r.lrem("queue:low", 1, jid)
        r._zsets[AGING_LOW_KEY].pop(jid, None)
        if n > 0:
            r._lists["queue:medium"].insert(0, jid)
            r._zsets[AGING_MEDIUM_KEY][jid] = now
            count += 1

    stale_med = [jid for jid, score in r._zsets[AGING_MEDIUM_KEY].items() if score <= now - med_thr]
    for jid in stale_med:
        n = r.lrem("queue:medium", 1, jid)
        r._zsets[AGING_MEDIUM_KEY].pop(jid, None)
        if n > 0:
            r._lists["queue:high"].insert(0, jid)
            count += 1

    return count


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
        # High-priority jobs skip the aging sets; low/medium are tracked for elevation.
        if priority == Priority.HIGH:
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


# ---------------------------------------------------------------------------
# 4. Enqueue aging registration
# ---------------------------------------------------------------------------


class TestEnqueueAging:
    """enqueue() must register low/medium jobs in the aging sorted sets."""

    async def test_low_job_added_to_aging_low_set(self):
        r = FakeRedis()
        job_id = uuid.uuid4()
        await enqueue(r, job_id, Priority.LOW)
        assert str(job_id) in r.zset_members(AGING_LOW_KEY)

    async def test_medium_job_added_to_aging_medium_set(self):
        r = FakeRedis()
        job_id = uuid.uuid4()
        await enqueue(r, job_id, Priority.MEDIUM)
        assert str(job_id) in r.zset_members(AGING_MEDIUM_KEY)

    async def test_high_job_not_added_to_any_aging_set(self):
        r = FakeRedis()
        job_id = uuid.uuid4()
        await enqueue(r, job_id, Priority.HIGH)
        assert r.zset_members(AGING_LOW_KEY) == set()
        assert r.zset_members(AGING_MEDIUM_KEY) == set()

    async def test_aging_score_is_approximately_now(self):
        r = FakeRedis()
        job_id = uuid.uuid4()
        before = time.time()
        await enqueue(r, job_id, Priority.LOW)
        after = time.time()
        score = r._zsets[AGING_LOW_KEY][str(job_id)]
        assert before <= score <= after


# ---------------------------------------------------------------------------
# 5. Dequeue aging cleanup
# ---------------------------------------------------------------------------


class TestDequeueAgingCleanup:
    """dequeue() must remove the job from its aging set immediately after pickup."""

    async def test_dequeue_low_removes_from_aging_low(self):
        r = FakeRedis()
        job_id = uuid.uuid4()
        await enqueue(r, job_id, Priority.LOW)

        await dequeue(r, timeout=0)

        assert str(job_id) not in r.zset_members(AGING_LOW_KEY)

    async def test_dequeue_medium_removes_from_aging_medium(self):
        r = FakeRedis()
        job_id = uuid.uuid4()
        await enqueue(r, job_id, Priority.MEDIUM)

        await dequeue(r, timeout=0)

        assert str(job_id) not in r.zset_members(AGING_MEDIUM_KEY)

    async def test_dequeue_high_does_not_touch_aging_sets(self):
        r = FakeRedis()
        job_id = uuid.uuid4()
        await enqueue(r, job_id, Priority.HIGH)

        await dequeue(r, timeout=0)

        assert r.zset_members(AGING_LOW_KEY) == set()
        assert r.zset_members(AGING_MEDIUM_KEY) == set()

    async def test_aging_set_empty_after_all_jobs_dequeued(self):
        r = FakeRedis()
        ids = [uuid.uuid4() for _ in range(3)]
        for job_id in ids:
            await enqueue(r, job_id, Priority.LOW)

        for _ in ids:
            await dequeue(r, timeout=0)

        assert r.zset_members(AGING_LOW_KEY) == set()


# ---------------------------------------------------------------------------
# 6. Elevation (starvation prevention)
# ---------------------------------------------------------------------------


class TestElevation:
    """Stale jobs are moved to higher-priority queues to prevent starvation."""

    LOW_THR = 300.0
    MED_THR = 600.0

    async def test_stale_low_job_moved_to_medium_queue(self):
        r = FakeRedis()
        job_id = uuid.uuid4()
        await r.lpush("queue:low", str(job_id))
        await r.zadd(AGING_LOW_KEY, {str(job_id): time.time() - 400})

        _simulate_elevation(r, time.time(), self.LOW_THR, self.MED_THR)

        assert r.queue_len("queue:medium") == 1
        assert r.queue_len("queue:low") == 0

    async def test_stale_low_job_removed_from_aging_low(self):
        r = FakeRedis()
        job_id = uuid.uuid4()
        await r.lpush("queue:low", str(job_id))
        await r.zadd(AGING_LOW_KEY, {str(job_id): time.time() - 400})

        _simulate_elevation(r, time.time(), self.LOW_THR, self.MED_THR)

        assert str(job_id) not in r.zset_members(AGING_LOW_KEY)

    async def test_elevated_low_job_tracked_in_aging_medium(self):
        r = FakeRedis()
        job_id = uuid.uuid4()
        await r.lpush("queue:low", str(job_id))
        await r.zadd(AGING_LOW_KEY, {str(job_id): time.time() - 400})

        _simulate_elevation(r, time.time(), self.LOW_THR, self.MED_THR)

        assert str(job_id) in r.zset_members(AGING_MEDIUM_KEY)

    async def test_elevation_resets_medium_aging_clock(self):
        """After low→medium elevation the aging clock in medium starts at now, not the original enqueue time."""
        r = FakeRedis()
        job_id = uuid.uuid4()
        await r.lpush("queue:low", str(job_id))
        await r.zadd(AGING_LOW_KEY, {str(job_id): time.time() - 400})

        now = time.time()
        _simulate_elevation(r, now, self.LOW_THR, self.MED_THR)

        score = r._zsets[AGING_MEDIUM_KEY][str(job_id)]
        assert abs(score - now) < 1.0

    async def test_fresh_low_job_not_elevated(self):
        r = FakeRedis()
        job_id = uuid.uuid4()
        await enqueue(r, job_id, Priority.LOW)

        _simulate_elevation(r, time.time(), self.LOW_THR, self.MED_THR)

        assert r.queue_len("queue:low") == 1
        assert r.queue_len("queue:medium") == 0

    async def test_stale_medium_job_moved_to_high_queue(self):
        r = FakeRedis()
        job_id = uuid.uuid4()
        await r.lpush("queue:medium", str(job_id))
        await r.zadd(AGING_MEDIUM_KEY, {str(job_id): time.time() - 700})

        _simulate_elevation(r, time.time(), self.LOW_THR, self.MED_THR)

        assert r.queue_len("queue:high") == 1
        assert r.queue_len("queue:medium") == 0

    async def test_stale_medium_job_removed_from_aging_medium(self):
        r = FakeRedis()
        job_id = uuid.uuid4()
        await r.lpush("queue:medium", str(job_id))
        await r.zadd(AGING_MEDIUM_KEY, {str(job_id): time.time() - 700})

        _simulate_elevation(r, time.time(), self.LOW_THR, self.MED_THR)

        assert str(job_id) not in r.zset_members(AGING_MEDIUM_KEY)

    async def test_already_dequeued_job_cleaned_from_aging_without_requeue(self):
        """If a job was already picked up (not in the list), elevation cleans its aging
        entry but does NOT push it back to any queue."""
        r = FakeRedis()
        job_id = uuid.uuid4()
        # Aging entry exists but the job is no longer in the list (already dequeued).
        await r.zadd(AGING_LOW_KEY, {str(job_id): time.time() - 400})

        _simulate_elevation(r, time.time(), self.LOW_THR, self.MED_THR)

        assert str(job_id) not in r.zset_members(AGING_LOW_KEY)
        assert r.queue_len("queue:medium") == 0

    async def test_elevation_returns_count_of_moved_jobs(self):
        r = FakeRedis()
        ids = [uuid.uuid4() for _ in range(3)]
        for job_id in ids:
            await r.lpush("queue:low", str(job_id))
            await r.zadd(AGING_LOW_KEY, {str(job_id): time.time() - 400})

        count = _simulate_elevation(r, time.time(), self.LOW_THR, self.MED_THR)

        assert count == 3
