# AI Tool Usage

## Tools I Used

- **Claude Code (claude-sonnet-4-6)** — primary tool throughout the entire implementation. Used for architecture planning, code generation, debugging, and writing tests.

---

## What Helped Most

**1. System architecture and upfront planning**

Before writing any code I asked Claude to reason through the full architecture — Redis data structures, the priority queue design using `BRPOP`, the job state machine, idempotency with a partial unique index, and the outbox-pattern tradeoffs for DB–Redis write consistency. Claude produced a detailed `PLAN.md` that served as the implementation roadmap. Having the architecture written down and validated before coding saved significant backtracking time, especially for the worker pool (crash recovery, graceful shutdown, and the scheduler running as a separate process).

---

## What I Had to Fix

**1. Worker crash recovery — timing window between DB write and Redis enqueue**

Claude's initial worker logic updated the DB to `RUNNING` immediately after dequeuing, before recording the assignment anywhere else. The problem: if the worker crashed between the `BRPOP` and the DB write, the DB would still show `PENDING` but the job ID was gone from the Redis queue — so the job would never run. I had to reorder the operations: first write the assignment to a Redis hash set, then update the DB. That way, if the worker dies before the DB write, the DB still shows `PENDING` and the job is naturally recoverable without any staleness scanning.

**2. Concurrency bug — duplicate job assignment across workers**

Claude suggested adding an explicit locking mechanism to ensure only one worker could claim a given `job_id` at a time, introducing extra complexity (a Redis `SET NX` lock around the dequeue step). This was unnecessary — `BRPOP` is atomic by design: Redis pops the job ID from the list in a single operation, so only one worker ever receives a given ID. The "fix" Claude proposed was solving a problem that didn't exist. I had to point out that the atomicity guarantee was already built into the dequeue path and remove the extra locking code.

---

## What AI Struggled With

**Scheduler test isolation.** The scheduler tests were initially flaky because they shared Redis state with the worker-loop tests — Claude generated the fixtures without a per-test Redis flush, so delayed-queue entries from one test bled into the next. This required manual intervention to add explicit cleanup in `conftest.py`. Claude also initially suggested mocking `time.time()` for the scheduler promotion logic, which would have made the tests pass without testing the actual Lua script that runs atomically in Redis. I replaced those with real-time tests using short `run_at` offsets instead.
