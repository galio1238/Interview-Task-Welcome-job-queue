# Design Decisions

## 1. Job Pickup Strategy

**Approach chosen:** Redis queue using BRPOP across three priority lists.

**Why:** It is very simple and Redis already has atomic functionality when dequeuing, which ensures only one worker picks up a given job.

**Trade-offs:** BRPOP is destructive — the job is gone from the queue the moment a worker picks it up, so there is no built-in acknowledgment. If a worker crashes after dequeuing but before finishing, the job is silently lost. To handle this I had to build a separate tracking layer (`processing:assignments`) on top.

---

## 2. Worker Crash Recovery

**Approach chosen:** A Redis hash (`processing:assignments`) that maps every in-flight job ID to its worker PID. A background scheduler periodically scans the hash and checks whether each recorded worker is still alive.

**Why:** It is the simplest solution using Redis without reaching for a different tool. The hash gives a single place to see every running job, and PID liveness checks are cheap. PID-based liveness only works because all workers run on the same machine — if workers were spread across multiple hosts, PIDs would be meaningless. One limitation worth noting: as the number of concurrent workers grows, scanning the entire hash on every liveness check gets more expensive. For the scope of this project that is fine, but in a large multi-worker deployment a heartbeat-based approach would scale better.

**What happens if worker crashes mid-job:** The scheduler detects that the worker PID is no longer alive, atomically removes the job from the assignments hash and pushes it back to its priority queue, then resets the job status in the database to `PENDING` so the next available worker can pick it up cleanly.

---

## 3. Priority Queue Implementation

**Approach chosen:** Three separate Redis lists (`queue:high`, `queue:medium`, `queue:low`) with a starvation elevation mechanism that promotes old low/medium jobs to a higher queue after a time threshold.

**Why:** The simplest approach with Redis. `BRPOP` handles priority ordering natively just by the order you pass the keys. The alternative — a single sorted set with a score computed from priority + enqueue time — would require a custom scoring algorithm and more complex dequeue logic. Three lists keeps it straightforward, and the elevation handles the one real problem with strict priority queues (starvation) without adding much complexity.

---

## 4. Retry Backoff Strategy

**Approach chosen:** Exponential backoff — each retry waits longer than the previous one. On failure the job is pushed back into the delayed queue with a future `run_at` time. After exhausting all retries the job is marked `FAILED` and added to a dead letter set.

**Timing:** Base delay is 60 seconds, doubling each attempt (up to 3 retries by default):
- Attempt 1 → 60s
- Attempt 2 → 120s
- Attempt 3 → 240s

---

## 5. One Thing I Would Do Differently With More Time

I skipped health monitoring for Redis itself. Ideally there would be a watchdog that detects when Redis goes down, waits for it to recover, and then reconciles state — re-syncing in-flight jobs from the database back into the queues and restarting workers cleanly. I chose not to implement this because it adds a significant layer of complexity: you have to handle partial state, avoid duplicate entries when rebuilding queues, and coordinate with running processes. Given the scope of the project, that complexity would likely introduce more problems than it solves.
