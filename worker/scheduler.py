import asyncio
import logging
import os
import signal
import time

from config import settings
from rqueue.client import get_redis
from worker.logging_config import setup_logging

logger = logging.getLogger(__name__)

_DELAYED_KEY = "queue:delayed"
_QUEUE_HIGH = "queue:high"
_QUEUE_MEDIUM = "queue:medium"
_QUEUE_LOW = "queue:low"

# Atomically promote all due jobs from the delayed sorted set into their
# priority queues. Returns the number of jobs promoted.
_PROMOTE_SCRIPT = """
local items = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
local count = 0
for _, item in ipairs(items) do
    redis.call('ZREM', KEYS[1], item)
    local sep = string.find(item, ':')
    if sep then
        local priority = string.sub(item, 1, sep - 1)
        local job_id   = string.sub(item, sep + 1)
        if priority == 'high' then
            redis.call('LPUSH', KEYS[2], job_id)
        elseif priority == 'medium' then
            redis.call('LPUSH', KEYS[3], job_id)
        else
            redis.call('LPUSH', KEYS[4], job_id)
        end
    else
        -- Legacy / unknown format: fall back to medium queue.
        redis.call('LPUSH', KEYS[3], item)
    end
    count = count + 1
end
return count
"""

_shutdown_requested = False


def _handle_sigterm(signum: int, frame: object) -> None:
    global _shutdown_requested
    _shutdown_requested = True


async def _run_scheduler() -> None:
    r = get_redis()
    promote = r.register_script(_PROMOTE_SCRIPT)

    logger.info("scheduler.started", extra={"event": "scheduler.started", "worker_pid": os.getpid()})

    while not _shutdown_requested:
        now = time.time()
        promoted = await promote(
            keys=[_DELAYED_KEY, _QUEUE_HIGH, _QUEUE_MEDIUM, _QUEUE_LOW],
            args=[now],
        )
        if promoted:
            logger.info(
                "scheduler.promoted",
                extra={"event": "scheduler.promoted", "count": promoted, "worker_pid": os.getpid()},
            )
        await asyncio.sleep(settings.scheduler_interval)

    logger.info("scheduler.shutdown", extra={"event": "scheduler.shutdown", "worker_pid": os.getpid()})


def main() -> None:
    setup_logging(settings.log_level)
    signal.signal(signal.SIGTERM, _handle_sigterm)
    asyncio.run(_run_scheduler())


if __name__ == "__main__":
    main()
