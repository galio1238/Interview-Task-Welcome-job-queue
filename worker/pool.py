import logging
import multiprocessing
import os
import signal
import time

from config import settings
from worker.logging_config import setup_logging
from worker.process import main as worker_main
from worker.scheduler import main as scheduler_main

logger = logging.getLogger(__name__)

_CRASH_CHECK_INTERVAL = 5   # seconds between liveness checks
_GRACEFUL_SHUTDOWN_TIMEOUT = 30  # seconds to wait for workers after SIGTERM

_shutdown_requested = False


def _handle_signal(signum: int, frame: object) -> None:
    global _shutdown_requested
    _shutdown_requested = True


def _spawn_worker(index: int) -> multiprocessing.Process:
    p = multiprocessing.Process(target=worker_main, name=f"worker-{index}", daemon=False)
    p.start()
    logger.info(
        "worker.spawned",
        extra={"event": "worker.spawned", "worker_index": index, "pid": p.pid},
    )
    return p


def _spawn_scheduler() -> multiprocessing.Process:
    p = multiprocessing.Process(target=scheduler_main, name="scheduler", daemon=False)
    p.start()
    logger.info("scheduler.spawned", extra={"event": "scheduler.spawned", "pid": p.pid})
    return p


def _terminate_all(processes: list[multiprocessing.Process]) -> None:
    for p in processes:
        if p.is_alive():
            p.terminate()

    deadline = time.monotonic() + _GRACEFUL_SHUTDOWN_TIMEOUT
    for p in processes:
        remaining = max(0.0, deadline - time.monotonic())
        p.join(timeout=remaining)
        if p.is_alive():
            logger.warning(
                "worker.sigkill",
                extra={"event": "worker.sigkill", "pid": p.pid, "name": p.name},
            )
            p.kill()
            p.join()


def main() -> None:
    setup_logging(settings.log_level)
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    logger.info(
        "pool.started",
        extra={"event": "pool.started", "worker_count": settings.worker_count, "pid": os.getpid()},
    )

    workers: list[multiprocessing.Process] = [
        _spawn_worker(i) for i in range(settings.worker_count)
    ]
    scheduler = _spawn_scheduler()
    all_processes = workers + [scheduler]

    try:
        while not _shutdown_requested:
            time.sleep(_CRASH_CHECK_INTERVAL)

            for i, w in enumerate(workers):
                if not w.is_alive():
                    logger.warning(
                        "worker.crashed",
                        extra={"event": "worker.crashed", "pid": w.pid, "worker_index": i},
                    )
                    workers[i] = _spawn_worker(i)
                    logger.info(
                        "worker.respawned",
                        extra={"event": "worker.respawned", "worker_index": i, "pid": workers[i].pid},
                    )

            if not scheduler.is_alive():
                logger.warning("scheduler.crashed", extra={"event": "scheduler.crashed", "pid": scheduler.pid})
                scheduler = _spawn_scheduler()

            all_processes = workers + [scheduler]

    finally:
        logger.info("pool.shutdown", extra={"event": "pool.shutdown", "pid": os.getpid()})
        _terminate_all(all_processes)


if __name__ == "__main__":
    main()
