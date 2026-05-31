import time
from typing import Any, Callable

from jobs.base import BaseJob
from jobs.registry import register


@register
class BatchJob(BaseJob):
    name = "batch_process"
    timeout_seconds = 300

    def run(self, payload: dict[str, Any], on_progress: Callable[[float], None] | None = None) -> dict[str, Any]:
        items: list[Any] = payload.get("items", [])
        if not items:
            return {"processed": 0, "failed": 0, "summary": []}

        results = []
        failed = 0
        for i, item in enumerate(items):
            time.sleep(0.1)
            try:
                results.append({"item": item, "status": "ok"})
            except Exception as exc:
                results.append({"item": item, "status": "error", "error": str(exc)})
                failed += 1

            if on_progress is not None:
                on_progress((i + 1) / len(items) * 100)

        return {
            "processed": len(items),
            "failed": failed,
            "summary": results,
        }
