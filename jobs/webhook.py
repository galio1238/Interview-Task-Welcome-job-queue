import random
import time
from typing import Any, Callable

from jobs.base import BaseJob
from jobs.registry import register


@register
class WebhookJob(BaseJob):
    name = "send_webhook"
    timeout_seconds = 30

    def run(self, payload: dict[str, Any], on_progress: Callable[[float], None] | None = None) -> dict[str, Any]:
        time.sleep(random.uniform(1, 2))
        if random.random() < 0.2:
            raise RuntimeError("Simulated webhook delivery failure (20% chance)")
        return {
            "delivered": True,
            "url": payload.get("url", "https://example.com/webhook"),
            "status_code": 200,
        }
