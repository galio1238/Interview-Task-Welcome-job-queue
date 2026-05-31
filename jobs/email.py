import random
import time
import uuid
from typing import Any, Callable

from jobs.base import BaseJob
from jobs.registry import register


@register
class SendEmailJob(BaseJob):
    name = "send_email"
    timeout_seconds = 30

    def run(self, payload: dict[str, Any], on_progress: Callable[[float], None] | None = None) -> dict[str, Any]:
        time.sleep(random.uniform(1, 3))
        return {
            "sent": True,
            "message_id": str(uuid.uuid4()),
            "to": payload.get("to", "unknown@example.com"),
            "subject": payload.get("subject", "(no subject)"),
        }
