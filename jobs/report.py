import random
import time
import uuid
from typing import Any, Callable

from jobs.base import BaseJob
from jobs.registry import register


@register
class GenerateReportJob(BaseJob):
    name = "generate_report"
    timeout_seconds = 60

    def run(self, payload: dict[str, Any], on_progress: Callable[[float], None] | None = None) -> dict[str, Any]:
        time.sleep(random.uniform(3, 5))
        report_id = str(uuid.uuid4())
        return {
            "file_url": f"https://storage.example.com/reports/{report_id}.pdf",
            "report_id": report_id,
            "name": payload.get("name", "report"),
            "format": payload.get("format", "pdf"),
        }
