from abc import ABC, abstractmethod
from typing import Any, Callable


class BaseJob(ABC):
    name: str
    timeout_seconds: int | None = None

    @abstractmethod
    def run(self, payload: dict[str, Any], on_progress: Callable[[float], None] | None = None) -> dict[str, Any]:
        ...
