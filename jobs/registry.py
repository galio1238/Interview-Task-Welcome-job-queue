from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from jobs.base import BaseJob

REGISTRY: dict[str, type["BaseJob"]] = {}


def register(cls: type["BaseJob"]) -> type["BaseJob"]:
    REGISTRY[cls.name] = cls
    return cls


def load_all() -> None:
    """Import all job modules so their @register decorators fire."""
    import jobs.email  # noqa: F401
    import jobs.webhook  # noqa: F401
    import jobs.report  # noqa: F401
    import jobs.batch  # noqa: F401
