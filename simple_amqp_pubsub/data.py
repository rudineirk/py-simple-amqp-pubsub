from typing import Any

from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Event:
    service: str
    topic: str
    payload: Any
    retry_count: int = 0

    def replace(self, **kwargs):
        return replace(self, **kwargs)
