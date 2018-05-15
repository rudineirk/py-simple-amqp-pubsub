from typing import Any, List

from dataclasses import dataclass, field, replace


class Data:
    def replace(self, **kwargs):
        return replace(self, **kwargs)


@dataclass(frozen=True)
class Event(Data):
    source: str
    topic: str
    payload: Any
    pipe: str = ''
    retry_count: int = 0


@dataclass(frozen=True)
class Source(Data):
    name: str


@dataclass(frozen=True)
class Pipe(Data):
    name: str
    retries_enabled: bool = True
    retries: List[str] = field(
        default_factory=lambda: ['1m', '15m', '1h'],
    )
    exclusive: bool = False
