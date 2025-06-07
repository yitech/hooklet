from typing import Any, AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
import time
import random

@dataclass
class Event:
    id: int = field(default_factory=lambda: random.randint(1, 10**6 - 1))
    target: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    created_at: int = field(default_factory=lambda: int(time.time() * 1000))
    started_at: int = field(default_factory=lambda: int(time.time() * 1000))
    finished_at: int = field(default_factory=lambda: int(time.time() * 1000))


MessageHandlerCallback = Callable[[Any], Awaitable[Any]]
GeneratorFunc = Callable[[], AsyncIterator[dict[str, Any]]]

EventHandlerCallback = Callable[[Event], Awaitable[Event]]
