import random
import time
import uuid
from typing import Any, AsyncIterator, Awaitable, Callable

from pydantic import BaseModel, Field

MessageHandlerCallback = Callable[[Any], Awaitable[Any]]
GeneratorFunc = Callable[[], AsyncIterator[dict[str, Any]]]


class HookletMessage(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    node_id: str | None = None
    correlation_id: int = Field(default_factory=lambda: random.randint(0, 2**64 - 1))
    type: str | None = None
    payload: Any
    start_at: int = Field(default_factory=lambda: int(time.time() * 1000))
    finish_at: int = Field(default_factory=lambda: -1)


HookletGenerator = Callable[[], AsyncIterator[HookletMessage]]
HookletHandler = Callable[[HookletMessage], AsyncIterator[HookletMessage]]
