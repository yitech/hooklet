from typing import Any, AsyncIterator, Awaitable, Callable
from pydantic import BaseModel, Field
import uuid
import time

MessageHandlerCallback = Callable[[Any], Awaitable[Any]]
GeneratorFunc = Callable[[], AsyncIterator[dict[str, Any]]]


def get_current_timestamp() -> int:
    return int(time.time() * 1000)


class HookletMessage(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    correlation_id: uuid.UUID | None = None
    type: str | None = None
    payload: Any
    created_at: int = Field(default_factory=lambda: int(time.time()) * 1000)
    start_at: int | None = None
    finish_at: int | None = None

HookletGenerator = Callable[[], AsyncIterator[HookletMessage]]
HookletHandler = Callable[[HookletMessage], AsyncIterator[HookletMessage]]