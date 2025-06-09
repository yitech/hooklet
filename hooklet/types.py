from typing import Any, AsyncIterator, Awaitable, Callable
from pydantic import BaseModel
import uuid
import time

MessageHandlerCallback = Callable[[Any], Awaitable[Any]]
GeneratorFunc = Callable[[], AsyncIterator[dict[str, Any]]]


class HookletMessage(BaseModel):
    id: uuid.UUID = uuid.uuid4()
    correlation_id: uuid.UUID | None = None
    target: str | None = None
    payload: Any
    created_at: int = lambda: int(time.time() * 1000)
    start_at: int | None = None
    finish_at: int | None = None

HookletGenerator = Callable[[], AsyncIterator[HookletMessage]]
HandlerHandler = Callable[[HookletMessage], AsyncIterator[HookletMessage]]