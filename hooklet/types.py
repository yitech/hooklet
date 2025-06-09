import random
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Optional, Union

from pydantic import BaseModel, Field


class HookletMessage(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    
    id: int = Field(default_factory=lambda: random.randint(1, 10**6 - 1))
    correlation_id: int = Field(default_factory=lambda: random.randint(1, 10**6 - 1))
    target: Optional[str] = None
    payload: Union[bytes, dict[str, Any]] = Field(default_factory=dict)
    created_at: int = Field(default_factory=lambda: int(time.time() * 1000))
    started_at: int = Field(default_factory=lambda: int(time.time() * 1000))
    finished_at: int = Field(default_factory=lambda: int(time.time() * 1000))


MessageHandlerCallback = Callable[[Any], Awaitable[Any]]
GeneratorFunc = Callable[[], AsyncIterator[dict[str, Any]]]

EventHandlerCallback = Callable[[HookletMessage], AsyncIterator[HookletMessage]]
