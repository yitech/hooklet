from typing import Any, Awaitable, Callable

Msg = dict[str, Any]
AsyncCallback = Callable[[Msg], Awaitable[Any]]



