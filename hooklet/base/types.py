from typing import Any, Awaitable, Callable

Msg = dict[str, Any]
Headers = dict[str, Any]
AsyncCallback = Callable[[Msg], Awaitable[Any]]


