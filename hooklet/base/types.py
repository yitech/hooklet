from typing import Any, Awaitable, Callable

Msg = dict[str, Any]
Headers = dict[str, Any]
Callback = Callable[[Msg], Any]
AsyncCallback = Callable[[Msg], Awaitable[Any]]


