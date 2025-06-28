from typing import Any, Awaitable, Callable

Callback = Callable[[Any], Any]
AsyncCallback = Callable[[Any], Awaitable[Any]]

