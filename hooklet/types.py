from typing import Any, AsyncIterator, Awaitable, Callable

MessageHandlerCallback = Callable[[Any], Awaitable[Any]]
GeneratorFunc = Callable[[], AsyncIterator[dict[str, Any]]]
