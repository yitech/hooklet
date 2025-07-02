import asyncio
from abc import ABC, abstractmethod
from enum import Enum
from typing import Coroutine

from hooklet.logger import get_node_logger


class EventType(Enum):
    """
    Event types for nodes.
    """

    START = "start"
    CLOSE = "close"
    FINISH = "finish"
    ERROR = "error"


class Node(ABC):
    def __init__(self, name: str):
        self._name = name
        self.task: asyncio.Task | None = None
        self.event_handlers: dict[EventType, list[Coroutine]] = {
            EventType.START: [],
            EventType.CLOSE: [],
            EventType.FINISH: [],
            EventType.ERROR: [],
        }
        self.shutdown_event = asyncio.Event()
        self.logger = get_node_logger(self._name)

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_running(self) -> bool:
        return self.task is not None and not self.task.done()

    async def start(self):
        for coroutine in self.event_handlers[EventType.START]:
            await coroutine
        self.event_handlers[EventType.START].clear()
        self.task = asyncio.create_task(self.run())

    @abstractmethod
    async def run(self):
        """
        This method is called when the node is started. Expect as a blocking call.
        """
        raise NotImplementedError("Subclasses must implement this method")

    async def close(self):
        self.shutdown_event.set()
        await self.on_finish()

    async def on_finish(self):
        """
        This method is called when the node is finished.
        """
        try:
            await asyncio.wait_for(self.task, timeout=2)
        except asyncio.TimeoutError:
            self.task.cancel()
        try:
            await self.task
        except asyncio.CancelledError:
            pass
        for coroutine in self.event_handlers[EventType.FINISH]:
            await coroutine
        self.event_handlers[EventType.FINISH].clear()

    def register(self, event_type: EventType, coroutine: Coroutine):
        self.event_handlers[event_type].append(coroutine)

    async def on_error(self, error: Exception):
        self.logger.error(f"Error in {self.name}: {error}", exc_info=True)
