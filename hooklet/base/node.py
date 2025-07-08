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
    ERROR = "error"


class Node(ABC):
    def __init__(self, name: str):
        self._name = name
        self._is_running = False
        self.event_handlers: dict[EventType, list[Coroutine]] = {
            EventType.START: [],
            EventType.CLOSE: [],
            EventType.ERROR: [],
        }
        self.logger = get_node_logger(self._name)

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_running(self) -> bool:
        return self._is_running

    async def start(self):
        for coroutine in self.event_handlers[EventType.START]:
            await coroutine
        self.event_handlers[EventType.START].clear()
        await self.on_start()
        self._is_running = True

    @abstractmethod
    async def on_start(self):
        """
        This method is called when the node is started.
        """
        raise NotImplementedError("Subclasses must implement this method")

    async def close(self):
        """
        This method is called when the node is finished.
        """
        self._is_running = False
        for coroutine in self.event_handlers[EventType.CLOSE]:
            await coroutine
        self.event_handlers[EventType.CLOSE].clear()
        await self.on_close()

    @abstractmethod
    async def on_close(self):
        raise NotImplementedError("Subclasses must implement this method")

    def register(self, event_type: EventType, coroutine: Coroutine):
        self.event_handlers[event_type].append(coroutine)

    async def on_error(self, error: Exception):
        self.logger.error(f"Error in {self.name}: {error}", exc_info=True)
