from enum import Enum
from hooklet.logger import get_node_logger
from typing import Coroutine
import asyncio
from abc import ABC, abstractmethod
from .pilot import Pilot, Msg
from .types import AsyncCallback

class EventType(Enum):
    """
    Event types for nodes.
    """
    START = "start"
    CLOSE = "close"
    FINISH = "finish"
    ERROR = "error"


class Node:
    def __init__(self, name: str):
        self.name = name
        self.task: asyncio.Task | None = None
        self.event_handlers: dict[EventType, list[Coroutine]] = {
            EventType.START: [],
            EventType.MESSAGE: [],
            EventType.CLOSE: [],
            EventType.FINISH: [],
            EventType.ERROR: [],
        }
        self.shutdown_event = asyncio.Event()
        self.logger = get_node_logger(self.name)
        
    
    @property
    def name(self) -> str:
        return self.name
    

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
        while not self.shutdown_event.is_set():
            await asyncio.sleep(1)

    async def close(self):
        self.shutdown_event.set()
        await asyncio.wait_for(self.task, timeout=2)
        if self.task is not None and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    async def on_finish(self):
        """
        This method is called when the node is finished.
        """
        for coroutine in self.event_handlers[EventType.FINISH]:
            await coroutine
        self.event_handlers[EventType.FINISH].clear()

    def register(self, event_type: EventType, coroutine: Coroutine):
        self.event_handlers[event_type].append(coroutine)

    async def on_error(self, error: Exception):
        self.logger.error(f"Error in {self.name}: {error}", exc_info=True)
        
