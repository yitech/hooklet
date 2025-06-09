import asyncio
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Callable, Literal

from hooklet.base import BaseEventrix, BasePilot
from hooklet.types import HookletHandler, HookletGenerator, HookletMessage

class Node(BaseEventrix, ABC):
    """
    Base class for nodes.
    """
    
    def __init__(self, pilot: BasePilot, sources: list[str], router: Callable[[HookletMessage], str | None], node_id: None | str = None):
        super().__init__(pilot, node_id)
        self.sources = sources
        self.router = router

        self._shutdown_event = asyncio.Event()
        self._generator_task: asyncio.Task | None = None
        self._handler_task: asyncio.Task | None = None
        
    @property
    def node_id(self) -> str:
        return self._executor_id
    
    @property
    def is_running(self) -> bool:
        return not self._shutdown_event.is_set()
    
    async def generator_func(self) -> AsyncIterator[HookletMessage]:
        """
        Default generator function that yields nothing.
        Override this method to provide custom generator behavior.
        """
        yield 
    
    async def handler_func(self, message: HookletMessage) -> AsyncIterator[HookletMessage]:    
        """
        Default handler function that returns the input message unchanged.
        Override this method to provide custom handler behavior.
        """
        yield 
    
    async def _run_generator(self) -> None:
        """
        Get the source generator.
        """
        try:
            async for message in self.generator_func():
                self.pilot.publish(self.router(message), message)
        except Exception as e:
            self.logger.error(f"Error in generator: {e}")
            self.on_error(e)
        finally:
            self.logger.info(f"Generator {self.node_id} finished")
    
    
    async def _message_handler(self, encoded_message: str) -> None:
        """
        Handle incoming messages from sources.
        """
        try:
            message = HookletMessage.model_validate_json(encoded_message)
            async for processed_message in self.handler_func(message):
                if processed_message is not None:
                    subject = self.router(processed_message)
                    self.pilot.publish(subject, processed_message.model_dump_json())
        except Exception as e:
            self.logger.error(f"Error processing message in {self.node_id}: {e}")
            self.on_error(e)

    async def _register_handlers(self) -> None:
        """
        Register all handlers defined in get_handlers().
        """
        try:
            for source in self.sources:
                self.pilot.register_handler(source, self._message_handler)
        except Exception as e:
            self.logger.error(f"Error in handler {self.node_id}: {str(e)}", exc_info=True)
            raise
    
    async def on_start(self) -> None:
        """
        Called when the node starts.
        """
        self.logger.info(f"Starting node {self.node_id}")
        self._shutdown_event.clear()
        await self._register_handlers()
    
    async def on_execute(self) -> None:
        """
        Called when the node executes.
        """
        self.logger.info(f"Executing node {self.node_id}")
        if self._generator_task:
            self._generator_task.cancel()
        self._generator_task = asyncio.create_task(self._get_generator())
        if self._handler_task:
            self._handler_task.cancel()
        self._handler_task = asyncio.create_task(self._get_handler(self._generator_task))
        self._shutdown_event.wait()
    
    async def on_stop(self) -> None:
        """
        Called when the node stops.
        """
        self.logger.info(f"Stopping node {self.node_id}")
        self._shutdown_event.set()
        if self._generator_task:
            self._generator_task.cancel()
        if self._handler_task:
            self._handler_task.cancel()
        self.logger.info(f"Node {self.node_id} stopped")
    
    async def on_finish(self) -> None:
        """
        Called when the node finishes.
        """
        self.logger.info(f"Node {self.node_id} finished")
        self._shutdown_event.set()
        if self._generator_task:
            self._generator_task.cancel()
        if self._handler_task:
            self._handler_task.cancel()
        self.logger.info(f"Node {self.node_id} finished")
    
    async def on_error(self, error: Exception) -> None:
        """
        Called when the node encounters an error.
        """
        self.logger.error(f"Node {self.node_id} encountered an error: {error}", exc_info=True)
    

    
    
