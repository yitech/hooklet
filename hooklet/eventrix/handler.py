import asyncio
from abc import ABC, abstractmethod
from typing import Any

from hooklet.base import BaseEventrix, BasePilot
from hooklet.types import MessageHandlerCallback


class Handler(BaseEventrix, ABC):
    """
    Base class for event handlers.
    This abstract class provides the structure for handling events.
    """

    def __init__(self, pilot: BasePilot, executor_id: None | str = None):
        super().__init__(pilot, executor_id)
        self._registered_handlers: dict[str, list[str]] = {}
        self._shutdown_event = asyncio.Event()

    @abstractmethod
    def get_handlers(self) -> dict[str, MessageHandlerCallback]:
        """
        Get the mapping of subjects to handler functions.

        This method must be implemented by subclasses to define which
        NATS subjects they want to subscribe to and what handler functions
        should be called when messages are received.

        Returns:
            Dictionary mapping subject strings to handler functions
        """
        raise NotImplementedError("Strategies must implement get_handlers()")

    def is_running(self) -> bool:
        """
        Check if the handler is running.
        This method can be overridden by subclasses to implement custom checks.
        """
        return not self._shutdown_event.is_set()

    @property
    def status(self) -> dict[str, Any]:
        base_status = super().status
        curr_status = {
            "executor_id": self.executor_id,
            "type": self.__class__.__name__,
            "status": "running" if self.is_running() else "stopped",
            "registered_handlers": self._registered_handlers,
        }

        return {**base_status, **curr_status}

    async def on_start(self) -> None:
        self._shutdown_event.clear()
        await self._register_handlers()

    async def on_finish(self) -> None:
        await self._unregister_handlers()

    async def on_stop(self) -> None:
        """
        This method is called when the executor stops.
        It can be overridden by subclasses to implement custom behavior.
        """
        self._shutdown_event.set()

    async def on_execute(self) -> None:
        """
        This method is called when the executor starts executing.
        It can be overridden by subclasses to implement custom behavior.
        """
        await self._shutdown_event.wait()

    async def _register_handlers(self) -> None:
        """
        Register all handlers defined in get_handlers().
        """
        try:
            handlers = self.get_handlers()
            for subject, handler in handlers.items():
                handler_id = f"{self.executor_id}_{subject}"
                try:
                    await self.pilot.register_handler(subject, handler, handler_id)
                    if subject not in self._registered_handlers:
                        self._registered_handlers[subject] = []
                    self._registered_handlers[subject].append(handler_id)
                    self.logger.debug(f"Registered handler for {subject} with ID {handler_id}")
                except Exception as e:
                    self.logger.error(f"Failed to register handler for {subject}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error in handler {self.executor_id}: {str(e)}", exc_info=True)
            raise

    async def _unregister_handlers(self) -> None:
        """
        Unregister all handlers that were registered by this strategy.
        """
        try:
            for subject, handler_ids in self._registered_handlers.items():
                for handler_id in handler_ids:
                    try:
                        # Try to unregister the handler
                        # Note: Some pilot implementations return None, others may return a boolean
                        result = await self.pilot.unregister_handler(handler_id)
                        # If result is a boolean and False, log a warning
                        if result is False:
                            self.logger.warning(
                                f"Failed to unregister handler {handler_id} from {subject}"
                            )
                        else:
                            # Success or None return value (assume success)
                            self.logger.debug(f"Unregistered handler {handler_id} from {subject}")
                    except Exception as e:
                        # Check if this is a connection closed error
                        if "connection closed" in str(e).lower():
                            self.logger.debug(
                                "Connection already closed while unregistering "
                                f"handler {handler_id}."
                                "This is normal during shutdown."
                            )
                        else:
                            self.logger.error(f"Error unregistering handler {handler_id}: {str(e)}")
            self._registered_handlers.clear()
        except Exception as e:
            self.logger.error(f"Error in strategy {self.executor_id}: {str(e)}")
            raise
