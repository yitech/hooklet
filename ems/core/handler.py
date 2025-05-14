import logging
from abc import ABC, abstractmethod
from typing import Any

from ems.nats_manager import NatsManager

from .executor import EventExecutor

logger = logging.getLogger(__name__)


class EventHandler(EventExecutor, ABC):
    """
    Base class for event handlers.
    This abstract class provides the structure for handling events.
    """

    def __init__(self, nats_manager: NatsManager, executor_id: None | str = None):
        super().__init__(nats_manager, executor_id)
        self._registered_handlers: dict[str, list[str]] = {}

    async def on_start(self) -> None:
        await self._register_handlers()

    async def on_finish(self) -> None:
        await self._unregister_handlers()

    async def on_execute(self) -> None:
        """
        This method is called when the executor starts executing.
        It can be overridden by subclasses to implement custom behavior.
        """
        pass

    async def _register_handlers(self) -> None:
        """
        Register all handlers defined in get_handlers().
        """
        try:
            handlers = self.get_handlers()
            for subject, handler in handlers.items():
                handler_id = f"{self.executor_id}_{subject}"
                try:
                    await self.nats_manager.register_handler(subject, handler, handler_id)
                    if subject not in self._registered_handlers:
                        self._registered_handlers[subject] = []
                    self._registered_handlers[subject].append(handler_id)
                    logger.debug(f"Registered handler for {subject} with ID {handler_id}")
                except Exception as e:
                    logger.error(f"Failed to register handler for {subject}: {str(e)}")
        except Exception as e:
            logger.error(f"Error in handler {self.executor_id}: {str(e)}")
            raise

    async def _unregister_handlers(self) -> None:
        """
        Unregister all handlers that were registered by this strategy.
        """
        try:
            for subject, handler_ids in self._registered_handlers.items():
                for handler_id in handler_ids:
                    try:
                        success = await self.nats_manager.unregister_handler(subject, handler_id)
                        if success:
                            logger.debug(f"Unregistered handler {handler_id} from {subject}")
                        else:
                            logger.warning(
                                f"Failed to unregister handler {handler_id} from {subject}"
                            )
                    except Exception as e:
                        logger.error(f"Error unregistering handler {handler_id}: {str(e)}")
            self._registered_handlers.clear()
        except Exception as e:
            logger.error(f"Error in strategy {self.executor_id}: {str(e)}")
            raise

    @abstractmethod
    def get_handlers(self) -> dict[str, Any]:
        """
        Get the mapping of subjects to handler functions.

        This method must be implemented by subclasses to define which
        NATS subjects they want to subscribe to and what handler functions
        should be called when messages are received.

        Returns:
            Dictionary mapping subject strings to handler functions
        """
        raise NotImplementedError("Strategies must implement get_handlers()")
