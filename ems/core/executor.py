from abc import ABC, abstractmethod
import logging
import asyncio
from typing import Any
import uuid
import time
from ems.nats_manager import NatsManager

logger = logging.getLogger(__name__)

class EventExecutor(ABC):
    """
    Base class for event executor.
    This abstract class provides the structure for event-driven execution.
    """

    def __init__(self, nats_manager: NatsManager, executor_id: None | str = None):
        self.nats_manager = nats_manager
        self._executor_id = executor_id or uuid.uuid4().hex
        self._stop_event = asyncio.Event()
        self._created_at: int = int(time.time() * 1000)  # milliseconds

    @property
    def executor_id(self) -> str:
        """
        Get the unique ID of the executor.
        :return: Unique ID of the executor.
        """
        return self._executor_id

    def is_running(self) -> bool:
        """
        Check if the executor is currently running.
        :return: True if running, False otherwise.
        """
        return self._stop_event.is_set()

    async def start(self) -> None:
        logger.info(f"Starting executor with ID {self._executor_id}")

        if not self._nats_manager.is_connected():
            await self._nats_manager.connect()

        self._stop_event.clear()  # clear the stop event to start the executor
        await self.on_start()
        
        try:
            await self._run_executor()
        except Exception as e:
            logger.error(f"Executor {self._executor_id} failed: {str(e)}")
            await self.on_error(e)  # Optional error handling
            raise
        finally:
            await self.on_finish()

    async def _run_executor(self) -> None:
        """
        Waits until stop() is called.
        """
        await self.on_execute()
        await self._stop_event.wait()  # wait until externally told to stop

    async def _finish(self) -> None:
        self._stop_event.set()  # set the stop event to finish the executor
        await self.on_finish()

    async def stop(self) -> None:
        """
        Stop the executor.
        This method sets the stop event and allows the executor to finish its current task.
        """
        logger.info(f"Stopping executor with ID {self._executor_id}")
        self._stop_event.set()
    
    async def publish(self, subject: str, data: Any) -> None:
        """
        Publish data to the configured subject.
        
        Args:
            subject: The NATS subject to publish to
            data: The data to publish (will be JSON encoded)
        """
        if not self._nats_manager.is_connected():
            await self._nats_manager.connect()
        
        await self._nats_manager.publish(subject, data)

    @abstractmethod
    async def on_execute(self) -> None:
        """
        Subclass should override this method to implement execution logic.
        """
        raise NotImplementedError("Subclasses must implement on_execute()")
    
    @abstractmethod
    async def on_start(self) -> None:
        """Optional startup hook."""
        raise NotImplementedError("Subclasses must implement on_start()")
    
    @abstractmethod
    async def on_finish(self) -> None:
        """finish hook."""
        raise NotImplementedError("Subclasses must implement on_finish()")
    
    async def on_error(self, exception: Exception) -> None:
        """OPTIONAL: Override to handle execution errors."""
        pass