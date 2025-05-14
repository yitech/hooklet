from abc import ABC, abstractmethod
import logging
import asyncio
from typing import Any
import uuid
import time

logger = logging.getLogger(__name__)

class EventExecutor(ABC):
    """
    Base class for event executor.
    This abstract class provides the structure for event-driven execution.
    """

    def __init__(self, nats_manager: Any, executor_id: None | str = None):
        self._nats_manager = nats_manager
        self._executor_id = executor_id or uuid.uuid4().hex
        self._stop_event = asyncio.Event()
        self._created_at: int = time.time()

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

        await self.on_start()

        try:
            await self._run_executor()  # template method
        finally:
            await self._finish()

    async def _run_executor(self) -> None:
        """
        Waits until stop() is called.
        """
        await self.on_execute()
        await self._stop_event.wait()  # wait until externally told to stop

    async def _finish(self) -> None:
        await self.on_finish()

    async def stop(self) -> None:
        """
        Stop the executor.
        This method sets the stop event and allows the executor to finish its current task.
        """
        logger.info(f"Stopping executor with ID {self._executor_id}")
        self._stop_event.set()

    @abstractmethod
    async def on_execute(self) -> None:
        """
        Subclass should override this method to implement execution logic.
        """
        pass
    
    @abstractmethod
    async def on_start(self) -> None:
        """Optional startup hook."""
        pass
    
    @abstractmethod
    async def on_finish(self) -> None:
        """Optional finish hook."""
        pass