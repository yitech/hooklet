import datetime
import logging
import os
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any

from hooklet.nats_manager import NatsManager

logger = logging.getLogger(__name__)


class EventExecutor(ABC):
    """
    Base class for event executor.
    This abstract class provides the structure for event-driven execution.
    """

    def __init__(self, nats_manager: NatsManager | None = None, executor_id: None | str = None):
        if nats_manager is None:
            nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
            nats_manager = NatsManager(nats_url=nats_url)
        self.nats_manager = nats_manager
        self._executor_id = executor_id or uuid.uuid4().hex

        self._created_at: int = int(time.time() * 1000)  # milliseconds
        self._started_at: int = 0
        self._finished_at: int = 0

    @property
    def executor_id(self) -> str:
        """
        Get the unique ID of the executor.
        :return: Unique ID of the executor.
        """
        return self._executor_id

    async def start(self) -> None:
        logger.info(f"Starting executor with ID {self._executor_id}")

        if not self.nats_manager.is_connected():
            await self.nats_manager.connect()

        self._started_at = int(time.time() * 1000)  # milliseconds
        await self.on_start()

        try:
            await self._run_executor()

        except Exception as e:
            logger.error(f"Executor {self._executor_id} failed: {str(e)}")
            await self.on_error(e)  # Optional error handling
            raise
        finally:
            await self._finish()  # finish the executor

    async def _run_executor(self) -> None:
        """
        Waits until stop() is called.
        """
        await self.on_execute()

    async def _finish(self) -> None:
        await self.on_finish()
        self._finished_at = int(time.time() * 1000)
        started_dt = datetime.datetime.fromtimestamp(self._started_at / 1000)
        finished_dt = datetime.datetime.fromtimestamp(self._finished_at / 1000)
        logging.info(
            f"Executor {self._executor_id} finished. "
            f"{started_dt=}, "
            f"{finished_dt=}"
            f"Running elapse = {finished_dt - started_dt}"
        )

    async def stop(self) -> None:
        """
        Stop the executor.
        This method sets the stop event and allows the executor to finish its current task.
        """
        logger.info(f"Stopping executor with ID {self._executor_id}")
        await self.on_stop()

    async def publish(self, subject: str, data: Any) -> None:
        """
        Publish data to the configured subject.

        Args:
            subject: The NATS subject to publish to
            data: The data to publish (will be JSON encoded)
        """
        if not self.nats_manager.is_connected():
            await self.nats_manager.connect()

        await self.nats_manager.publish(subject, data)

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
