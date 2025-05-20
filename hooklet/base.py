import asyncio
import logging
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from hooklet.types import MessageHandlerCallback

logger = logging.getLogger(__name__)


class BasePilot(ABC):
    """
    Base class for eventrix.
    This abstract class provides the structure for managing events.
    """

    @abstractmethod
    async def connect(self) -> None:
        """
        Connect to the event manager.
        This method should be implemented by subclasses to establish a connection.
        """
        raise NotImplementedError("Subclasses must implement connect()")

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if the event manager is connected.
        This method should be implemented by subclasses to check the connection status.
        """
        raise NotImplementedError("Subclasses must implement is_connected()")

    @abstractmethod
    async def close(self) -> None:
        """
        Close the connection to the NATS server.
        """
        raise NotImplementedError("Subclasses must implement close()")

    @abstractmethod
    async def register_handler(
        self, subject: str, handler: MessageHandlerCallback, handler_id: str | None = None
    ) -> str:
        """
        Register a handler for a specific subject.
        This method should be implemented by subclasses to register the handler.
        """
        raise NotImplementedError("Subclasses must implement register_handler()")

    @abstractmethod
    async def unregister_handler(self, handler_id: str) -> bool:
        """
        Unregister a handler by its ID.
        This method should be implemented by subclasses to unregister the handler.
        """
        raise NotImplementedError("Subclasses must implement unregister_handler()")

    @abstractmethod
    async def publish(self, subject: str, data: Any) -> None:
        """
        Publish data to a specific subject.
        This method should be implemented by subclasses to publish the data.
        """
        raise NotImplementedError("Subclasses must implement publish()")


class BaseEventrix(ABC):
    """
    Base class for event executor.
    This abstract class provides the structure for event-driven execution.
    """

    def __init__(self, pilot: BasePilot, executor_id: None | str = None):
        self.pilot = pilot
        self._executor_id = executor_id or str(uuid.uuid4())

        self._task: asyncio.Task | None = None
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

        if not self.pilot.is_connected():
            await self.pilot.connect()

        self._started_at = int(time.time() * 1000)  # milliseconds
        await self.on_start()

        task = asyncio.create_task(self._run_executor())
        if self._task:
            logger.warning(
                f"Executor {self._executor_id} is already running. "
                f"Stopping the previous task before starting a new one."
            )
            self._task.cancel()
        self._task = task

    async def _run_executor(self) -> None:
        """
        Waits until stop() is called.
        """
        try:
            await self.on_execute()
        except Exception as e:
            logger.error(f"Executor {self._executor_id} failed: {str(e)}")
            await self.on_error(e)  # Optional error handling
            raise
        finally:
            await self._finish()  # finish the executor

    async def _finish(self) -> None:
        await self.on_finish()
        self._finished_at = int(time.time() * 1000)
        started_dt = datetime.fromtimestamp(self._started_at / 1000)
        finished_dt = datetime.fromtimestamp(self._finished_at / 1000)
        logging.info(
            f"Executor {self._executor_id} finished. "
            f"Start At: {started_dt.strftime('%d/%m/%Y, %H:%M:%S.%f')}, "
            f"Finish At: {finished_dt.strftime('%d/%m/%Y, %H:%M:%S.%f')}, "
            f"Running elapse = {(finished_dt - started_dt).total_seconds()} seconds"
        )

    async def stop(self) -> None:
        """
        Stop the executor.
        This method sets the stop event and allows the executor to finish its current task.
        """
        logger.info(f"Stopping executor with ID {self._executor_id}")
        self._task.cancel()
        await self.on_stop()

    @property
    def status(self) -> dict[str, Any]:
        """
        Get the status of the executor.
        :return: A dictionary containing the status of the executor.
        """
        return {
            "type": self.__class__.__name__,
            "executor_id": self._executor_id,
            "created_at": self._created_at,
            "started_at": self._started_at,
            "finished_at": self._finished_at,
            "status": "running" if self._started_at and not self._finished_at else "stopped",
        }

    async def publish(self, subject: str, data: Any) -> None:
        """
        Publish data to the configured subject.

        Args:
            subject: The NATS subject to publish to
            data: The data to publish (will be JSON encoded)
        """
        if not self.pilot.is_connected():
            await self.pilot.connect()

        await self.pilot.publish(subject, data)

    @abstractmethod
    async def on_execute(self) -> None:
        """
        Subclass should override this method to implement execution logic.
        """
        raise NotImplementedError("Subclasses must implement on_execute()")

    @abstractmethod
    async def on_stop(self) -> None:
        """stop hook."""
        raise NotImplementedError("Subclasses must implement on_stop()")

    @abstractmethod
    async def on_start(self) -> None:
        """startup hook."""
        raise NotImplementedError("Subclasses must implement on_start()")

    @abstractmethod
    async def on_finish(self) -> None:
        """finish hook."""
        raise NotImplementedError("Subclasses must implement on_finish()")

    async def on_error(self, exception: Exception) -> None:
        """OPTIONAL: Override to handle execution errors."""
