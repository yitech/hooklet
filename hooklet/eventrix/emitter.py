import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any

from hooklet.base import BaseEventrix, BasePilot
from hooklet.types import GeneratorFunc

logger = logging.getLogger(__name__)


class Emitter(BaseEventrix, ABC):
    """
    Base class for event emitters.
    This abstract class provides the structure for emitting events.
    """

    def __init__(self, pilot: BasePilot, executor_id: None | str = None):
        super().__init__(pilot, executor_id)
        self._generator_tasks: list[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()

    @abstractmethod
    async def get_generators(self) -> dict[str, GeneratorFunc]:
        """
        This method should be overridden by subclasses to return a dictionary
        of subjects and their corresponding generator functions.
        """
        raise NotImplementedError("Subclasses must implement get_generators()")

    def is_running(self) -> bool:
        """
        Check if the emitter is running.
        This method can be overridden by subclasses to implement custom checks.
        """
        return not self._shutdown_event.is_set()

    @property
    def status(self) -> dict[str, Any]:
        curr_status = {
            "executor_id": self.executor_id,
            "type": self.__class__.__name__,
            "status": "running" if self.is_running() else "stopped",
            "generator_tasks": [task.get_name() for task in self._generator_tasks],
        }
        base_status = super().status
        return {**base_status, **curr_status}

    async def on_start(self) -> None:
        """
        Called when the emitter starts. Override to add specific initialization.
        """
        self._shutdown_event.clear()
        logger.info(f"Starting emitter {self.executor_id}")

    async def on_finish(self) -> None:
        """
        Called when the emitter finishes. Override to add specific cleanup.
        """
        logger.info(f"Stopping emitter {self.executor_id}")

    async def _register_generators(self) -> None:
        """
        Launch generator coroutines that produce data and publish to NATS.

        Each generator is an async iterable that yields messages to be published
        to its corresponding subject.
        """
        try:
            generators = await self.get_generators()
            self._generator_tasks = []

            for subject, generator in generators.items():

                async def run_generator(subject=subject, generator=generator):
                    try:
                        async for data in generator():
                            await self.pilot.publish(subject, data)
                    except asyncio.CancelledError:
                        logger.info(f"Generator for subject '{subject}' was cancelled.")
                    except Exception as e:
                        logger.exception(f"Error in generator for subject '{subject}': {str(e)}")

                task = asyncio.create_task(run_generator())
                self._generator_tasks.append(task)
                logger.debug(f"Launched generator task for subject '{subject}'")

        except Exception as e:
            logger.error(f"Failed to start generators in {self.executor_id}: {str(e)}")
            raise

    async def _unregister_generators(self) -> None:
        """
        Cancel all running generator tasks.
        """
        try:
            for task in self._generator_tasks:
                task.cancel()
            await asyncio.gather(*self._generator_tasks, return_exceptions=True)
            logger.debug("All generator tasks have been cancelled and awaited.")
        except Exception as e:
            logger.error(f"Error while cancelling generator tasks: {str(e)}")

    async def on_execute(self) -> None:
        """
        Main execution loop. Override to implement specific behavior.
        """
        await self._register_generators()
        await self._shutdown_event.wait()

    async def on_stop(self) -> None:
        """
        This method is called when the executor stops.
        It can be overridden by subclasses to implement custom behavior.
        """
        self._shutdown_event.set()
