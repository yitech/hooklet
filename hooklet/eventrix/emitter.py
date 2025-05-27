import asyncio
from abc import ABC, abstractmethod
from typing import Any

from hooklet.base import BaseEventrix, BasePilot
from hooklet.types import GeneratorFunc


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
        self.logger.info(f"Starting emitter {self.executor_id}")

    async def on_finish(self) -> None:
        """
        Called when the emitter finishes. Override to add specific cleanup.
        """
        self.logger.info(f"Stopping emitter {self.executor_id}")

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
                        self.logger.info(f"Generator for subject '{subject}' was cancelled.")
                    except Exception as e:
                        self.logger.exception(
                            f"Error in generator for subject '{subject}': {str(e)}"
                        )

                task = asyncio.create_task(run_generator())
                self._generator_tasks.append(task)
                self.logger.debug(f"Launched generator task for subject '{subject}'")

        except Exception as e:
            self.logger.error(f"Failed to start generators in {self.executor_id}: {str(e)}")
            raise

    async def _unregister_generators(self) -> None:
        """
        Cancel all running generator tasks.
        """
        try:
            for task in self._generator_tasks:
                task.cancel()
            await asyncio.gather(*self._generator_tasks, return_exceptions=True)
            self.logger.debug("All generator tasks have been cancelled and awaited.")
        except Exception as e:
            self.logger.error(f"Error while cancelling generator tasks: {str(e)}")

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


class RouterEmitter(Emitter, ABC):
    """
    Base class for event emitters.
    This abstract class provides the structure for emitting events.
    """

    def __init__(
        self, pilot: BasePilot, subject: str, router_key: None | str, executor_id: None | str = None
    ):
        super().__init__(pilot, executor_id)
        self._subject = subject
        self._router_key = router_key
        self._generator_tasks: list[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()

    @abstractmethod
    async def get_generators(self) -> list[GeneratorFunc]:
        """
        This method should be overridden by subclasses to return a dictionary
        of subjects and their corresponding generator functions.
        """
        raise NotImplementedError("Subclasses must implement get_generators()")

    @property
    def status(self) -> dict[str, Any]:
        curr_status = {
            "executor_id": self.executor_id,
            "type": self.__class__.__name__,
            "status": "running" if self.is_running() else "stopped",
            "generator_tasks": [task.get_name() for task in self._generator_tasks],
            "subject": self._subject,
            "router_key": self._router_key,
        }
        base_status = super().status
        return {**base_status, **curr_status}

    async def _register_generators(self) -> None:
        """
        Launch generator coroutines that produce data and publish to NATS.

        Each generator is an async iterable that yields messages to be published
        to its corresponding subject.
        """
        try:
            generators = await self.get_generators()
            self._generator_tasks = []

            for generator in generators:

                async def run_generator(generator=generator):
                    try:
                        async for data in generator():
                            if self._router_key is not None and self._router_key in data:
                                subject = f"{self._subject}.{data[self._router_key]}"
                                router_value = data[self._router_key]
                                del data[self._router_key]
                                self.logger.debug(
                                    f"Routing to subject '{subject}' "
                                    f"with router value: {router_value}"
                                )
                            else:
                                subject = self._subject
                            await self.pilot.publish(subject, data)
                    except asyncio.CancelledError:
                        self.logger.info(f"Generator for subject '{self._subject}' was cancelled.")
                    except Exception as e:
                        self.logger.exception(
                            f"Error in generator for subject '{self._subject}': {str(e)}"
                        )

                task = asyncio.create_task(run_generator())
                self._generator_tasks.append(task)
                self.logger.debug(f"Launched generator task for subject '{self._subject}'")

        except Exception as e:
            self.logger.error(f"Failed to start generators in {self.executor_id}: {str(e)}")
            raise
