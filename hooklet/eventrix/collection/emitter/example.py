"""
Example emitter implementation.
This emitter generates events with unique IDs at regular intervals.
"""

# pylint: disable=R0801
import asyncio
import uuid
from datetime import datetime

from hooklet.base import BasePilot
from hooklet.eventrix.v1.emitter import Emitter, RouterEmitter


class ExampleEmitter(Emitter):
    """
    Example implementation of an Emitter.
    Generates events with UUID identifiers once per second.
    """

    async def get_generators(self):
        """
        Defines and returns event generators.

        Returns:
            Dictionary mapping subjects to generator functions
        """

        async def example_generator():
            """Generator that yields events with UUID and timestamp."""
            counter = 0
            while True:
                # Add timestamp and counter to make the code more unique
                event = {
                    "id": str(uuid.uuid4()),
                    "timestamp": datetime.now().isoformat(),
                    "counter": counter,
                }

                await asyncio.sleep(1)  # Emit every second
                self.logger.info(f"Emitting event: {event['id']} (#{counter})")
                counter += 1
                yield event

        # Return a mapping of subjects to generator functions
        return {"example": example_generator}


class ExampleRouterEmitter(RouterEmitter):
    """
    Example implementation of a RouterEmitter.
    Generates events with UUID identifiers once per second.
    """

    def __init__(
        self,
        pilot: BasePilot,
        subject: str,
        router_key: None | str = None,
        executor_id: None | str = None,
    ):
        super().__init__(pilot, subject, router_key, executor_id)

    async def get_generators(self):
        """
        Defines and returns event generators.

        Returns:
            Dictionary mapping subjects to generator functions
        """

        async def example_generator():
            """Generator that yields events with UUID and timestamp."""
            counter = 0
            while True:
                # Add timestamp and counter to make the code more unique
                event = {
                    "id": str(uuid.uuid4()),
                    "timestamp": datetime.now().isoformat(),
                    "counter": counter,
                }

                await asyncio.sleep(1)  # Emit every second
                self.logger.info(f"Emitting event: {event['id']} (#{counter})")
                counter += 1
                yield event

        # Return a mapping of subjects to generator functions
        return [example_generator]
