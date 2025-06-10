"""
Example emitter implementation.
This emitter generates events with unique IDs at regular intervals.
"""

# pylint: disable=R0801
import asyncio
import uuid

from hooklet.eventrix.v1.emitter import RouterEmitter


class ExampleRouterEmitter(RouterEmitter):
    """
    Example implementation of a RouterEmitter.
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
                    self._router_key: "even" if counter % 2 == 0 else "odd",
                    "counter": counter,
                }

                await asyncio.sleep(1)  # Emit every second
                self.logger.info(f"Emitting event: {event['id']} (#{counter})")
                counter += 1
                yield event

        # Return a mapping of subjects to generator functions
        return [example_generator]
