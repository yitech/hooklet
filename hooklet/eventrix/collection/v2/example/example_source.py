"""
Example source node implementation.
This source node generates events with unique IDs at regular intervals.
"""

# pylint: disable=R0801
import asyncio
from typing import AsyncIterator, Callable

from hooklet.base import BasePilot
from hooklet.eventrix.v2.node import Node
from hooklet.types import HookletMessage


class ExampleSource(Node):
    """
    Example implementation of a source node.
    Generates events with UUID identifiers once per second.
    """

    def __init__(
        self,
        pilot: BasePilot,
        router: Callable[[HookletMessage], str | None],
        node_id: None | str = None,
    ):
        super().__init__(pilot, [], router, node_id)

    async def generator_func(self) -> AsyncIterator[HookletMessage]:
        """
        Generator function that yields events with UUID identifiers once per second.
        """
        while self.is_running:
            message = HookletMessage(
                type="example",
                payload={"name": "John Doe"},
            )
            self.logger.info(f"Generated message {message}")
            yield message
            await asyncio.sleep(1)
