"""
Example sinker node implementation.
This sink node sinks events to stdout.
"""

# pylint: disable=R0801
import uuid
from typing import AsyncIterator, Callable

from hooklet.base import BasePilot
from hooklet.eventrix.v2.node import Node
from hooklet.types import HookletMessage


class ExampleSinker(Node):
    """
    Example implementation of a sink node.
    Sinks events to stdout.
    """

    def __init__(
        self,
        pilot: BasePilot,
        sources: list[str],
        router: Callable[[HookletMessage], str | None],
        node_id: None | str = None,
    ):
        super().__init__(pilot, sources, router, node_id)

    async def handler_func(self, message: HookletMessage) -> AsyncIterator[HookletMessage]:
        """
        Handler function that sinks events to stdout.
        """
        self.logger.info(f"Received message: {message}")
        if "name" in message.payload:
            name = message.payload["name"]
            response = HookletMessage(
                correlation_id=message.correlation_id,
                type="response",
                payload={"greeting": f"Hello, {name}!"},
            )
            self.logger.info(f"Sending response: {response}")
            yield response
        else:
            self.logger.warning(f"Received message without 'name' in payload: {message.payload}")
