import asyncio
from abc import ABC, abstractmethod

from hooklet.base import BasePilot, BaseEventrix


class Vertex(BaseEventrix, ABC):
    """Base class for all vertices."""

    def __init__(self, pilot: BasePilot, sources: list[str], destinations: list[str], executor_id: str):
        super().__init__(pilot, executor_id)
        self.sources = sources
        self.destinations = destinations

    @abstractmethod
    async def on_execute(self) -> None:
        """Execute the vertex."""
        pass

    async def on_start(self) -> None:
        """Start the vertex."""
        pass

    async def on_finish(self) -> None:
        """Finish the vertex."""
        pass
        