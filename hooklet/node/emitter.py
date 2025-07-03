from abc import ABC, abstractmethod
from contextlib import aclosing
from typing import AsyncGenerator, Callable

from hooklet.base.node import Node
from hooklet.base.pilot import Msg, PubSub


class Emitter(Node, ABC):
    def __init__(self, name: str, pubsub: PubSub, router: Callable[[Msg], str]):
        """
        Emitter node that emits messages to the router.
        """
        super().__init__(name)
        self.pubsub = pubsub
        self.router = router

    @abstractmethod
    async def emit(self) -> AsyncGenerator[Msg, None]:
        """
        Use is_running to stop the emit.
        """
        raise NotImplementedError("Subclasses must implement emit()")

    async def run(self):
        try:
            async with aclosing(self.emit()) as gen:
                async for msg in gen:
                    if not self.is_running:
                        break
                    subject = self.router(msg)
                    await self.pubsub.publish(subject, msg)
        except Exception as e:
            await self.on_error(e)
