from hooklet.base import PubSub
from abc import abstractmethod, ABC
from typing import Callable, AsyncGenerator
from hooklet.base.pilot import Msg
import asyncio
from hooklet.base.node import Node

class Pipe(Node, ABC):
    def __init__(self, name: str, pubsub: PubSub, router: Callable[[Msg], str]):
        self.name = name
        self.pubsub = pubsub
        self.router = router
        self.queue: asyncio.Queue[Msg] = asyncio.Queue()
        

    async def start(self):
        await super().start()
        self.pubsub.subscribe(self.name, self.queue.put)

    @abstractmethod
    async def pipe(self, msg: Msg) -> AsyncGenerator[Msg, None]:
        raise NotImplementedError("Subclasses must implement pipe()")


    async def run(self):
        while self.is_running:
            try:
                msg = await asyncio.wait_for(self.queue.get(), timeout=2)
                async for msg in self.pipe(msg):
                    subject = self.router(msg)
                    await self.pubsub.publish(subject, msg)
            except asyncio.TimeoutError:
                pass

    