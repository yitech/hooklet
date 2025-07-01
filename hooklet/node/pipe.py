from hooklet.base import PubSub
from abc import abstractmethod, ABC
from typing import Callable, AsyncGenerator
from hooklet.base.pilot import Msg
import asyncio
from contextlib import aclosing
from hooklet.base.node import Node

class Pipe(Node, ABC):
    def __init__(self, name: str, subscribes: list[str], pubsub: PubSub, router: Callable[[Msg], str]):
        super().__init__(name)
        self.subscribes = subscribes
        self.pubsub = pubsub
        self.router = router
        self.queue: asyncio.Queue[Msg] = asyncio.Queue()
        

    async def start(self):
        await super().start()
        for subscribe in self.subscribes:
            self.pubsub.subscribe(subscribe, self.queue.put)

    @abstractmethod
    async def pipe(self, msg: Msg) -> AsyncGenerator[Msg, None]:
        raise NotImplementedError("Subclasses must implement pipe()")

    async def run(self):
        while self.is_running:
            try:
                msg = await asyncio.wait_for(self.queue.get(), timeout=2)
                async with aclosing(self.pipe(msg)) as gen:
                    async for msg in gen:
                        if not self.is_running:
                            break
                        subject = self.router(msg)
                        await self.pubsub.publish(subject, msg)
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                await self.on_error(e)
    