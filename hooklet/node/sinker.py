import asyncio
from abc import ABC, abstractmethod

from hooklet.base.node import Node
from hooklet.base.pilot import Msg, PubSub


class Sinker(Node, ABC):
    def __init__(self, name: str, subscribes: list[str], pubsub: PubSub):
        """
        Sinker node that sinks messages to the router.
        """
        super().__init__(name)
        self.subscribes = subscribes
        self.pubsub = pubsub
        self.queue: asyncio.Queue[Msg] = asyncio.Queue()

    async def start(self):
        await super().start()
        for subscribe in self.subscribes:
            self.pubsub.subscribe(subscribe, self.queue.put)

    @abstractmethod
    async def sink(self, msg: Msg) -> None:
        raise NotImplementedError("Subclasses must implement sink()")

    async def run(self):
        while self.is_running:
            try:
                msg = await self.queue.get()
                await self.sink(msg)
            except Exception as e:
                await self.on_error(e)

    async def close(self):
        await super().close()
        for subscribe in self.subscribes:
            self.pubsub.unsubscribe(subscribe, hash(self.queue.put))
