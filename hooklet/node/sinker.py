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
        self.subscription_ids: dict[str, int] = {}

    async def on_start(self):
        for subscribe in self.subscribes:
            subscription_id = await self.pubsub.subscribe(subscribe, self.on_message)
            self.subscription_ids[subscribe] = subscription_id

    @abstractmethod
    async def on_message(self, msg: Msg) -> None:
        raise NotImplementedError("Subclasses must implement on_message()")

    async def on_close(self):
        for subscribe in self.subscribes:
            if subscribe in self.subscription_ids:
                await self.pubsub.unsubscribe(subscribe, self.subscription_ids[subscribe])
        self.subscription_ids.clear()
