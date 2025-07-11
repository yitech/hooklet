from abc import ABC, abstractmethod
from contextlib import aclosing
from typing import AsyncGenerator, Callable

from hooklet.base import PubSub
from hooklet.base.node import Node
from hooklet.base.pilot import Msg


class Pipe(Node, ABC):
    def __init__(
        self, name: str, subscribes: list[str], pubsub: PubSub, router: Callable[[Msg], str]
    ):
        super().__init__(name)
        self.subscribes = subscribes
        self.pubsub = pubsub
        self.router = router
        self.subscriber_id: int | None = None

    async def on_start(self):
        async def on_pipe_message(in_msg: Msg):
            async with aclosing(self.on_message(in_msg)) as gen:  # type: ignore[type-var]
                try:
                    async for out_msg in gen:  # type: ignore[attr-defined]
                        subject = self.router(out_msg)
                        await self.pubsub.publish(subject, out_msg)
                except Exception as e:
                    await self.on_error(e)

        self.subscriber_id = id(on_pipe_message)
        for subscribe in self.subscribes:
            await self.pubsub.subscribe(subscribe, on_pipe_message)

    @abstractmethod
    async def on_message(self, msg: Msg) -> AsyncGenerator[Msg, None]:
        raise NotImplementedError("Subclasses must implement on_message()")

    async def on_close(self):
        if self.subscriber_id is None:
            return
        for subscribe in self.subscribes:
            await self.pubsub.unsubscribe(subscribe, self.subscriber_id)
