import asyncio
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

        self.task: asyncio.Task | None = None
        self.shutdown_event = asyncio.Event()

    @abstractmethod
    async def emit(self) -> AsyncGenerator[Msg, None]:
        """
        Use is_running to stop the emit.
        """
        raise NotImplementedError("Subclasses must implement emit()")

    async def on_start(self):
        if self.task is not None:
            self.task.cancel()
            await self.task
        self.shutdown_event.clear()

        self.task = asyncio.create_task(self.run())

    async def on_close(self):
        self.shutdown_event.set()
        if self.task is not None:
            try:
                await asyncio.wait_for(self.task, timeout=2)
            except asyncio.TimeoutError:
                self.task.cancel()
                await self.task
        self.task = None

    async def run(self):
        try:
            async with aclosing(self.emit()) as gen:  # type: ignore[type-var]
                async for msg in gen:  # type: ignore[attr-defined]
                    if self.shutdown_event.is_set():
                        break
                    subject = self.router(msg)
                    await self.pubsub.publish(subject, msg)
        except Exception as e:
            await self.on_error(e)
