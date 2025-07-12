from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable

from hooklet.base.node import Node
from hooklet.base.pilot import PushPull
from hooklet.base.types import Job


class Worker(Node, ABC):
    def __init__(self, name: str, pushpull: PushPull):
        super().__init__(name)
        self.pushpull = pushpull

    @abstractmethod
    async def on_job(self, job: Job) -> int:
        """
        Process the job and return the result.
        Return 0 if the job is processed successfully, Other values are reserved for future use.
        """
        raise NotImplementedError("Subclasses must implement process()")

    async def on_start(self):
        await self.pushpull.register_worker(self.name, self.on_job)

    async def on_close(self):
        await self.pushpull.unregister_worker(self.name)


class Dispatcher:
    def __init__(self, pushpull: PushPull):
        self.pushpull = pushpull
        self._subscriptions: dict[str, int] = {}

    async def dispatch(self, subject: str, job: Job) -> bool:
        return await self.pushpull.push(subject, job)

    async def subscribe(self, subject: str, callback: Callable[[Job], Awaitable[Any]]) -> None:
        subscription_id = await self.pushpull.subscribe(subject, callback)
        self._subscriptions[subject] = subscription_id

    async def unsubscribe(self, subject: str) -> bool:
        subscription_id = self._subscriptions.pop(subject, None)
        if subscription_id is None:
            return False
        return await self.pushpull.unsubscribe(subject, subscription_id)
