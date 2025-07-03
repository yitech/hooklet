from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable

from hooklet.base.node import Node
from hooklet.base.pilot import PushPull, PubSub, Msg
from hooklet.base.types import Job


class Worker(Node, ABC):
    def __init__(self, name: str, pushpull: PushPull, pubsub: PubSub):
        super().__init__(name)
        self.pushpull = pushpull
        self.pubsub = pubsub

    @abstractmethod
    async def process(self, job: Job) -> int:
        """
        Process the job and return the result.
        Return 0 if the job is processed successfully, Other values are reserved for future use.
        """
        raise NotImplementedError("Subclasses must implement process()")
    
    async def start(self):
        await super().start()
        await self.pushpull.register_callback(self.name, self.process)
        async def publish_status(job: Job) -> None:
            msg = Msg(
                _id=job["_id"],
                type=job["type"],
                data={
                    "status": job.get("status", ""),
                    "error": job.get("error", None),
                    "recv_ms": job.get("recv_ms", 0),
                    "start_ms": job.get("start_ms", 0),
                    "end_ms": job.get("end_ms", 0),
                    "retry_count": job.get("retry_count", 0),
                    "data": job.get("data", None),
                }
            )
            await self.pubsub.publish(msg)
        await self.pubsub.subscribe(self.name, publish_status)
    
    async def run(self):
        await self.shutdown_event.wait()

    async def on_finish(self):
        await super().on_finish()
        await self.pushpull.unregister_callback(self.name)


class Dispatcher:
    def __init__(self, pushpull: PushPull, pubsub: PubSub):
        self.pushpull = pushpull
        self.pubsub = pubsub

        self._subscriptions = {}

    async def dispatch(self, subject: str, job: Job) -> bool:
        return await self.pushpull.push(subject, job)
    
    async def subscribe(self, subject: str, callback: Callable[[Job], Awaitable[Any]]) -> int:
        def to_msg_callback(job_callback: Callable[[Job], Awaitable[Any]]) -> Callable[[Msg], Awaitable[Any]]:
            async def msg_callback(msg: Msg) -> Any:
                data = msg["data"]
                job = Job(
                    _id=msg["_id"],
                    type=msg["type"],
                    data=data["data"],
                    error=data.get("error", None),
                    recv_ms=data.get("recv_ms", 0),
                    start_ms=data.get("start_ms", 0),
                    end_ms=data.get("end_ms", 0),
                    status=data.get("status", ""),
                    retry_count=data.get("retry_count", 0),
                )
                return await job_callback(job)
            return msg_callback
        msg_callback = to_msg_callback(callback)
        subscription_id = await self.pubsub.subscribe(subject, msg_callback)
        self._subscriptions[subject] = subscription_id
        return subscription_id
    
    async def unsubscribe(self, subject: str, subscription_id: int) -> bool:
        return await self.pubsub.unsubscribe(subject, subscription_id)
