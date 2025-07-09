import asyncio
import time
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict

from hooklet.base import Job, Msg, Pilot, PubSub, PushPull, Reply, Req, ReqReply
from hooklet.logger import get_logger

logger = get_logger(__name__)


class InprocPubSub(PubSub):
    def __init__(self) -> None:
        self._subscriptions: Dict[str, list[Callable[[Msg], Awaitable[Any]]]] = defaultdict(list)

    async def publish(self, subject: str, data: Msg) -> None:
        subscriptions = self._subscriptions[subject]
        tasks = [callback(data) for callback in subscriptions]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                subscription_id = id(subscriptions[i])
                logger.error(f"Subscription {subscription_id} failed for {subject}: {result}")

    async def subscribe(self, subject: str, callback: Callable[[Msg], Awaitable[Any]]) -> int:
        self._subscriptions[subject].append(callback)
        subscription_id = id(callback)
        logger.info(f"Subscribed to {subject} with ID {subscription_id}")
        return subscription_id

    async def unsubscribe(self, subject: str, subscription_id: int) -> bool:
        if subject in self._subscriptions:
            try:
                callback_to_remove = next(
                    callback
                    for callback in self._subscriptions[subject]
                    if id(callback) == subscription_id
                )
                self._subscriptions[subject].remove(callback_to_remove)
                logger.info(f"Unsubscribed from {subject} with ID {subscription_id}")
                return True
            except StopIteration:
                pass
        return False

    def get_subscriptions(self, subject: str) -> list[Callable[[Msg], Awaitable[Any]]]:
        return self._subscriptions[subject]


class InprocReqReply(ReqReply):
    def __init__(self) -> None:
        self._callbacks: Dict[str, Callable[[Any], Awaitable[Any]]] = {}

    async def request(self, subject: str, data: Req, timeout: float = 10.0) -> Reply:
        if subject not in self._callbacks:
            raise ValueError(f"No callback registered for {subject}")
        try:
            return await asyncio.wait_for(self._callbacks[subject](data), timeout=timeout)
        except asyncio.TimeoutError:
            return Reply(error="Timeout")
        except Exception as e:
            return Reply(error=str(e))

    async def register_callback(
        self, subject: str, callback: Callable[[Any], Awaitable[Any]]
    ) -> str:
        self._callbacks[subject] = callback
        return subject

    async def unregister_callback(self, subject: str) -> None:
        if subject in self._callbacks:
            del self._callbacks[subject]


class InprocPushPull(PushPull):
    def __init__(self, pilot: "InprocPilot") -> None:
        self._pilot = pilot
        self._pushpulls: Dict[str, SimplePushPull] = {}

    async def push(self, subject: str, job: Job) -> bool:
        if subject not in self._pushpulls:
            self._pushpulls[subject] = SimplePushPull(subject, self._pilot)
        return await self._pushpulls[subject].push(job)

    async def register_worker(
        self, subject: str, callback: Callable[[Job], Awaitable[Any]], n_workers: int = 1
    ) -> None:
        if subject not in self._pushpulls:
            self._pushpulls[subject] = SimplePushPull(subject, self._pilot)
        await self._pushpulls[subject].register_worker(callback, n_workers)

    async def subscribe(self, subject: str, callback: Callable[[Job], Awaitable[Any]]) -> int:
        if subject not in self._pushpulls:
            self._pushpulls[subject] = SimplePushPull(subject, self._pilot)
        return await self._pushpulls[subject].subscribe(callback)

    async def unsubscribe(self, subject: str, subscription_id: int) -> bool:
        if subject not in self._pushpulls:
            return False
        return await self._pushpulls[subject].unsubscribe(subscription_id)

    async def _cleanup(self) -> None:
        await asyncio.gather(*[pushpull._cleanup() for pushpull in self._pushpulls.values()])
        self._pushpulls.clear()


class SimplePushPull:
    def __init__(self, subject: str, pilot: "InprocPilot") -> None:
        self.subject = subject
        self._job_queue = pilot.get_job_queue(subject)
        self._worker_loops: list[asyncio.Task[Any]] = []
        self._subscription_lock = asyncio.Lock()
        self._subscriptions: list[Callable[[Job], Awaitable[Any]]] = []
        self._shutdown_event = asyncio.Event()
        self._shutdown_event.set()

    async def push(self, job: Job) -> bool:
        if self._job_queue.full():
            return False
        job.recv_ms = int(time.time() * 1000)
        job.status = "new"
        await self._notify_subscriptions(job)
        self._job_queue.put_nowait(job)
        return True

    async def register_worker(
        self, callback: Callable[[Job], Awaitable[Any]], n_workers: int = 1
    ) -> None:
        self._shutdown_event.clear()
        for _ in range(n_workers):
            self._worker_loops.append(asyncio.create_task(self._worker_loop(callback)))

    async def subscribe(self, callback: Callable[[Job], Awaitable[Any]]) -> int:
        logger.info(f"Subscribed {id(callback)} to {self.subject}")
        async with self._subscription_lock:
            self._subscriptions.append(callback)
        return id(callback)

    async def unsubscribe(self, subscription_id: int) -> bool:
        async with self._subscription_lock:
            try:
                callback_to_remove = next(
                    callback for callback in self._subscriptions if id(callback) == subscription_id
                )
                self._subscriptions.remove(callback_to_remove)
                logger.info(f"Unsubscribed {subscription_id} from {self.subject}")
                return True
            except StopIteration:
                return False

    async def _worker_loop(self, callback: Callable[[Job], Awaitable[Any]]) -> None:
        while not self._shutdown_event.is_set():
            done, pending = await asyncio.wait(
                [
                    asyncio.create_task(self._job_queue.get()),
                    asyncio.create_task(self._shutdown_event.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if self._shutdown_event.is_set():
                break
            # Get the first (and only) completed task from the done set
            completed_task = next(iter(done))
            job = completed_task.result()
            job.start_ms = int(time.time() * 1000)
            job.status = "running"
            await self._notify_subscriptions(job)
            try:
                await callback(job)
                job.end_ms = int(time.time() * 1000)
                job.status = "finished"
                await self._notify_subscriptions(job)
            except asyncio.CancelledError:
                job.end_ms = int(time.time() * 1000)
                job.status = "cancelled"
            except Exception as e:
                logger.error(f"Error in worker loop for {self.subject}: {e}")
                job.end_ms = int(time.time() * 1000)
                job.status = "failed"
                job.error = str(e)
                await self._notify_subscriptions(job)

        logger.info(f"Worker loop for {self.subject} shutdown")

    async def _notify_subscriptions(self, job: Job) -> None:
        try:
            async with self._subscription_lock:
                subscriptions = self._subscriptions.copy()
            tasks = [subscription(job) for subscription in subscriptions]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    subscription_id = id(subscriptions[i])
                    logger.error(
                        f"Subscription {subscription_id} failed for {self.subject}: {result}"
                    )
        except Exception as e:
            logger.error(f"Error in notify_subscriptions for {self.subject}: {e}")

    async def _cleanup(self) -> None:
        self._shutdown_event.set()
        await asyncio.gather(
            *[asyncio.wait_for(task, timeout=2.0) for task in self._worker_loops],
            return_exceptions=True,
        )
        self._worker_loops.clear()
        self._subscriptions.clear()


class InprocPilot(Pilot):
    def __init__(self) -> None:
        super().__init__()
        self._connected = False
        self._pubsub = InprocPubSub()
        self._reqreply = InprocReqReply()
        self._pushpull = InprocPushPull(self)
        self._job_queues: Dict[str, asyncio.Queue[Job]] = defaultdict(
            lambda: asyncio.Queue(maxsize=1000)
        )

    def is_connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        self._connected = True
        logger.info("InProcPilot connected")

    async def disconnect(self) -> None:
        self._connected = False
        logger.info("InProcPilot disconnecting...")
        await self._pushpull._cleanup()
        # Clean up job queues to prevent memory leaks
        self._job_queues.clear()
        logger.info("InProcPilot disconnected")

    def pubsub(self) -> PubSub:
        return self._pubsub

    def reqreply(self) -> ReqReply:
        return self._reqreply

    def pushpull(self) -> PushPull:
        return self._pushpull

    def get_job_queue(self, subject: str) -> asyncio.Queue[Job]:
        return self._job_queues[subject]
