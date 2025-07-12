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

    async def publish(self, subject: str, msg: Msg) -> None:
        subscriptions = self._subscriptions[subject]
        tasks = [callback(msg) for callback in subscriptions]
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
        self._callbacks: Dict[str, Callable[[Req], Awaitable[Reply]]] = {}

    async def request(self, subject: str, req: Req, timeout: float = 10.0) -> Reply:
        if subject not in self._callbacks:
            raise ValueError(f"No callback registered for {subject}")
        try:
            result = await asyncio.wait_for(self._callbacks[subject](req), timeout=timeout)
            return result
        except asyncio.TimeoutError:
            start_ms = int(time.time() * 1000)
            end_ms = int(time.time() * 1000)
            return Reply(
                type="error", result=None, error="Timeout", start_ms=start_ms, end_ms=end_ms
            )
        except Exception as e:
            start_ms = int(time.time() * 1000)
            end_ms = int(time.time() * 1000)
            return Reply(type="error", result=None, error=str(e), start_ms=start_ms, end_ms=end_ms)

    async def register_callback(
        self, subject: str, callback: Callable[[Req], Awaitable[Reply]]
    ) -> None:
        self._callbacks[subject] = callback

    async def unregister_callback(self, subject: str) -> bool:
        if subject in self._callbacks:
            del self._callbacks[subject]
            return True
        return False


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

    async def unregister_worker(self, subject: str) -> None:
        if subject not in self._pushpulls:
            return
        await self._pushpulls[subject].unregister_worker()

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

    async def unregister_worker(self) -> None:
        self._shutdown_event.set()
        await asyncio.gather(
            *[asyncio.wait_for(task, timeout=2.0) for task in self._worker_loops],
            return_exceptions=True,
        )
        self._worker_loops.clear()

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
            try:
                # Wait for either a job or shutdown signal
                job_task = asyncio.create_task(self._job_queue.get())
                shutdown_task = asyncio.create_task(self._shutdown_event.wait())

                done, pending = await asyncio.wait(
                    [job_task, shutdown_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in pending:
                    task.cancel()

                if shutdown_task in done:
                    break

                if job_task in done:
                    job: Job = job_task.result()
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
                        await self._notify_subscriptions(job)
                    except Exception as e:
                        logger.error(f"Error in worker loop for {self.subject}: {e}")
                        job.end_ms = int(time.time() * 1000)
                        job.status = "failed"
                        job.error = str(e)
                        await self._notify_subscriptions(job)

            except Exception as e:
                logger.error(f"Error in worker loop for {self.subject}: {e}")

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

    def pubsub(self) -> InprocPubSub:
        return self._pubsub

    def reqreply(self) -> InprocReqReply:
        return self._reqreply

    def pushpull(self) -> InprocPushPull:
        return self._pushpull

    def get_job_queue(self, subject: str) -> asyncio.Queue[Job]:
        return self._job_queues[subject]
