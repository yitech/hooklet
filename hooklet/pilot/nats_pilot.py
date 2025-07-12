import asyncio
import json
import time
from functools import lru_cache, wraps
from typing import Any, Awaitable, Callable, Dict

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg as NatsMsg
from nats.aio.subscription import Subscription
from nats.js import JetStreamContext, api

from hooklet.base import Job, Msg, Pilot, PubSub, PushPull, Reply, Req, ReqReply
from hooklet.logger import get_logger

logger = get_logger(__name__)


def async_once(func):
    """Decorator that ensures an async function is called only once per set of arguments."""
    called = set()
    locks = {}

    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Create a cache key from args and kwargs
        key = (args, tuple(sorted(kwargs.items())))

        if key in called:
            return

        # Use a lock to prevent concurrent calls with the same arguments
        if key not in locks:
            locks[key] = asyncio.Lock()

        async with locks[key]:
            if key in called:
                return
            result = await func(*args, **kwargs)
            called.add(key)
            return result

    return wrapper


class NatsPubSub(PubSub):
    def __init__(self, pilot: "NatsPilot") -> None:
        self._pilot = pilot
        self._subscriptions: Dict[str, list[Callable[[Msg], Awaitable[Any]]]] = {}
        self._nats_subscriptions: Dict[str, list[Subscription]] = {}

    async def publish(self, subject: str, msg: Msg) -> None:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")
        payload = json.dumps(msg.model_dump(by_alias=True)).encode()
        await self._pilot._nats_client.publish(subject, payload)
        logger.debug(f"Published to {subject}: {msg}")

    async def subscribe(self, subject: str, callback: Callable[[Msg], Awaitable[Any]]) -> int:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")
        subscription_id = id(callback)

        async def nats_callback(msg: NatsMsg):
            try:
                data = Msg.model_validate_json(msg.data.decode())
                await callback(data)
            except Exception as e:
                logger.error(
                    f"Error in NATS subscription callback for {msg.subject}: {e}", exc_info=True
                )

        sub = await self._pilot._nats_client.subscribe(subject, cb=nats_callback)
        if subject not in self._subscriptions:
            self._subscriptions[subject] = []
            self._nats_subscriptions[subject] = []
        self._subscriptions[subject].append(callback)
        self._nats_subscriptions[subject].append(sub)
        logger.info(f"Subscribed to {subject} with ID {subscription_id}")
        return subscription_id

    async def unsubscribe(self, subject: str, subscription_id: int) -> bool:
        if subject not in self._subscriptions:
            return False
        try:
            callback_to_remove = next(
                callback
                for callback in self._subscriptions[subject]
                if id(callback) == subscription_id
            )
            index = self._subscriptions[subject].index(callback_to_remove)
            self._subscriptions[subject].pop(index)
            nats_sub = self._nats_subscriptions[subject].pop(index)
            await nats_sub.unsubscribe()
            if not self._subscriptions[subject]:
                del self._subscriptions[subject]
                del self._nats_subscriptions[subject]
            logger.info(f"Unsubscribed from {subject} with ID {subscription_id}")
            return True
        except (StopIteration, ValueError):
            return False

    async def _cleanup(self) -> None:
        for subject in list(self._subscriptions.keys()):
            for nats_sub in self._nats_subscriptions[subject]:
                await nats_sub.unsubscribe()
            self._subscriptions[subject].clear()
            self._nats_subscriptions[subject].clear()
        self._subscriptions.clear()
        self._nats_subscriptions.clear()


class NatsReqReply(ReqReply):
    def __init__(self, pilot: "NatsPilot") -> None:
        self._pilot = pilot
        self._callbacks: Dict[str, Callable[[Any], Awaitable[Any]]] = {}
        self._nats_subscriptions: Dict[str, Subscription] = {}

    async def request(self, subject: str, req: Req, timeout: float = 10.0) -> Reply:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")
        payload = json.dumps(req.model_dump(by_alias=True)).encode()
        try:
            response = await self._pilot._nats_client.request(subject, payload, timeout=timeout)
            return Reply.model_validate_json(response.data.decode())
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"Request to {subject} timed out after {timeout} seconds") from exc
        except Exception as e:
            raise e

    async def register_callback(
        self, subject: str, callback: Callable[[Req], Awaitable[Reply]]
    ) -> None:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")

        async def nats_callback(msg: NatsMsg):
            try:
                data = Req.model_validate_json(msg.data.decode())
                response = await callback(data)
                response_payload = response.model_dump_json(by_alias=True).encode()

                await msg.respond(response_payload)
            except Exception as e:
                logger.error(f"Error in NATS request callback for {subject}: {e}", exc_info=True)
                error_response = {"error": str(e)}
                await msg.respond(json.dumps(error_response).encode())

        sub = await self._pilot._nats_client.subscribe(subject, cb=nats_callback)
        self._callbacks[subject] = callback
        self._nats_subscriptions[subject] = sub
        logger.info(f"Registered callback for {subject}")

    async def unregister_callback(self, subject: str) -> bool:
        if subject in self._callbacks:
            if subject in self._nats_subscriptions:
                sub = self._nats_subscriptions[subject]
                await sub.unsubscribe()
                del self._nats_subscriptions[subject]
            del self._callbacks[subject]
            logger.info(f"Unregistered callback for {subject}")
            return True
        return False

    async def _cleanup(self) -> None:
        for subject in list(self._callbacks.keys()):
            await self.unregister_callback(subject)


class NatsPushPull(PushPull):
    def __init__(self, pilot: "NatsPilot", js_context: JetStreamContext) -> None:
        self._pilot = pilot
        self._nats_client = self._pilot._nats_client
        self._js: JetStreamContext = js_context
        self._workers: Dict[str, list[asyncio.Task]] = {}
        self._nats_subscriptions: Dict[str, list[Subscription]] = {}
        self._shutdown_event = asyncio.Event()

    @classmethod
    async def create(cls, pilot: "NatsPilot"):
        js_context = pilot._nats_client.jetstream()
        return cls(pilot, js_context)

    @lru_cache(maxsize=10)
    def stream_name(self, subject: str) -> str:
        return subject.upper().replace(".", "-")

    @lru_cache(maxsize=10)
    def consumer_name(self, subject: str) -> str:
        return f"worker-{subject.replace('.', '-')}"

    @lru_cache(maxsize=10)
    def subscriber_subject(self, subject: str) -> str:
        return f"{subject}.subscriber"

    @lru_cache(maxsize=10)
    def job_subject(self, subject: str) -> str:
        return f"{subject}.job"

    @async_once
    async def _ensure_stream(self, subject: str) -> None:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")
        try:
            await self._js.stream_info(self.stream_name(subject))
            logger.info(f"📦 Stream '{self.stream_name(subject)}' already exists")
            return
        except Exception:
            # Stream doesn't exist, continue to create it
            pass

        logger.info(f"📦 Creating JetStream stream: {self.stream_name(subject)}")
        await self._js.add_stream(
            name=self.stream_name(subject),
            subjects=[f"{subject}.>"],  # allow all hierarchy levels
            retention="workqueue",  # Work queue retention for push-pull pattern
            max_msgs_per_subject=1000,
            max_msgs=10000,
            max_bytes=1024 * 1024,  # 1MB
            max_age=3600,  # 1 hour
            max_msg_size=1024 * 1024,  # 1MB
            storage="memory",  # Use memory for better performance
            num_replicas=1,
            duplicate_window=120,  # 2 minutes
        )
        logger.info(f"Created JetStream stream: {self.stream_name(subject)}")

    @async_once
    async def _ensure_consumer(self, subject: str) -> None:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")
        try:
            await self._js.consumer_info(self.stream_name(subject), self.consumer_name(subject))
            logger.info(f"📦 Consumer '{self.consumer_name(subject)}' already exists")
            return
        except Exception:
            # Consumer doesn't exist, continue to create it
            pass

        logger.info(f"📦 Creating JetStream consumer: {self.consumer_name(subject)}")
        await self._js.add_consumer(
            stream=self.stream_name(subject),
            config=api.ConsumerConfig(
                durable_name=self.consumer_name(subject),
                ack_policy=api.AckPolicy.EXPLICIT,
                deliver_policy=api.DeliverPolicy.ALL,
                filter_subject=self.job_subject(subject),
            )
        )
        logger.info(f"Created JetStream consumer: {self.consumer_name(subject)}")

    async def push(self, subject: str, job: Job) -> bool:
        """Push a job to the specified subject using JetStream."""
        try:
            await self._ensure_stream(subject)
            # Update job metadata
            job.recv_ms = int(time.time() * 1000)
            job.status = "new"
            if not hasattr(job, "retry_count") or job.retry_count is None:
                job.retry_count = 0

            # Publish job to JetStream
            payload = json.dumps(job.model_dump(by_alias=True)).encode()
            ack = await self._js.publish(self.job_subject(subject), payload)

            # Notify subscriptions
            await self._notify_subscriptions(subject, job)

            logger.info(f"Pushed job {job.id} to {self.job_subject(subject)}, sequence: {ack.seq}")
            return True

        except Exception as e:
            logger.error(f"Error pushing job to {subject}: {e}", exc_info=True)
            return False

    async def register_worker(
        self, subject: str, callback: Callable[[Job], Awaitable[Any]], n_workers: int = 1
    ) -> None:
        """Register workers for the specified subject using JetStream consumer."""
        try:
            self._shutdown_event.clear()
            await self._ensure_stream(subject)
            await self._ensure_consumer(subject)
            if subject not in self._workers:
                self._workers[subject] = []
            for _ in range(n_workers):
                self._workers[subject].append(
                    asyncio.create_task(self._worker_loop(subject, callback))
                )
            logger.info(f"Registered {n_workers} workers for {subject}")

        except Exception as e:
            logger.error(f"Error registering workers for {subject}: {e}", exc_info=True)
            raise

    async def unregister_worker(self, subject: str) -> None:
        if subject in self._workers:
            self._shutdown_event.set()
            await asyncio.wait_for(
                asyncio.gather(*self._workers[subject], return_exceptions=True), timeout=2.0
            )
            self._workers[subject].clear()
            del self._workers[subject]
        logger.info(f"Unregistered workers for {subject}")

    async def _worker_loop(self, subject: str, callback: Callable[[Job], Awaitable[Any]]) -> None:
        try:
            await self._ensure_stream(subject)
            await self._ensure_consumer(subject)
            subscription = await self._js.pull_subscribe(
                stream=self.stream_name(subject),
                subject=self.job_subject(subject),
                durable=self.consumer_name(subject),
            )

            while not self._shutdown_event.is_set():
                fetch_task = asyncio.create_task(subscription.fetch(batch=1, timeout=10.0))
                shutdown_task = asyncio.create_task(self._shutdown_event.wait())
                done, pending = await asyncio.wait(
                    [fetch_task, shutdown_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                if self._shutdown_event.is_set():
                    break
                if fetch_task in done:
                    await self._handle_fetched_messages(fetch_task, subject, callback)
        except Exception as e:
            logger.error(f"Error in worker loop for {subject}: {e}", exc_info=True)
            raise
        finally:
            await subscription.unsubscribe()

    async def _handle_fetched_messages(self, fetch_task, subject, callback):
        try:
            messages = fetch_task.result()
        except TimeoutError:
            # This is expected when no messages are available
            return
        except Exception as e:
            logger.error(f"Error fetching messages for {subject}: {e}", exc_info=True)
            return
        for message in messages:
            await self._process_job_message(message, subject, callback)

    async def _process_job_message(self, message, subject, callback):
        job_data = json.loads(message.data.decode())
        job = Job(**job_data)
        if not hasattr(job, "retry_count") or job.retry_count is None:
            job.retry_count = 0
        try:
            job.start_ms = int(time.time() * 1000)
            job.status = "running"
            await self._notify_subscriptions(subject, job)
            await callback(job)
            job.end_ms = int(time.time() * 1000)
            job.status = "finished"
            await self._notify_subscriptions(subject, job)
        except asyncio.CancelledError:
            job.end_ms = int(time.time() * 1000)
            job.status = "cancelled"
            await self._notify_subscriptions(subject, job)
        except Exception as e:
            logger.error(f"Error in worker loop for {subject}: {e}", exc_info=True)
            job.end_ms = int(time.time() * 1000)
            job.status = "failed"
            await self._notify_subscriptions(subject, job)
        finally:
            await message.ack()

    async def subscribe(self, subject: str, callback: Callable[[Job], Awaitable[Any]]) -> int:
        """Subscribe to job notifications for the specified subject."""
        subscription_subject = self.subscriber_subject(subject)

        async def nats_callback(msg: NatsMsg):
            try:
                data = json.loads(msg.data.decode())
                job = Job(**data)
                await callback(job)
            except Exception as e:
                logger.error(
                    f"Error in NATS subscription callback for {msg.subject}: {e}", exc_info=True
                )

        sub = await self._nats_client.subscribe(subscription_subject, cb=nats_callback)
        subscription_id = sub._id
        if subscription_subject not in self._nats_subscriptions:
            self._nats_subscriptions[subscription_subject] = []
        self._nats_subscriptions[subscription_subject].append(sub)
        logger.info(f"Subscribed {subscription_id} to {subject}")
        return subscription_id

    async def unsubscribe(self, subject: str, subscription_id: int) -> bool:
        """Unsubscribe from job notifications for the specified subject."""
        subscription_subject = self.subscriber_subject(subject)
        if subscription_subject not in self._nats_subscriptions:
            return False
        try:
            remove_subscription = next(
                sub
                for sub in self._nats_subscriptions[subscription_subject]
                if sub._id == subscription_id
            )
            await remove_subscription.unsubscribe()
            self._nats_subscriptions[subscription_subject].remove(remove_subscription)
            logger.info(f"Unsubscribed from {subscription_subject} with ID {subscription_id}")
            return True
        except (StopIteration, ValueError):
            return False

    async def _notify_subscriptions(self, subject: str, job: Job) -> None:
        """Notify all subscriptions for a subject about a job update."""
        subscription_subject = self.subscriber_subject(subject)
        if subscription_subject not in self._nats_subscriptions:
            return

        await self._nats_client.publish(
            subscription_subject, json.dumps(job.model_dump(by_alias=True)).encode()
        )

    async def _cleanup(self) -> None:
        """Clean up all resources."""
        self._shutdown_event.set()

        # Gather all worker tasks from all subjects
        all_workers = []
        for workers in self._workers.values():
            all_workers.extend(workers)

        if all_workers:
            await asyncio.wait_for(
                asyncio.gather(*all_workers, return_exceptions=True), timeout=10.0
            )

            # Cancel all worker tasks if timeout is reached
            for task in all_workers:
                if not task.done():
                    task.cancel()

        # Clear all collections
        self._workers.clear()
        for _, subs in self._nats_subscriptions.items():
            for sub in subs:
                await sub.unsubscribe()
        self._nats_subscriptions.clear()

        logger.info("NatsPushPull cleanup completed")


class NatsPilot(Pilot):
    def __init__(self, nats_urls: list[str] | None = None, **kwargs) -> None:
        super().__init__()
        if nats_urls is None:
            nats_urls = ["nats://localhost:4222"]
        self._nats_urls = nats_urls
        self._nats_client = NATS()
        self._connected = False
        self._pubsub = NatsPubSub(self)
        self._reqreply = NatsReqReply(self)
        self._pushpull = None  # Will be set after connect
        self._kwargs = kwargs

    def is_connected(self) -> bool:
        return self._connected and self._nats_client.is_connected

    async def connect(self) -> None:
        try:
            await self._nats_client.connect(servers=self._nats_urls, **self._kwargs)
            self._connected = True
            logger.info(f"NatsPilot connected to {self._nats_urls}")
            self._pushpull = await NatsPushPull.create(self)
        except Exception as e:
            logger.error(f"Failed to connect to NATS at {self._nats_urls}: {e}", exc_info=True)
            raise

    async def disconnect(self) -> None:
        try:
            await self._pubsub._cleanup()
            await self._reqreply._cleanup()
            if self._pushpull is not None:
                await self._pushpull._cleanup()
            await self._nats_client.close()
            self._connected = False
            logger.info("NatsPilot disconnected")
        except Exception as e:
            logger.error(f"Error disconnecting from NATS: {e}", exc_info=True)
            raise

    def pubsub(self) -> NatsPubSub:
        return self._pubsub

    def reqreply(self) -> NatsReqReply:
        return self._reqreply

    def pushpull(self) -> NatsPushPull:
        if self._pushpull is None:
            raise RuntimeError("NatsPushPull is not initialized. Call connect() first.")
        return self._pushpull
