from typing import Any, Awaitable, Callable, Dict
import asyncio
import time
import json

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg as NatsMsg
from nats.aio.subscription import Subscription
from nats.js import JetStreamContext

from hooklet.base import Job, Msg, Pilot, PubSub, PushPull, ReqReply
from hooklet.logger import get_logger

logger = get_logger(__name__)


class NatsPubSub(PubSub):
    def __init__(self, pilot: "NatsPilot") -> None:
        self._pilot = pilot
        self._subscriptions: Dict[str, list[Callable[[Msg], Awaitable[Any]]]] = {}
        self._nats_subscriptions: Dict[str, list[Subscription]] = {}

    async def publish(self, subject: str, data: Msg) -> None:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")
        import json

        payload = json.dumps(data).encode()
        await self._pilot._nats_client.publish(subject, payload)
        logger.debug(f"Published to {subject}: {data}")

    async def subscribe(self, subject: str, callback: Callable[[Msg], Awaitable[Any]]) -> int:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")
        subscription_id = hash(callback)

        async def nats_callback(msg: NatsMsg):
            try:
                import json

                data = json.loads(msg.data.decode())
                await callback(data)
            except Exception as e:
                logger.error(
                    f"Error in NATS subscription callback for {subject}: {e}", exc_info=True
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
                if hash(callback) == subscription_id
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

    async def request(self, subject: str, data: Msg) -> Any:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")
        import json

        payload = json.dumps(data).encode()
        try:
            response = await self._pilot._nats_client.request(subject, payload, timeout=30.0)
            return json.loads(response.data.decode())
        except Exception as e:
            logger.error(f"Error making request to {subject}: {e}", exc_info=True)
            raise

    async def register_callback(
        self, subject: str, callback: Callable[[Any], Awaitable[Any]]
    ) -> str:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")

        async def nats_callback(msg: NatsMsg):
            try:
                import json

                data = json.loads(msg.data.decode())
                response = await callback(data)
                response_payload = json.dumps(response).encode()
                await msg.respond(response_payload)
            except Exception as e:
                logger.error(f"Error in NATS request callback for {subject}: {e}", exc_info=True)
                error_response = {"error": str(e)}
                await msg.respond(json.dumps(error_response).encode())

        sub = await self._pilot._nats_client.subscribe(subject, cb=nats_callback)
        self._callbacks[subject] = callback
        self._nats_subscriptions[subject] = sub
        logger.info(f"Registered callback for {subject}")
        return subject

    async def unregister_callback(self, subject: str) -> None:
        if subject in self._callbacks:
            if subject in self._nats_subscriptions:
                sub = self._nats_subscriptions[subject]
                await sub.unsubscribe()
                del self._nats_subscriptions[subject]
            del self._callbacks[subject]
            logger.info(f"Unregistered callback for {subject}")

    async def _cleanup(self) -> None:
        for subject in list(self._callbacks.keys()):
            await self.unregister_callback(subject)


class NatsPushPull(PushPull):
    def __init__(self, pilot: "NatsPilot") -> None:
        self._pilot = pilot
        self._js: JetStreamContext | None = None
        self._workers: Dict[str, list[Callable[[Job], Awaitable[Any]]]] = {}
        self._subscriptions: Dict[str, list[Callable[[Job], Awaitable[Any]]]] = {}
        self._nats_subscriptions: Dict[str, list[Subscription]] = {}
        self._worker_tasks: Dict[str, list[asyncio.Task[Any]]] = {}
        self._shutdown_event = asyncio.Event()
        self._shutdown_event.set()

    async def _ensure_js_context(self) -> JetStreamContext:
        """Ensure JetStream context is available."""
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")
        
        if self._js is None:
            self._js = self._pilot._nats_client.jetstream()
        
        return self._js

    async def _ensure_stream(self, subject: str) -> None:
        """Ensure the stream exists for the given subject."""
        js = await self._ensure_js_context()
        stream_name = f"hooklet-{subject.replace('.', '-')}"
        
        try:
            # Try to get stream info to check if it exists
            await js.streams_info(stream_name)
        except Exception:
            # Stream doesn't exist, create it
            await js.add_stream(
                name=stream_name,
                subjects=[subject],
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
            logger.info(f"Created JetStream stream: {stream_name}")

    async def push(self, subject: str, job: Job) -> bool:
        """Push a job to the specified subject using JetStream."""
        try:
            await self._ensure_stream(subject)
            js = await self._ensure_js_context()
            
            # Update job metadata
            job.update({
                "recv_ms": int(time.time() * 1000),
                "status": "new",
            })
            
            # Publish job to JetStream
            payload = json.dumps(job).encode()
            ack = await js.publish(subject, payload)
            
            # Notify subscriptions
            await self._notify_subscriptions(subject, job)
            
            logger.debug(f"Pushed job {job['_id']} to {subject}, sequence: {ack.seq}")
            return True
            
        except Exception as e:
            logger.error(f"Error pushing job to {subject}: {e}", exc_info=True)
            return False

    async def register_worker(
        self, subject: str, callback: Callable[[Job], Awaitable[Any]], n_workers: int = 1
    ) -> None:
        """Register workers for the specified subject using JetStream consumer."""
        try:
            await self._ensure_stream(subject)
            js = await self._ensure_js_context()
            
            stream_name = f"hooklet-{subject.replace('.', '-')}"
            consumer_name = f"worker-{subject.replace('.', '-')}-{int(time.time())}"
            
            # Create consumer for work queue pattern
            await js.add_consumer(
                stream_name,
                durable_name=consumer_name,
                deliver_policy="all",
                ack_policy="explicit",
                max_deliver=3,  # Retry up to 3 times
                ack_wait=30,  # 30 seconds ack wait
                max_waiting=1,  # Only one pending message per consumer
                filter_subject=subject,
            )
            
            # Store worker callbacks
            if subject not in self._workers:
                self._workers[subject] = []
            self._workers[subject].extend([callback] * n_workers)
            
            # Start worker tasks
            if subject not in self._worker_tasks:
                self._worker_tasks[subject] = []
            
            self._shutdown_event.clear()
            for i in range(n_workers):
                task = asyncio.create_task(
                    self._worker_loop(subject, consumer_name, callback, i)
                )
                self._worker_tasks[subject].append(task)
            
            logger.info(f"Registered {n_workers} workers for {subject}")
            
        except Exception as e:
            logger.error(f"Error registering workers for {subject}: {e}", exc_info=True)
            raise

    async def _worker_loop(
        self, subject: str, consumer_name: str, callback: Callable[[Job], Awaitable[Any]], worker_id: int
    ) -> None:
        """Worker loop that processes jobs from JetStream."""
        js = await self._ensure_js_context()
        stream_name = f"hooklet-{subject.replace('.', '-')}"
        
        while not self._shutdown_event.is_set():
            try:
                # Fetch messages from the consumer
                logger.info(f"[Worker {worker_id}] About to call pull_subscribe for {subject}")
                messages = await js.pull_subscribe(
                    subject,
                    consumer_name,
                    stream=stream_name,
                    batch_size=1,
                    timeout=1.0,  # 1 second timeout
                )
                logger.info(f"[Worker {worker_id}] Got messages iterator: {messages}")
                
                logger.info(f"[Worker {worker_id}] About to iterate over messages for {subject}")
                async for msg in messages:
                    logger.info(f"[Worker {worker_id}] Got message: {msg}")
                    if self._shutdown_event.is_set():
                        await msg.ack()
                        break
                    
                    try:
                        # Parse job data
                        job_data = json.loads(msg.data.decode())
                        logger.info(f"[Worker {worker_id}] Parsed job_data: {job_data}")
                        job = Job(**job_data)
                        logger.info(f"[Worker {worker_id}] Created Job object: {job}")
                        
                        # Update job status
                        job.update({
                            "start_ms": int(time.time() * 1000),
                            "status": "running",
                        })
                        
                        logger.info(f"[Worker {worker_id}] About to call callback for job {job['_id']} on {subject}")
                        await callback(job)
                        logger.info(f"[Worker {worker_id}] Callback finished for job {job['_id']} on {subject}")
                        
                        # Update job status on success
                        job.update({
                            "end_ms": int(time.time() * 1000),
                            "status": "finished",
                        })
                        
                        # Acknowledge the message
                        await msg.ack()
                        
                        logger.debug(f"Worker {worker_id} processed job {job['_id']} from {subject}")
                        
                    except asyncio.CancelledError:
                        await msg.ack()
                        raise
                    except Exception as e:
                        logger.error(f"[Worker {worker_id}] Exception in callback for job on {subject}: {e} job_data={job_data if 'job_data' in locals() else 'N/A'}", exc_info=True)
                        # Update job status on failure
                        try:
                            job_data = json.loads(msg.data.decode())
                            job = Job(**job_data)
                            job.update({
                                "end_ms": int(time.time() * 1000),
                                "status": "failed",
                                "error": str(e),
                                "retry_count": job.get("retry_count", 0) + 1,
                            })
                        except Exception as e2:
                            logger.error(f"[Worker {worker_id}] Exception updating job status after callback failure: {e2}")
                        # Acknowledge the message (JetStream will handle retries)
                        await msg.ack()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Worker {worker_id}] Outer loop error for {subject}: {e}", exc_info=True)
                await asyncio.sleep(1)  # Wait before retrying
        
        logger.info(f"Worker {worker_id} loop for {subject} shutdown")

    async def subscribe(self, subject: str, callback: Callable[[Job], Awaitable[Any]]) -> int:
        """Subscribe to job notifications for the specified subject."""
        subscription_id = hash(callback)
        
        if subject not in self._subscriptions:
            self._subscriptions[subject] = []
        
        self._subscriptions[subject].append(callback)
        
        logger.info(f"Subscribed {subscription_id} to {subject}")
        return subscription_id

    async def unsubscribe(self, subject: str, subscription_id: int) -> bool:
        """Unsubscribe from job notifications for the specified subject."""
        if subject not in self._subscriptions:
            return False
        
        try:
            callback_to_remove = next(
                callback
                for callback in self._subscriptions[subject]
                if hash(callback) == subscription_id
            )
            self._subscriptions[subject].remove(callback_to_remove)
            
            if not self._subscriptions[subject]:
                del self._subscriptions[subject]
            
            logger.info(f"Unsubscribed {subscription_id} from {subject}")
            return True
            
        except StopIteration:
            return False

    async def _notify_subscriptions(self, subject: str, job: Job) -> None:
        """Notify all subscriptions for a subject about a job update."""
        if subject not in self._subscriptions:
            return
        
        for callback in self._subscriptions[subject]:
            try:
                await callback(job)
            except Exception as e:
                logger.error(f"Error in subscription callback for {subject}: {e}", exc_info=True)

    async def _cleanup(self) -> None:
        """Clean up all resources."""
        self._shutdown_event.set()
        
        # Cancel all worker tasks
        for subject, tasks in self._worker_tasks.items():
            for task in tasks:
                if not task.done():
                    task.cancel()
        
        # Wait for all tasks to complete
        if self._worker_tasks:
            await asyncio.gather(*[
                task for tasks in self._worker_tasks.values() for task in tasks
            ], return_exceptions=True)
        
        # Clear all collections
        self._workers.clear()
        self._subscriptions.clear()
        self._nats_subscriptions.clear()
        self._worker_tasks.clear()
        
        logger.info("NatsPushPull cleanup completed")


class NatsPilot(Pilot):
    def __init__(self, nats_url: str = "nats://localhost:4222", **kwargs) -> None:
        super().__init__()
        self._nats_url = nats_url
        self._nats_client = NATS()
        self._connected = False
        self._pubsub = NatsPubSub(self)
        self._reqreply = NatsReqReply(self)
        self._pushpull = NatsPushPull(self)
        self._kwargs = kwargs

    def is_connected(self) -> bool:
        return self._connected and self._nats_client.is_connected

    async def connect(self) -> None:
        try:
            await self._nats_client.connect(self._nats_url, **self._kwargs)
            self._connected = True
            logger.info(f"NatsPilot connected to {self._nats_url}")
        except Exception as e:
            logger.error(f"Failed to connect to NATS at {self._nats_url}: {e}", exc_info=True)
            raise

    async def disconnect(self) -> None:
        try:
            await self._pubsub._cleanup()
            await self._reqreply._cleanup()
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
        return self._pushpull
