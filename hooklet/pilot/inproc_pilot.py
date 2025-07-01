import asyncio
import uuid
import time
from collections import defaultdict
from typing import Any, Dict, Tuple, Callable, Awaitable, List, Optional
from hooklet.base import Pilot, PubSub, ReqReply, Msg, PushPull, Job
from hooklet.logger import get_logger

logger = get_logger(__name__)

class InprocPubSub(PubSub):
    def __init__(self) -> None:
        self._subscriptions: Dict[str, list[Callable[[Msg], Awaitable[Any]]]] = defaultdict(list)

    async def publish(self, subject: str, data: Msg) -> None:
        subscriptions = self._subscriptions[subject]
        tasks = [callback(data) for callback in subscriptions]
        await asyncio.gather(*tasks)

    def subscribe(self, subject: str, callback: Callable[[Msg], Awaitable[Any]]) -> int:
        self._subscriptions[subject].append(callback)
        subscription_id = hash(callback)
        logger.info(f"Subscribed to {subject} with ID {subscription_id}")
        return subscription_id
    
    def unsubscribe(self, subject: str, subscription_id: int) -> bool:
        if subject in self._subscriptions:
            try:
                callback_to_remove = next(
                    callback for callback in self._subscriptions[subject] 
                    if hash(callback) == subscription_id
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

    async def request(self, subject: str, data: Msg) -> Any:
        if subject not in self._callbacks:
            raise ValueError(f"No callback registered for {subject}")
        return await self._callbacks[subject](data)
    
    async def register_callback(self, subject: str, callback: Callable[[Any], Awaitable[Any]]) -> str:
        self._callbacks[subject] = callback
        return subject
    
    async def unregister_callback(self, subject: str) -> None:
        if subject in self._callbacks:
            del self._callbacks[subject]

class InprocPushPull(PushPull):
    def __init__(self) -> None:
        # Job queues for each subject
        self._job_queues: Dict[str, asyncio.Queue] = defaultdict(lambda: asyncio.Queue(maxsize=1000))
        self._worker_tasks: Dict[str, List[asyncio.Task]] = defaultdict(list)
        self._worker_callbacks: Dict[str, Callable[[Job], Awaitable[Any]]] = {}
        self._status_subscribers: Dict[str, List[Callable[[Job], Awaitable[Any]]]] = defaultdict(list)
        self._active_jobs: Dict[str, Dict[str, Job]] = defaultdict(dict)
        self._worker_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
    
    async def push(self, subject: str, job: Job) -> bool:
        """Push a job to the subject queue."""
        try:
            # Set initial status
            job["status"] = "new"
            job["recv_ms"] = int(time.time() * 1000)
            
            # Notify status subscribers
            await self._notify_status_subscribers(subject, job)
            
            # Try to put job in queue
            queue = self._job_queues[subject]
            try:
                queue.put_nowait(job)
                logger.info(f"Job {job['_id']} pushed to subject {subject}")
                return True
            except asyncio.QueueFull:
                # Queue is full, mark job as failed
                job["status"] = "fail"
                job["error"] = "Queue full"
                job["start_ms"] = int(time.time() * 1000)
                job["end_ms"] = job["start_ms"]
                await self._notify_status_subscribers(subject, job)
                logger.warning(f"Job {job['_id']} failed - queue full for subject {subject}")
                return False
        except Exception as e:
            logger.error(f"Error pushing job {job['_id']} to subject {subject}: {e}")
            return False
    
    async def register_worker(self, subject: str, callback: Callable[[Job], Awaitable[Any]], n_workers: int = 1) -> None:
        """Register n_workers to consume jobs from the subject."""
        async with self._worker_locks[subject]:
            # Unregister existing workers first (without lock since we already have it)
            if subject in self._worker_tasks:
                # Cancel all worker tasks
                tasks = self._worker_tasks[subject]
                for task in tasks:
                    if not task.done():
                        task.cancel()
                
                # Wait for all tasks to complete with timeout
                if tasks:
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*tasks, return_exceptions=True),
                            timeout=1.0  # 1 second timeout
                        )
                    except asyncio.TimeoutError:
                        logger.warning(f"Timeout waiting for workers to complete for subject {subject}")
                    except Exception as e:
                        logger.warning(f"Error waiting for workers to complete for subject {subject}: {e}")
                
                # Clear worker data
                self._worker_tasks[subject].clear()
                if subject in self._worker_callbacks:
                    del self._worker_callbacks[subject]
            
            # Store the callback
            self._worker_callbacks[subject] = callback
            
            # Create new worker tasks
            for i in range(n_workers):
                task = asyncio.create_task(self._worker_loop(subject, i))
                self._worker_tasks[subject].append(task)
            
            logger.info(f"Registered {n_workers} workers for subject {subject}")
    
    async def unregister_worker(self, subject: str) -> int:
        """Unregister all workers for the subject."""
        async with self._worker_locks[subject]:
            if subject not in self._worker_tasks:
                return 0
            
            # Cancel all worker tasks
            tasks = self._worker_tasks[subject]
            for task in tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for all tasks to complete with timeout
            if tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True),
                        timeout=1.0  # 1 second timeout
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout waiting for workers to complete for subject {subject}")
                except Exception as e:
                    logger.warning(f"Error waiting for workers to complete for subject {subject}: {e}")
            
            # Clear worker data
            worker_count = len(tasks)
            self._worker_tasks[subject].clear()
            if subject in self._worker_callbacks:
                del self._worker_callbacks[subject]
            
            logger.info(f"Unregistered {worker_count} workers for subject {subject}")
            return worker_count
    
    async def subscribe(self, subject: str, callback: Callable[[Job], Awaitable[Any]]) -> int:
        """Subscribe to job status changes."""
        self._status_subscribers[subject].append(callback)
        subscription_id = hash(callback)
        logger.info(f"Subscribed to job status for subject {subject} with ID {subscription_id}")
        return subscription_id
    
    async def unsubscribe(self, subject: str, subscription_id: int) -> bool:
        """Unsubscribe from job status changes."""
        if subject in self._status_subscribers:
            try:
                callback_to_remove = next(
                    callback for callback in self._status_subscribers[subject] 
                    if hash(callback) == subscription_id
                )
                self._status_subscribers[subject].remove(callback_to_remove)
                logger.info(f"Unsubscribed from job status for subject {subject} with ID {subscription_id}")
                return True
            except StopIteration:
                pass
        return False
    
    async def _worker_loop(self, subject: str, worker_id: int) -> None:
        """Worker loop that consumes jobs from the queue."""
        queue = self._job_queues[subject]
        callback = self._worker_callbacks[subject]
        
        logger.info(f"Worker {worker_id} started for subject {subject}")
        
        try:
            while True:
                # Get job from queue with proper cancellation handling
                try:
                    job = await asyncio.wait_for(queue.get(), timeout=None)
                except asyncio.TimeoutError:
                    # This should never happen with timeout=None, but handle it gracefully
                    continue
                except asyncio.CancelledError:
                    logger.info(f"Worker {worker_id} cancelled while waiting for job for subject {subject}")
                    raise
                except Exception as e:
                    # Handle any other exceptions (like RuntimeError when event loop is closed)
                    logger.info(f"Worker {worker_id} exiting due to exception: {e}")
                    break
                
                try:
                    # Mark job as processing
                    job["status"] = "processing"
                    self._active_jobs[subject][job["_id"]] = job
                    await self._notify_status_subscribers(subject, job)
                    
                    logger.info(f"Worker {worker_id} processing job {job['_id']} for subject {subject}")
                    
                    # Process the job
                    await callback(job)
                    
                    # Mark job as finished
                    job["status"] = "finish"
                    job["end_ms"] = int(time.time() * 1000)
                    await self._notify_status_subscribers(subject, job)
                    
                    logger.info(f"Worker {worker_id} finished job {job['_id']} for subject {subject}")
                    
                except Exception as e:
                    # Mark job as failed
                    job["status"] = "fail"
                    job["error"] = str(e)
                    job["end_ms"] = int(time.time() * 1000)
                    await self._notify_status_subscribers(subject, job)
                    
                    logger.error(f"Worker {worker_id} failed job {job['_id']} for subject {subject}: {e}")
                
                finally:
                    # Remove from active jobs
                    if job["_id"] in self._active_jobs[subject]:
                        del self._active_jobs[subject][job["_id"]]
                    
                    # Mark task as done
                    try:
                        queue.task_done()
                    except RuntimeError:
                        # Queue might be closed, ignore the error
                        pass
        
        except asyncio.CancelledError:
            logger.info(f"Worker {worker_id} cancelled for subject {subject}")
            raise
        except Exception as e:
            logger.error(f"Worker {worker_id} error for subject {subject}: {e}")
            raise
    
    async def _notify_status_subscribers(self, subject: str, job: Job) -> None:
        """Notify all status subscribers about job status change."""
        if subject in self._status_subscribers:
            subscribers = self._status_subscribers[subject]
            tasks = [callback(job) for callback in subscribers]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

class InprocPilot(Pilot):
    def __init__(self) -> None:
        super().__init__()
        self._connected = False
        self._pubsub = InprocPubSub()
        self._reqreply = InprocReqReply()
        self._pushpull = InprocPushPull()

    def is_connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        self._connected = True
        logger.info("InProcPilot connected")

    async def disconnect(self) -> None:
        self._connected = False
        logger.info("InProcPilot disconnected")

    def pubsub(self) -> PubSub:
        return self._pubsub

    def reqreply(self) -> ReqReply:
        return self._reqreply

    def pushpull(self) -> PushPull:
        return self._pushpull



