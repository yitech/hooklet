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
    def __init__(self, q_size: int = 1000) -> None:
        self._job_queues: Dict[str, asyncio.Queue] = defaultdict(lambda: asyncio.Queue(maxsize=q_size))
        self._worker_tasks: Dict[str, List[asyncio.Task]] = defaultdict(list)
        self._worker_callbacks: Dict[str, Callable[[Job], Awaitable[Any]]] = {}
        self._status_subscribers: Dict[str, List[Callable[[Job], Awaitable[Any]]]] = defaultdict(list)
        self._active_jobs: Dict[str, Dict[str, Job]] = defaultdict(dict)
        self._worker_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._cleanup_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()  # Better shutdown signaling
    
    async def _cleanup(self) -> None:
        """Gracefully cleanup all workers and resources"""
        async with self._cleanup_lock:
            if self._shutdown_event.is_set():
                return
            self._shutdown_event.set()
            
            logger.info("Starting InprocPushPull cleanup")
            
            # First: prevent new jobs from being pushed
            # Second: cancel workers but let them finish current jobs
            # Third: handle queued jobs
            subjects = list(self._worker_tasks.keys())
            
            # Step 1: Mark queued jobs as failed
            for subject in subjects:
                queue = self._job_queues[subject]
                while not queue.empty():
                    try:
                        job = queue.get_nowait()
                        job["status"] = "fail"
                        job["error"] = "System shutdown"
                        job["start_ms"] = job["end_ms"] = int(time.time() * 1000)
                        await self._notify_status_subscribers(subject, job)
                    except asyncio.QueueEmpty:
                        break
            
            # Step 2: Request worker shutdown and wait for completion
            for subject in subjects:
                try:
                    await self.unregister_worker(subject, wait=True)
                except Exception as e:
                    logger.warning(f"Error unregistering workers for {subject}: {e}")
            
            # Step 3: Handle any remaining active jobs
            for subject in subjects:
                for job_id, job in list(self._active_jobs[subject].items()):
                    job["status"] = "fail"
                    job["error"] = "System shutdown"
                    job["end_ms"] = int(time.time() * 1000)
                    await self._notify_status_subscribers(subject, job)
                    del self._active_jobs[subject][job_id]
            
            # Clear all data structures
            self._job_queues.clear()
            self._worker_tasks.clear()
            self._worker_callbacks.clear()
            self._status_subscribers.clear()
            self._active_jobs.clear()
            self._worker_locks.clear()
            
            logger.info("InprocPushPull cleanup completed")

    async def push(self, subject: str, job: Job) -> bool:
        if self._shutdown_event.is_set():
            job.update({
                "status": "fail",
                "error": "System shutting down",
                "start_ms": int(time.time() * 1000),
                "end_ms": int(time.time() * 1000)
            })
            logger.warning(f"Job {job['_id']} rejected - system shutting down")
            return False
            
        try:
            job.update({
                "status": "new",
                "recv_ms": int(time.time() * 1000)
            })
            
            await self._notify_status_subscribers(subject, job)
            queue = self._job_queues[subject]
            
            try:
                await asyncio.wait_for(
                    queue.put(job),
                    timeout=0.1
                )
                logger.info(f"Job {job['_id']} pushed to {subject}")
                return True
            except (asyncio.QueueFull, asyncio.TimeoutError):
                job.update({
                    "status": "fail",
                    "error": "Queue full",
                    "start_ms": int(time.time() * 1000),
                    "end_ms": int(time.time() * 1000)
                })
                await self._notify_status_subscribers(subject, job)
                logger.warning(f"Job {job['_id']} failed - queue full")
                return False
        except Exception as e:
            logger.error(f"Error pushing job {job['_id']}: {e}")
            return False
    
    async def register_worker(self, subject: str, callback: Callable[[Job], Awaitable[Any]], n_workers: int = 1) -> None:
        """Register n_workers to consume jobs from the subject."""
        # First unregister outside the lock to avoid deadlock
        if subject in self._worker_tasks:
            await self.unregister_worker(subject)
            
        async with self._worker_locks[subject]:
            # Store the callback
            self._worker_callbacks[subject] = callback
            
            # Create new worker tasks
            for i in range(n_workers):
                task = asyncio.create_task(
                    self._worker_loop(subject, i),
                    name=f"worker-{subject}-{i}"
                )
                self._worker_tasks[subject].append(task)
            
            logger.info(f"Registered {n_workers} workers for subject {subject}")

    async def unregister_worker(self, subject: str, wait: bool = False) -> int:
        """Unregister all workers for the subject."""
        # Make a copy of tasks outside the lock
        async with self._worker_locks[subject]:
            if subject not in self._worker_tasks:
                return 0
                
            tasks = self._worker_tasks[subject].copy()
            self._worker_tasks[subject].clear()
            self._worker_callbacks.pop(subject, None)
        
        # Cancel and wait outside the lock
        worker_count = len(tasks)
        if tasks:
            for task in tasks:
                task.cancel()
                
            if wait:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True),
                        timeout=2.0
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout waiting for workers to finish for subject {subject}")
        
        logger.info(f"Unregistered {worker_count} workers for subject {subject}")
        return worker_count
    
    async def _worker_loop(self, subject: str, worker_id: int) -> None:
        queue = self._job_queues[subject]
        callback = self._worker_callbacks[subject]
        logger.info(f"Worker {worker_id} started for {subject}")
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    job = await asyncio.wait_for(queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    logger.info(f"Worker {worker_id} cancelled while idle for subject {subject}")
                    break
                
                job_id = job["_id"]
                try:
                    # Update job status
                    job["status"] = "processing"
                    job["start_ms"] = int(time.time() * 1000)
                    self._active_jobs[subject][job_id] = job
                    await self._notify_status_subscribers(subject, job)
                    
                    logger.info(f"Worker {worker_id} processing {job_id}")
                    
                    # Process job (with cancellation awareness)
                    await asyncio.shield(callback(job))  # Protect from immediate cancellation
                    
                    # Finalize job
                    job.update({
                        "status": "finish",
                        "end_ms": int(time.time() * 1000)
                    })
                    logger.info(f"Worker {worker_id} finished {job_id}")
                
                except asyncio.CancelledError:
                    # Handle cancellation during job processing
                    job.update({
                        "status": "fail",
                        "error": "Processing cancelled",
                        "end_ms": int(time.time() * 1000)
                    })
                    await self._notify_status_subscribers(subject, job)
                    logger.warning(f"Worker {worker_id} cancelled during job {job['_id']}")
                    raise
                
                except Exception as e:
                    job.update({
                        "status": "fail",
                        "error": str(e),
                        "end_ms": int(time.time() * 1000)
                    })
                    logger.error(f"Worker {worker_id} failed {job_id}: {e}")
                
                finally:
                    # Final status update and cleanup
                    await self._notify_status_subscribers(subject, job)
                    self._active_jobs[subject].pop(job_id, None)
                    queue.task_done()
        
        except asyncio.CancelledError:
            logger.info(f"Worker {worker_id} cancelled")
        except Exception as e:
            logger.error(f"Worker {worker_id} crashed: {e}")
        finally:
            logger.info(f"Worker {worker_id} exiting")
    
    async def _notify_status_subscribers(self, subject: str, job: Job) -> None:
        if subject not in self._status_subscribers:
            return
            
        tasks = []
        for callback in self._status_subscribers[subject]:
            try:
                tasks.append(callback(job))
            except Exception as e:
                logger.error(f"Status subscriber error: {e}")
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
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
        logger.info("InProcPilot disconnecting...")
        await self._pushpull._cleanup()
        logger.info("InProcPilot disconnected")

    def pubsub(self) -> PubSub:
        return self._pubsub

    def reqreply(self) -> ReqReply:
        return self._reqreply

    def pushpull(self) -> PushPull:
        return self._pushpull



