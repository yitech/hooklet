import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import time

from hooklet.pilot.inproc_pilot import InprocPilot, InprocPubSub, InprocReqReply, InprocPushPull
from hooklet.base.types import Msg, Job


class TestInprocPilot:
    """Test cases for InprocPilot class."""

    @pytest.fixture
    def pilot(self):
        """Create a fresh InprocPilot instance for each test."""
        return InprocPilot()

    @pytest.fixture
    def sample_msg(self):
        """Create a sample message for testing."""
        return {"data": "test_message", "id": "123"}

    @pytest_asyncio.fixture
    async def connected_pilot(self):
        """Create and connect an InprocPilot instance."""
        pilot = InprocPilot()
        await pilot.connect()
        yield pilot
        await pilot.disconnect()

    def test_init(self, pilot):
        """Test InprocPilot initialization."""
        assert pilot is not None
        assert not pilot.is_connected()
        assert pilot.pubsub() is not None
        assert pilot.reqreply() is not None

    @pytest.mark.asyncio
    async def test_connect(self, pilot):
        """Test connecting to the pilot."""
        assert not pilot.is_connected()
        await pilot.connect()
        assert pilot.is_connected()

    @pytest.mark.asyncio
    async def test_disconnect(self, connected_pilot):
        """Test disconnecting from the pilot."""
        assert connected_pilot.is_connected()
        await connected_pilot.disconnect()
        assert not connected_pilot.is_connected()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test using InprocPilot as async context manager."""
        async with InprocPilot() as pilot:
            assert pilot.is_connected()
        assert not pilot.is_connected()

    def test_pubsub_interface(self, pilot):
        """Test that pubsub() returns the correct interface."""
        pubsub = pilot.pubsub()
        assert hasattr(pubsub, 'publish')
        assert hasattr(pubsub, 'subscribe')
        assert hasattr(pubsub, 'unsubscribe')

    def test_reqreply_interface(self, pilot):
        """Test that reqreply() returns the correct interface."""
        reqreply = pilot.reqreply()
        assert hasattr(reqreply, 'request')
        assert hasattr(reqreply, 'register_callback')
        assert hasattr(reqreply, 'unregister_callback')


class TestInprocPubSub:
    """Test cases for InprocPubSub class."""

    @pytest.fixture
    def pubsub(self):
        """Create an InprocPubSub instance."""
        return InprocPubSub()

    @pytest.fixture
    def sample_msg(self):
        """Create a sample message for testing."""
        return {"data": "test_message", "id": "123"}

    @pytest.fixture
    def mock_callback(self):
        """Create a mock async callback."""
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_init(self):
        """Test InprocPubSub initialization."""
        pubsub = InprocPubSub()
        assert pubsub._subscriptions == {}

    @pytest.mark.asyncio
    async def test_publish(self, pubsub, sample_msg, mock_callback):
        """Test publishing a message."""
        subject = "test.subject"
        pubsub.subscribe(subject, mock_callback)
        await pubsub.publish(subject, sample_msg)
        mock_callback.assert_awaited_once_with(sample_msg)

    def test_subscribe(self, pubsub, mock_callback):
        """Test subscribing to a subject."""
        subject = "test.subject"
        subscription_id = pubsub.subscribe(subject, mock_callback)
        
        assert subscription_id == hash(mock_callback)
        assert mock_callback in pubsub._subscriptions[subject]

    def test_subscribe_multiple_callbacks(self, pubsub):
        """Test subscribing multiple callbacks to the same subject."""
        subject = "test.subject"
        callback1 = AsyncMock()
        callback2 = AsyncMock()
        
        sub_id1 = pubsub.subscribe(subject, callback1)
        sub_id2 = pubsub.subscribe(subject, callback2)
        
        assert sub_id1 == hash(callback1)
        assert sub_id2 == hash(callback2)
        assert len(pubsub._subscriptions[subject]) == 2
        assert callback1 in pubsub._subscriptions[subject]
        assert callback2 in pubsub._subscriptions[subject]

    def test_unsubscribe_success(self, pubsub, mock_callback):
        """Test successful unsubscription."""
        subject = "test.subject"
        subscription_id = pubsub.subscribe(subject, mock_callback)
        
        result = pubsub.unsubscribe(subject, subscription_id)
        
        assert result is True
        assert mock_callback not in pubsub._subscriptions[subject]

    def test_unsubscribe_nonexistent_subject(self, pubsub):
        """Test unsubscription from non-existent subject."""
        result = pubsub.unsubscribe("nonexistent.subject", 123)
        assert result is False

    def test_unsubscribe_nonexistent_id(self, pubsub, mock_callback):
        """Test unsubscription with non-existent subscription ID."""
        subject = "test.subject"
        pubsub.subscribe(subject, mock_callback)
        
        result = pubsub.unsubscribe(subject, 99999)  # Non-existent ID
        
        assert result is False
        assert mock_callback in pubsub._subscriptions[subject]  # Should still be subscribed

    def test_get_subscriptions(self, pubsub, mock_callback):
        """Test getting subscriptions for a subject."""
        subject = "test.subject"
        pubsub.subscribe(subject, mock_callback)
        
        subscriptions = pubsub.get_subscriptions(subject)
        
        assert mock_callback in subscriptions
        assert len(subscriptions) == 1

    def test_get_subscriptions_empty(self, pubsub):
        """Test getting subscriptions for a subject with no subscriptions."""
        subject = "test.subject"
        subscriptions = pubsub.get_subscriptions(subject)
        assert subscriptions == []


class TestInprocReqReply:
    """Test cases for InprocReqReply class."""

    @pytest.fixture
    def reqreply(self):
        """Create an InprocReqReply instance."""
        return InprocReqReply()

    @pytest.fixture
    def sample_msg(self):
        """Create a sample message for testing."""
        return {"data": "test_message", "id": "123"}

    @pytest.fixture
    def mock_callback(self):
        """Create a mock async callback."""
        return AsyncMock(return_value={"response": "success"})

    def test_init(self, reqreply):
        """Test InprocReqReply initialization."""
        assert reqreply._callbacks == {}

    @pytest.mark.asyncio
    async def test_register_callback(self, reqreply, mock_callback):
        """Test registering a callback."""
        subject = "test.subject"
        result = await reqreply.register_callback(subject, mock_callback)
        
        assert result == subject
        assert reqreply._callbacks[subject] == mock_callback

    @pytest.mark.asyncio
    async def test_request_success(self, reqreply, mock_callback, sample_msg):
        """Test successful request."""
        subject = "test.subject"
        await reqreply.register_callback(subject, mock_callback)
        
        result = await reqreply.request(subject, sample_msg)
        
        mock_callback.assert_called_once_with(sample_msg)
        assert result == {"response": "success"}

    @pytest.mark.asyncio
    async def test_request_nonexistent_subject(self, reqreply, sample_msg):
        """Test request to non-existent subject."""
        subject = "nonexistent.subject"
        
        with pytest.raises(ValueError, match=f"No callback registered for {subject}"):
            await reqreply.request(subject, sample_msg)

    @pytest.mark.asyncio
    async def test_unregister_callback(self, reqreply, mock_callback):
        """Test unregistering a callback."""
        subject = "test.subject"
        await reqreply.register_callback(subject, mock_callback)
        
        assert subject in reqreply._callbacks
        
        await reqreply.unregister_callback(subject)
        
        assert subject not in reqreply._callbacks

    @pytest.mark.asyncio
    async def test_unregister_nonexistent_callback(self, reqreply):
        """Test unregistering a non-existent callback."""
        subject = "nonexistent.subject"
        # Should not raise an exception
        await reqreply.unregister_callback(subject)


class TestInprocPilotIntegration:
    """Integration tests for InprocPilot with PubSub and ReqReply."""

    @pytest_asyncio.fixture
    async def pilot(self):
        """Create and connect an InprocPilot instance."""
        pilot = InprocPilot()
        await pilot.connect()
        yield pilot
        await pilot.disconnect()

    @pytest.fixture
    def sample_msg(self):
        """Create a sample message for testing."""
        return {"data": "test_message", "id": "123"}

    @pytest.mark.asyncio
    async def test_pubsub_integration(self, pilot, sample_msg):
        """Test pub/sub functionality integration."""
        pubsub = pilot.pubsub()
        received_messages = []
        
        async def message_handler(msg):
            received_messages.append(msg)
        
        subject = "test.subject"
        subscription_id = pubsub.subscribe(subject, message_handler)
        
        await pubsub.publish(subject, sample_msg)
        
        # Give some time for async processing
        await asyncio.sleep(0.1)
        
        assert len(received_messages) == 1
        assert received_messages[0] == sample_msg
        
        # Test unsubscription
        success = pubsub.unsubscribe(subject, subscription_id)
        assert success is True

    @pytest.mark.asyncio
    async def test_reqreply_integration(self, pilot, sample_msg):
        """Test request/reply functionality integration."""
        reqreply = pilot.reqreply()
        response_data = {"response": "success", "processed": True}
        
        async def request_handler(msg):
            return response_data
        
        subject = "test.request"
        await reqreply.register_callback(subject, request_handler)
        
        result = await reqreply.request(subject, sample_msg)
        
        assert result == response_data
        
        # Test unregistration
        await reqreply.unregister_callback(subject)
        
        with pytest.raises(ValueError, match=f"No callback registered for {subject}"):
            await reqreply.request(subject, sample_msg)

    @pytest.mark.asyncio
    async def test_multiple_subscribers(self, pilot, sample_msg):
        """Test multiple subscribers to the same subject."""
        pubsub = pilot.pubsub()
        received_messages1 = []
        received_messages2 = []
        
        async def handler1(msg):
            received_messages1.append(msg)
        
        async def handler2(msg):
            received_messages2.append(msg)
        
        subject = "test.subject"
        pubsub.subscribe(subject, handler1)
        pubsub.subscribe(subject, handler2)
        
        await pubsub.publish(subject, sample_msg)
        
        # Give some time for async processing
        await asyncio.sleep(0.1)
        
        assert len(received_messages1) == 1
        assert len(received_messages2) == 1
        assert received_messages1[0] == sample_msg
        assert received_messages2[0] == sample_msg


class TestInprocPushPull:
    """Test cases for InprocPushPull class."""

    @pytest.fixture
    def pushpull(self):
        """Create an InprocPushPull instance."""
        return InprocPushPull()

    @pytest.fixture
    def sample_job(self):
        """Create a sample job for testing."""
        return {
            "_id": "test_job_123",
            "type": "test_job",
            "data": {"test": "data"},
            "error": None,
            "start_ms": 0,
            "end_ms": 0,
            "status": "pending",
            "retry_count": 0
        }

    @pytest.fixture
    def mock_worker_callback(self):
        """Create a mock worker callback."""
        return AsyncMock()

    @pytest.fixture
    def mock_status_callback(self):
        """Create a mock status callback."""
        return AsyncMock()

    def test_init(self, pushpull):
        """Test InprocPushPull initialization."""
        assert pushpull._job_queues == {}
        assert pushpull._worker_tasks == {}
        assert pushpull._worker_callbacks == {}
        assert pushpull._status_subscribers == {}
        assert pushpull._active_jobs == {}
        assert pushpull._worker_locks == {}

    @pytest.mark.asyncio
    async def test_push_success(self, pushpull, sample_job):
        """Test successful job push."""
        subject = "test.subject"
        result = await pushpull.push(subject, sample_job)
        
        assert result is True
        assert sample_job["status"] == "new"
        assert "recv_ms" in sample_job
        assert sample_job["recv_ms"] > 0

    @pytest.mark.asyncio
    async def test_push_queue_full(self, pushpull, sample_job):
        """Test job push when queue is full."""
        subject = "test.subject"
        
        # Fill the queue
        queue = pushpull._job_queues[subject]
        for i in range(1000):
            job = sample_job.copy()
            job["_id"] = f"job_{i}"
            await queue.put(job)
        
        # Try to push one more job
        result = await pushpull.push(subject, sample_job)
        
        assert result is False
        assert sample_job["status"] == "fail"
        assert sample_job["error"] == "Queue full"

    @pytest.mark.asyncio
    async def test_register_worker(self, pushpull, mock_worker_callback):
        """Test worker registration."""
        subject = "test.subject"
        n_workers = 3
        
        await pushpull.register_worker(subject, mock_worker_callback, n_workers)
        
        assert pushpull._worker_callbacks[subject] == mock_worker_callback
        assert len(pushpull._worker_tasks[subject]) == n_workers
        assert all(task.done() is False for task in pushpull._worker_tasks[subject])

    @pytest.mark.asyncio
    async def test_register_worker_replace_existing(self, pushpull, mock_worker_callback):
        """Test replacing existing workers."""
        subject = "test.subject"
        
        # Register initial workers
        await pushpull.register_worker(subject, mock_worker_callback, 2)
        initial_tasks = pushpull._worker_tasks[subject].copy()
        
        # Register new workers (should replace existing ones)
        new_callback = AsyncMock()
        await pushpull.register_worker(subject, new_callback, 3)
        
        # Check that old tasks are cancelled
        assert all(task.cancelled() for task in initial_tasks)
        assert pushpull._worker_callbacks[subject] == new_callback
        assert len(pushpull._worker_tasks[subject]) == 3

    @pytest.mark.asyncio
    async def test_unregister_worker(self, pushpull, mock_worker_callback):
        """Test worker unregistration."""
        subject = "test.subject"
        
        # Register workers first
        await pushpull.register_worker(subject, mock_worker_callback, 2)
        assert len(pushpull._worker_tasks[subject]) == 2
        
        # Unregister workers
        count = await pushpull.unregister_worker(subject)
        
        assert count == 2
        assert len(pushpull._worker_tasks[subject]) == 0
        assert subject not in pushpull._worker_callbacks

    @pytest.mark.asyncio
    async def test_unregister_worker_nonexistent(self, pushpull):
        """Test unregistering non-existent workers."""
        subject = "test.subject"
        count = await pushpull.unregister_worker(subject)
        assert count == 0

    @pytest.mark.asyncio
    async def test_subscribe_status(self, pushpull, mock_status_callback):
        """Test status subscription."""
        subject = "test.subject"
        subscription_id = await pushpull.subscribe(subject, mock_status_callback)
        
        assert subscription_id == hash(mock_status_callback)
        assert mock_status_callback in pushpull._status_subscribers[subject]

    @pytest.mark.asyncio
    async def test_unsubscribe_status(self, pushpull, mock_status_callback):
        """Test status unsubscription."""
        subject = "test.subject"
        subscription_id = await pushpull.subscribe(subject, mock_status_callback)
        
        result = await pushpull.unsubscribe(subject, subscription_id)
        
        assert result is True
        assert mock_status_callback not in pushpull._status_subscribers[subject]

    @pytest.mark.asyncio
    async def test_unsubscribe_status_nonexistent(self, pushpull):
        """Test unsubscribing from non-existent subscription."""
        subject = "test.subject"
        result = await pushpull.unsubscribe(subject, 99999)
        assert result is False

    @pytest.mark.asyncio
    async def test_job_processing_lifecycle(self, pushpull, sample_job):
        """Test complete job processing lifecycle."""
        subject = "test.subject"
        processed_jobs = []
        
        async def worker_callback(job):
            processed_jobs.append(job)
            await asyncio.sleep(0.01)  # Simulate processing time
        
        # Register worker
        await pushpull.register_worker(subject, worker_callback, 1)
        
        # Push job
        result = await pushpull.push(subject, sample_job)
        assert result is True
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Check job lifecycle
        assert len(processed_jobs) == 1
        processed_job = processed_jobs[0]
        assert processed_job["_id"] == sample_job["_id"]
        assert processed_job["status"] == "finish"
        assert processed_job["end_ms"] > processed_job["start_ms"]

    @pytest.mark.asyncio
    async def test_job_status_notifications(self, pushpull, sample_job, mock_status_callback):
        """Test job status notifications."""
        subject = "test.subject"
        
        # Subscribe to status changes
        await pushpull.subscribe(subject, mock_status_callback)
        
        # Register worker
        async def worker_callback(job):
            await asyncio.sleep(0.01)
        
        await pushpull.register_worker(subject, worker_callback, 1)
        
        # Push job
        await pushpull.push(subject, sample_job)
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Check that status callback was called multiple times
        assert mock_status_callback.call_count >= 3  # new, processing, finish

    @pytest.mark.asyncio
    async def test_job_processing_error(self, pushpull, sample_job):
        """Test job processing with error."""
        subject = "test.subject"
        processed_jobs = []
        
        async def worker_callback(job):
            processed_jobs.append(job)
            raise Exception("Processing error")
        
        # Register worker
        await pushpull.register_worker(subject, worker_callback, 1)
        
        # Push job
        result = await pushpull.push(subject, sample_job)
        assert result is True
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Check job error status
        assert len(processed_jobs) == 1
        processed_job = processed_jobs[0]
        assert processed_job["status"] == "fail"
        assert processed_job["error"] == "Processing error"

    @pytest.mark.asyncio
    async def test_multiple_workers(self, pushpull):
        """Test multiple workers processing jobs."""
        subject = "test.subject"
        processed_jobs = []
        
        async def worker_callback(job):
            processed_jobs.append(job)
            await asyncio.sleep(0.01)
        
        # Register multiple workers
        await pushpull.register_worker(subject, worker_callback, 3)
        
        # Push multiple jobs
        for i in range(5):
            job = {
                "_id": f"job_{i}",
                "type": "test_job",
                "data": {"index": i},
                "error": None,
                "start_ms": 0,
                "end_ms": 0,
                "status": "pending",
                "retry_count": 0
            }
            await pushpull.push(subject, job)
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Check that all jobs were processed
        assert len(processed_jobs) == 5
        job_ids = {job["_id"] for job in processed_jobs}
        expected_ids = {f"job_{i}" for i in range(5)}
        assert job_ids == expected_ids

    @pytest.mark.asyncio
    async def test_worker_cancellation(self, pushpull, sample_job):
        """Test worker cancellation during processing."""
        subject = "test.subject"
        processing_started = asyncio.Event()
        processing_completed = asyncio.Event()
        
        async def worker_callback(job):
            processing_started.set()
            await asyncio.sleep(0.1)  # Long processing time
            processing_completed.set()
        
        # Register worker
        await pushpull.register_worker(subject, worker_callback, 1)
        
        # Push job
        await pushpull.push(subject, sample_job)
        
        # Wait for processing to start
        await processing_started.wait()
        
        # Unregister workers (should cancel them)
        count = await pushpull.unregister_worker(subject)
        assert count == 1
        
        # Check that tasks are cancelled
        assert all(task.cancelled() for task in pushpull._worker_tasks[subject])

    @pytest.mark.asyncio
    async def test_concurrent_subjects(self, pushpull):
        """Test processing jobs on multiple subjects concurrently."""
        subjects = ["subject1", "subject2", "subject3"]
        results = {subject: [] for subject in subjects}
        
        async def worker_callback(job, subject_name):
            results[subject_name].append(job)
            await asyncio.sleep(0.01)
        
        # Register workers for each subject
        for subject in subjects:
            await pushpull.register_worker(subject, lambda job, s=subject: worker_callback(job, s), 1)
        
        # Push jobs to all subjects
        for i in range(3):
            for subject in subjects:
                job = {
                    "_id": f"{subject}_job_{i}",
                    "type": "test_job",
                    "data": {"subject": subject, "index": i},
                    "error": None,
                    "start_ms": 0,
                    "end_ms": 0,
                    "status": "pending",
                    "retry_count": 0
                }
                await pushpull.push(subject, job)
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Check that all jobs were processed
        for subject in subjects:
            assert len(results[subject]) == 3
            job_ids = {job["_id"] for job in results[subject]}
            expected_ids = {f"{subject}_job_{i}" for i in range(3)}
            assert job_ids == expected_ids

    @pytest.mark.asyncio
    async def test_queue_size_limit(self, pushpull):
        """Test that queue size limit is enforced."""
        subject = "test.subject"
        
        # Try to push more jobs than queue capacity
        successful_pushes = 0
        for i in range(1001):  # Queue max size is 1000
            job = {
                "_id": f"job_{i}",
                "type": "test_job",
                "data": {"index": i},
                "error": None,
                "start_ms": 0,
                "end_ms": 0,
                "status": "pending",
                "retry_count": 0
            }
            result = await pushpull.push(subject, job)
            if result:
                successful_pushes += 1
        
        # Should have exactly 1000 successful pushes
        assert successful_pushes == 1000

    @pytest.mark.asyncio
    async def test_cleanup_on_unregister(self, pushpull, mock_worker_callback):
        """Test proper cleanup when unregistering workers."""
        subject = "test.subject"
        
        # Register workers
        await pushpull.register_worker(subject, mock_worker_callback, 2)
        
        # Verify initial state
        assert subject in pushpull._worker_callbacks
        assert len(pushpull._worker_tasks[subject]) == 2
        
        # Unregister workers
        count = await pushpull.unregister_worker(subject)
        
        # Verify cleanup
        assert count == 2
        assert subject not in pushpull._worker_callbacks
        assert len(pushpull._worker_tasks[subject]) == 0
        assert subject not in pushpull._active_jobs

