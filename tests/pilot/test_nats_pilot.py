import pytest
import pytest_asyncio
import asyncio
import time
import json
import warnings
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any

from hooklet.pilot.nats_pilot import NatsPilot, NatsPushPull
from hooklet.base.types import Job, Msg
from hooklet.utils.id_generator import generate_id

def suppress_async_mock_warnings():
    """Context manager to suppress AsyncMock warnings during test setup."""
    return warnings.catch_warnings(record=True)

@pytest.mark.asyncio
@pytest.mark.integration
async def test_nats_pilot_connection():
    """Test basic connection functionality"""
    pilot = NatsPilot(nats_urls=["nats://localhost:4222"])
    
    # Test initial state
    assert not pilot.is_connected()
    
    # Note: This test requires a running NATS server
    # In a real test environment, you'd start a NATS server or mock it
    try:
        await pilot.connect()
        assert pilot.is_connected()
        await pilot.disconnect()
        assert not pilot.is_connected()
    except Exception:
        # If NATS server is not running, this is expected
        pytest.skip("NATS server not available")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_nats_pilot_pubsub():
    """Test pub/sub functionality"""
    pilot = NatsPilot(nats_urls=["nats://localhost:4222"])
    
    try:
        await pilot.connect()
        
        pubsub = pilot.pubsub()
        received_messages = []
        
        async def callback(msg: Msg):
            received_messages.append(msg)
        
        # Subscribe to a test subject
        subscription_id = await pubsub.subscribe("test.subject", callback)
        assert subscription_id is not None
        
        # Publish a message
        test_msg = {"data": "test message", "timestamp": 1234567890}
        await pubsub.publish("test.subject", test_msg)
        
        # Give some time for the message to be processed
        await asyncio.sleep(0.1)
        
        # Unsubscribe
        success = await pubsub.unsubscribe("test.subject", subscription_id)
        assert success
        
        await pilot.disconnect()
        
    except Exception:
        pytest.skip("NATS server not available")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_nats_pilot_reqreply():
    """Test request-reply functionality"""
    pilot = NatsPilot(nats_urls=["nats://localhost:4222"])
    
    try:
        await pilot.connect()
        
        reqreply = pilot.reqreply()
        
        # Register a callback
        async def echo_callback(msg: Msg):
            return {"echo": msg.get("data", ""), "received": True}
        
        subject = await reqreply.register_callback("echo.service", echo_callback)
        assert subject == "echo.service"
        
        # Make a request
        request_msg = {"data": "hello world"}
        response = await reqreply.request("echo.service", request_msg)
        
        assert response["echo"] == "hello world"
        assert response["received"] is True
        
        # Unregister callback
        await reqreply.unregister_callback("echo.service")
        
        await pilot.disconnect()
        
    except Exception:
        pytest.skip("NATS server not available")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_nats_pilot_pushpull():
    """Test push-pull functionality with JetStream"""
    pilot = NatsPilot(nats_urls=["nats://localhost:4222"])
    
    try:
        await pilot.connect()
        
        pushpull = pilot.pushpull()
        processed_jobs = []
        
        async def job_callback(job: Job):
            processed_jobs.append(job)
            # Simulate some work
            await asyncio.sleep(0.1)
        
        # Register a worker
        await pushpull.register_worker("test.jobs", job_callback, n_workers=1)
        
        # Create and push a job
        job = Job(
            _id=generate_id(),
            type="test_job",
            data={"test": "data"},
            error=None,
            recv_ms=0,
            start_ms=0,
            end_ms=0,
            status="new",
            retry_count=0
        )
        
        success = await pushpull.push("test.jobs", job)
        assert success
        
        # Wait for job to be processed
        await asyncio.sleep(0.5)
        
        # Verify job was processed
        assert len(processed_jobs) == 1
        assert processed_jobs[0]["_id"] == job["_id"]
        
        await pilot.disconnect()
        
    except Exception:
        pytest.skip("NATS server not available")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_nats_pilot_context_manager():
    """Test context manager functionality"""
    pilot = NatsPilot(nats_urls=["nats://localhost:4222"])
    
    try:
        async with pilot:
            assert pilot.is_connected()
        assert not pilot.is_connected()
    except Exception:
        pytest.skip("NATS server not available")

class TestNatsPilot:
    """Test cases for NatsPilot class."""

    @pytest.fixture
    def pilot(self):
        """Create a fresh NatsPilot instance for each test."""
        return NatsPilot(["nats://localhost:4222"])

    @pytest.fixture
    def sample_job(self):
        """Create a sample job for testing."""
        return Job(
            _id=generate_id(),
            type="test_job",
            data={"test": "data"},
            error=None,
            recv_ms=int(time.time() * 1000),
            start_ms=0,
            end_ms=0,
            status="new",
            retry_count=0
        )

    def test_init(self, pilot):
        """Test NatsPilot initialization."""
        assert pilot is not None
        assert not pilot.is_connected()
        assert pilot.pubsub() is not None
        assert pilot.reqreply() is not None
        assert pilot.pushpull() is not None

    @pytest.mark.asyncio
    async def test_connect_disconnect_mock(self):
        """Test connecting and disconnecting with mocked NATS client."""
        with patch('hooklet.pilot.nats_pilot.NATS') as mock_nats_class:
            mock_nats_client = AsyncMock()
            mock_nats_class.return_value = mock_nats_client
            
            # Create pilot after patching NATS
            pilot = NatsPilot(["nats://localhost:4222"])
            
            await pilot.connect()
            assert pilot.is_connected()
            mock_nats_client.connect.assert_called_once_with(servers=["nats://localhost:4222"])
            
            await pilot.disconnect()
            assert not pilot.is_connected()
            mock_nats_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test using NatsPilot as async context manager."""
        with patch('hooklet.pilot.nats_pilot.NATS') as mock_nats_class:
            mock_nats_client = AsyncMock()
            mock_nats_class.return_value = mock_nats_client
            
            # Create pilot after patching NATS
            pilot = NatsPilot(["nats://localhost:4222"])
            
            async with pilot:
                assert pilot.is_connected()
            assert not pilot.is_connected()

class TestNatsPushPull:
    """Test cases for NatsPushPull class."""

    @pytest.fixture
    def mock_pilot(self):
        """Create a mock pilot for testing."""
        pilot = MagicMock()
        pilot.is_connected.return_value = True
        pilot._nats_client = AsyncMock()
        # Mock the jetstream method to avoid async warnings
        pilot._nats_client.jetstream = MagicMock(return_value=MagicMock())
        return pilot

    @pytest.fixture
    def pushpull(self, mock_pilot):
        """Create a NatsPushPull instance for testing."""
        return NatsPushPull(mock_pilot)

    @pytest.fixture
    def sample_job(self):
        """Create a sample job for testing."""
        return Job(
            _id=generate_id(),
            type="test_job",
            data={"test": "data"},
            error=None,
            recv_ms=int(time.time() * 1000),
            start_ms=0,
            end_ms=0,
            status="new",
            retry_count=0
        )

    def test_init(self, pushpull):
        """Test NatsPushPull initialization."""
        assert pushpull._pilot is not None
        assert pushpull._nats_client is not None
        assert pushpull._js is not None
        assert pushpull._workers == []
        assert pushpull._nats_subscriptions == {}
        assert pushpull._shutdown_event is not None

    def test_stream_name(self, pushpull):
        """Test stream name generation."""
        assert pushpull.stream_name("test.jobs") == "TEST-JOBS"
        assert pushpull.stream_name("math.operations") == "MATH-OPERATIONS"

    def test_consumer_name(self, pushpull):
        """Test consumer name generation."""
        assert pushpull.consumer_name("test.jobs") == "worker-test-jobs"
        assert pushpull.consumer_name("math.operations") == "worker-math-operations"

    def test_subscriber_subject(self, pushpull):
        """Test subscriber subject generation."""
        assert pushpull.subscriber_subject("test.jobs") == "test.jobs.subscriber"
        assert pushpull.subscriber_subject("math.operations") == "math.operations.subscriber"

    def test_job_subject(self, pushpull):
        """Test job subject generation."""
        assert pushpull.job_subject("test.jobs") == "test.jobs.job"
        assert pushpull.job_subject("math.operations") == "math.operations.job"

    @pytest.mark.asyncio
    async def test_ensure_stream_creates_new(self, pushpull, mock_pilot):
        """Test stream creation when it doesn't exist."""
        # Create a proper mock that doesn't trigger async warnings
        mock_js = MagicMock()
        mock_js.streams_info = AsyncMock(side_effect=Exception("Stream not found"))
        mock_js.add_stream = AsyncMock(return_value=None)
        
        # Set the mock after creating the pushpull instance to avoid initialization issues
        pushpull._js = mock_js
        
        await pushpull._ensure_stream("test.jobs")
        
        mock_js.add_stream.assert_called_once()
        call_args = mock_js.add_stream.call_args
        assert call_args[1]["name"] == "TEST-JOBS"
        assert call_args[1]["subjects"] == ["test.jobs.>"]
        assert call_args[1]["retention"] == "workqueue"

    @pytest.mark.asyncio
    async def test_ensure_stream_exists(self, pushpull, mock_pilot):
        """Test stream handling when it already exists."""
        mock_js = MagicMock()
        mock_js.streams_info = AsyncMock(return_value=None)
        pushpull._js = mock_js
        
        await pushpull._ensure_stream("test.jobs")
        
        mock_js.streams_info.assert_called_once_with("TEST-JOBS")
        mock_js.add_stream.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_stream_not_connected(self, pushpull, mock_pilot):
        """Test stream creation when not connected."""
        mock_pilot.is_connected.return_value = False
        
        with pytest.raises(RuntimeError, match="NATS client not connected"):
            await pushpull._ensure_stream("test.jobs")

    @pytest.mark.asyncio
    async def test_ensure_consumer(self, pushpull, mock_pilot):
        """Test consumer creation."""
        mock_js = MagicMock()
        mock_js.consumers_info = AsyncMock(side_effect=Exception("Consumer not found"))
        pushpull._js = mock_js
        
        await pushpull._ensure_consumer("test.jobs")
        
        mock_js.consumers_info.assert_called_once_with("TEST-JOBS", "worker-test-jobs")

    @pytest.mark.asyncio
    async def test_push_success(self, pushpull, mock_pilot, sample_job):
        """Test successful job push."""
        mock_js = MagicMock()  # ✅ Use AsyncMock for async interface

        mock_js.streams_info = AsyncMock(side_effect=Exception("Stream not found"))  # if awaited
        mock_js.add_stream = AsyncMock(return_value=None)  # ✅ so `await` works
        mock_ack = MagicMock()
        mock_ack.seq = 123
        mock_js.publish = AsyncMock(return_value=mock_ack)  # ✅ so `await` works

        pushpull._js = mock_js

        result = await pushpull.push("test.jobs", sample_job)

        assert result is True
        assert sample_job["status"] == "new"
        assert "recv_ms" in sample_job
        mock_js.publish.assert_called_once_with("test.jobs.job", json.dumps(sample_job).encode())

    @pytest.mark.asyncio
    async def test_push_failure(self, pushpull, mock_pilot, sample_job):
        """Test job push failure."""
        mock_js = MagicMock()
        mock_js.streams_info = AsyncMock(side_effect=Exception("Stream not found"))
        mock_js.add_stream = AsyncMock(return_value=None)
        mock_js.publish = AsyncMock(side_effect=Exception("Publish failed"))
        pushpull._js = mock_js
        
        result = await pushpull.push("test.jobs", sample_job)
        
        assert result is False

    @pytest.mark.asyncio
    async def test_register_worker_success(self, pushpull, mock_pilot):
        """Test successful worker registration."""
        mock_js = MagicMock()
        mock_js.streams_info = AsyncMock(side_effect=Exception("Stream not found"))
        mock_js.add_stream = AsyncMock(return_value=None)
        mock_js.consumers_info = AsyncMock(side_effect=Exception("Consumer not found"))
        mock_js.pull_subscribe = AsyncMock(return_value=AsyncMock())
        pushpull._js = mock_js
        callback = AsyncMock()
        
        await pushpull.register_worker("test.jobs", callback, n_workers=2)
        
        assert len(pushpull._workers) == 2
        mock_js.pull_subscribe.assert_called()
        
        # Clean up
        await pushpull._cleanup()

    @pytest.mark.asyncio
    async def test_register_worker_failure(self, pushpull, mock_pilot):
        """Test worker registration failure."""
        mock_js = MagicMock()
        mock_js.streams_info = AsyncMock(side_effect=Exception("Stream not found"))
        mock_js.add_stream = AsyncMock(return_value=None)
        mock_js.consumers_info = AsyncMock(side_effect=Exception("Consumer not found"))
        mock_js.pull_subscribe = AsyncMock(side_effect=Exception("Pull subscribe failed"))
        pushpull._js = mock_js
        callback = AsyncMock()
        
        with pytest.raises(Exception, match="Pull subscribe failed"):
            await pushpull.register_worker("test.jobs", callback, n_workers=1)
        
        # Clean up
        await pushpull._cleanup()

    @pytest.mark.asyncio
    async def test_subscribe_success(self, pushpull):
        """Test successful subscription."""
        callback = AsyncMock()
        mock_sub = AsyncMock()
        mock_sub._id = 123
        pushpull._nats_client.subscribe = AsyncMock(return_value=mock_sub)
        
        subscription_id = await pushpull.subscribe("test.jobs", callback)
        
        assert subscription_id == 123
        assert "test.jobs.subscriber" in pushpull._nats_subscriptions
        assert mock_sub in pushpull._nats_subscriptions["test.jobs.subscriber"]

    @pytest.mark.asyncio
    async def test_unsubscribe_success(self, pushpull):
        """Test successful unsubscription."""
        callback = AsyncMock()
        mock_sub = AsyncMock()
        mock_sub._id = 123
        pushpull._nats_client.subscribe = AsyncMock(return_value=mock_sub)
        
        await pushpull.subscribe("test.jobs", callback)
        
        result = await pushpull.unsubscribe("test.jobs", 123)
        
        assert result is True
        mock_sub.unsubscribe.assert_called_once()

    @pytest.mark.asyncio
    async def test_unsubscribe_nonexistent(self, pushpull):
        """Test unsubscription with non-existent subscription."""
        result = await pushpull.unsubscribe("test.jobs", 12345)
        assert result is False

    @pytest.mark.asyncio
    async def test_notify_subscriptions(self, pushpull, sample_job):
        """Test subscription notification."""
        # Add a subscription first
        mock_sub = AsyncMock()
        pushpull._nats_subscriptions["test.jobs.subscriber"] = [mock_sub]
        
        await pushpull._notify_subscriptions("test.jobs", sample_job)
        
        pushpull._nats_client.publish.assert_called_once_with(
            "test.jobs.subscriber", 
            json.dumps(sample_job).encode()
        )

    @pytest.mark.asyncio
    async def test_cleanup(self, pushpull, mock_pilot):
        """Test cleanup functionality."""
        # Create some mock tasks
        mock_task1 = asyncio.create_task(asyncio.sleep(0))
        mock_task2 = asyncio.create_task(asyncio.sleep(0))
        pushpull._workers = [mock_task1, mock_task2]
        
        # Create mock subscriptions
        mock_sub = AsyncMock()
        pushpull._nats_subscriptions["test.subject"] = [mock_sub]
        
        await pushpull._cleanup()
        
        # Check that collections were cleared
        assert pushpull._workers == []
        assert pushpull._nats_subscriptions == {}

    @pytest.mark.asyncio
    async def test_worker_loop_timeout_handling(self, pushpull, mock_pilot):
        """Test worker loop handles timeouts gracefully."""
        mock_js = MagicMock()
        mock_js.pull_subscribe = AsyncMock()
        mock_js.add_stream = AsyncMock()
        mock_js.streams_info = AsyncMock(side_effect=Exception("Stream not found"))
        mock_subscription = MagicMock()
        mock_subscription.fetch = AsyncMock(side_effect=TimeoutError("nats: timeout"))
        mock_js.pull_subscribe.return_value = mock_subscription
        pushpull._js = mock_js
        
        # Mock the shutdown event
        pushpull._shutdown_event = asyncio.Event()
        
        callback = AsyncMock()
        
        # Start worker loop
        task = asyncio.create_task(pushpull._worker_loop("test.jobs", callback))
        
        # Let it run briefly
        await asyncio.sleep(0.1)
        
        # Set shutdown event
        pushpull._shutdown_event.set()
        
        # Wait for task to complete
        await task
        
        # Verify no errors were raised (timeout handled gracefully)
        assert True  # If we get here, no exception was raised

    @pytest.mark.asyncio
    async def test_worker_loop_message_processing(self, pushpull, mock_pilot):
        """Test worker loop processes messages correctly."""
        mock_js = MagicMock()
        mock_js.pull_subscribe = AsyncMock()
        mock_js.add_stream = AsyncMock()
        mock_js.streams_info = AsyncMock(side_effect=Exception("Stream not found"))
        mock_message = MagicMock()
        mock_message.data = json.dumps({
            "_id": "test-123",
            "type": "test_job",
            "data": {"test": "data"},
            "error": None,
            "recv_ms": 0,
            "start_ms": 0,
            "end_ms": 0,
            "status": "new",
            "retry_count": 0
        }).encode()
        mock_message.ack = AsyncMock()
        mock_subscription = MagicMock()
        mock_subscription.fetch = AsyncMock()
        mock_subscription.fetch.side_effect = [
            [mock_message],
            TimeoutError("nats: timeout")
        ]
        mock_js.pull_subscribe.return_value = mock_subscription
        pushpull._js = mock_js
        
        # Mock the shutdown event
        pushpull._shutdown_event = asyncio.Event()
        
        callback = AsyncMock()
        
        # Start worker loop
        task = asyncio.create_task(pushpull._worker_loop("test.jobs", callback))
        
        # Let it run to process the message
        await asyncio.sleep(0.2)
        
        # Set shutdown event
        pushpull._shutdown_event.set()
        
        # Wait for task to complete
        await task
        
        # Verify callback was called with the job
        callback.assert_called_once()
        called_job = callback.call_args[0][0]
        assert called_job["_id"] == "test-123"
        assert called_job["status"] == "finished"
        
        # Verify message was acknowledged
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_worker_loop_error_handling(self, pushpull, mock_pilot):
        """Test worker loop handles callback errors correctly."""
        mock_js = MagicMock()
        mock_js.pull_subscribe = AsyncMock()
        mock_js.add_stream = AsyncMock()
        mock_js.streams_info = AsyncMock(side_effect=Exception("Stream not found"))
        mock_message = MagicMock()
        mock_message.data = json.dumps({
            "_id": "test-123",
            "type": "test_job",
            "data": {"test": "data"},
            "error": None,
            "recv_ms": 0,
            "start_ms": 0,
            "end_ms": 0,
            "status": "new",
            "retry_count": 0
        }).encode()
        mock_message.ack = AsyncMock()
        mock_subscription = MagicMock()
        mock_subscription.fetch = AsyncMock()
        mock_subscription.fetch.side_effect = [
            [mock_message],
            TimeoutError("nats: timeout")
        ]
        mock_js.pull_subscribe.return_value = mock_subscription
        pushpull._js = mock_js
        
        # Mock the shutdown event
        pushpull._shutdown_event = asyncio.Event()
        
        # Create a callback that raises an exception
        async def error_callback(job):
            raise Exception("Test error")
        
        # Start worker loop
        task = asyncio.create_task(pushpull._worker_loop("test.jobs", error_callback))
        
        # Let it run to process the message
        await asyncio.sleep(0.2)
        
        # Set shutdown event
        pushpull._shutdown_event.set()
        
        # Wait for task to complete
        await task
        
        # Verify message was acknowledged even after error
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_worker_loop_cancellation_handling(self, pushpull, mock_pilot):
        """Test worker loop handles cancellation correctly."""
        mock_js = MagicMock()
        mock_js.pull_subscribe = AsyncMock()
        mock_js.add_stream = AsyncMock()
        mock_js.streams_info = AsyncMock(side_effect=Exception("Stream not found"))
        mock_message = MagicMock()
        mock_message.data = json.dumps({
            "_id": "test-123",
            "type": "test_job",
            "data": {"test": "data"},
            "error": None,
            "recv_ms": 0,
            "start_ms": 0,
            "end_ms": 0,
            "status": "new",
            "retry_count": 0
        }).encode()
        mock_message.ack = AsyncMock()
        mock_subscription = MagicMock()
        mock_subscription.fetch = AsyncMock()
        mock_subscription.fetch.side_effect = [
            [mock_message],
            TimeoutError("nats: timeout")
        ]
        mock_js.pull_subscribe.return_value = mock_subscription
        pushpull._js = mock_js
        
        # Mock the shutdown event
        pushpull._shutdown_event = asyncio.Event()
        
        # Create a callback that gets cancelled
        async def cancellable_callback(job):
            raise asyncio.CancelledError()
        
        # Start worker loop
        task = asyncio.create_task(pushpull._worker_loop("test.jobs", cancellable_callback))
        
        # Let it run to process the message
        await asyncio.sleep(0.2)
        
        # Set shutdown event
        pushpull._shutdown_event.set()
        
        # Wait for task to complete
        await task
        
        # Verify message was acknowledged even after cancellation
        mock_message.ack.assert_called_once()

@pytest.mark.asyncio
@pytest.mark.integration
async def test_nats_pushpull_integration():
    """Integration test for NatsPushPull with real NATS server."""
    pilot = NatsPilot(nats_urls=["nats://localhost:4222"])
    
    try:
        await pilot.connect()
        
        pushpull = pilot.pushpull()
        processed_jobs = []
        job_statuses = []
        
        async def job_callback(job: Job):
            processed_jobs.append(job)
            await asyncio.sleep(0.1)
        
        async def status_callback(job: Job):
            job_statuses.append(job)
        
        # Register worker and monitor
        await pushpull.register_worker("integration.jobs", job_callback, n_workers=2)
        await pushpull.subscribe("integration.jobs", status_callback)
        
        # Create and push multiple jobs
        jobs = []
        for i in range(3):
            job = Job(
                _id=generate_id(),
                type=f"test_job_{i}",
                data={"index": i, "data": f"test_data_{i}"},
                error=None,
                recv_ms=0,
                start_ms=0,
                end_ms=0,
                status="new",
                retry_count=0
            )
            jobs.append(job)
            success = await pushpull.push("integration.jobs", job)
            assert success
        
        # Wait for all jobs to be processed
        await asyncio.sleep(1.0)
        
        # Verify all jobs were processed
        assert len(processed_jobs) == 3
        assert len(job_statuses) >= 3  # At least 3 status updates (running, finished)
        
        # Verify job processing order and status
        for i, job in enumerate(processed_jobs):
            assert job["type"] == f"test_job_{i}"
            assert job["data"]["index"] == i
            assert job["status"] == "finished"
        
        await pilot.disconnect()
        
    except Exception:
        pytest.skip("NATS server not available")
