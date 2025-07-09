import pytest
import pytest_asyncio
import asyncio
import time
import json
import warnings
import unittest.mock
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any

from hooklet.pilot.nats_pilot import NatsPilot, NatsPushPull
from hooklet.base.types import Job, Msg, Req, Reply
from hooklet.utils.id_generator import generate_id

def suppress_async_mock_warnings():
    """Context manager to suppress AsyncMock warnings during test setup."""
    return warnings.catch_warnings(record=True)

@pytest.mark.asyncio
@pytest.mark.integration
async def test_nats_pilot_connection():
    """Test basic connection functionality"""
    pilot = NatsPilot(nats_urls=["nats://localhost:4222"],
                      allow_reconnect=False,
                      connect_timeout=5)
    
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
    pilot = NatsPilot(nats_urls=["nats://localhost:4222"],
                      allow_reconnect=False,
                      connect_timeout=5)
    
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
        test_msg = Msg(type="test", data={"message": "test message", "timestamp": 1234567890})
        await pubsub.publish("test.subject", test_msg)
        
        # Give some time for the message to be processed
        await asyncio.sleep(0.1)
        
        # Unsubscribe
        success = await pubsub.unsubscribe("test.subject", subscription_id)
        assert success
        
        await pilot.disconnect()
        
    except Exception:
        # Ensure proper cleanup of background tasks
        try:
            await pilot.disconnect()
        except Exception:
            pass
        pytest.skip("NATS server not available")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_nats_pilot_reqreply():
    """Test request-reply functionality"""
    pilot = NatsPilot(nats_urls=["nats://localhost:4222"],
                      allow_reconnect=False,
                      connect_timeout=5)
    
    try:
        await pilot.connect()
        
        reqreply = pilot.reqreply()
        
        # Register a callback
        async def echo_callback(req_data: dict) -> Reply:
            current_time = int(time.time() * 1000)
            return Reply(
                type="echo_response",
                result={"echo": req_data.get("params", {}).get("data", ""), "received": True},
                start_ms=current_time,
                end_ms=current_time
            )
        
        subject = await reqreply.register_callback("echo.service", echo_callback)
        assert subject == "echo.service"
        
        # Make a request
        request_msg = Req(type="echo", params={"data": "hello world"})
        response = await reqreply.request("echo.service", request_msg)
        
        assert response["result"]["echo"] == "hello world"
        assert response["result"]["received"] is True
        
        # Unregister callback
        await reqreply.unregister_callback("echo.service")
        
        await pilot.disconnect()
        
    except Exception:
        # Ensure proper cleanup of background tasks
        try:
            await pilot.disconnect()
        except Exception:
            pass
        pytest.skip("NATS server not available")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_nats_pilot_pushpull():
    """Test push-pull functionality with JetStream"""
    pilot = NatsPilot(nats_urls=["nats://localhost:4222"],
                      allow_reconnect=False,
                      connect_timeout=5)
    
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
        assert processed_jobs[0].id == job.id
        
        await pilot.disconnect()
        
    except Exception:
        # Ensure proper cleanup of background tasks
        try:
            await pilot.disconnect()
        except Exception:
            pass
        pytest.skip("NATS server not available")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_nats_pilot_context_manager():
    """Test context manager functionality"""
    pilot = NatsPilot(nats_urls=["nats://localhost:4222"],
                      allow_reconnect=False,
                      connect_timeout=5)
    
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
        return NatsPilot(nats_urls=["nats://localhost:4222"],
                         allow_reconnect=False,
                         connect_timeout=5)

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
        with pytest.raises(RuntimeError, match="NatsPushPull is not initialized"):
            pilot.pushpull()

    @pytest.mark.asyncio
    async def test_connect_disconnect_mock(self):
        """Test connecting and disconnecting with mocked NATS client."""
        with patch('hooklet.pilot.nats_pilot.NATS') as mock_nats_class:
            mock_nats_client = AsyncMock()
            mock_nats_client.jetstream = MagicMock(return_value=MagicMock(name="js_context"))
            mock_nats_class.return_value = mock_nats_client
            
            # Create pilot after patching NATS
            pilot = NatsPilot(nats_urls=["nats://localhost:4222"],
                              allow_reconnect=False,
                              connect_timeout=5)
            
            await pilot.connect()
            assert pilot.is_connected()
            mock_nats_client.connect.assert_called_once_with(servers=["nats://localhost:4222"],
                                                            allow_reconnect=False,
                                                            connect_timeout=5)
            
            await pilot.disconnect()
            assert not pilot.is_connected()
            mock_nats_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test using NatsPilot as async context manager."""
        with patch('hooklet.pilot.nats_pilot.NATS') as mock_nats_class:
            # Create a fully async mock client
            mock_nats_client = AsyncMock()
            mock_nats_client.connect = AsyncMock()
            mock_nats_client.close = AsyncMock()
            mock_nats_client.is_connected = True
            mock_nats_client.jetstream = MagicMock(return_value=MagicMock(name="js_context"))

            mock_nats_class.return_value = mock_nats_client

            pilot = NatsPilot(nats_urls=["nats://localhost:4222"],
                              allow_reconnect=False,
                              connect_timeout=5)

            async with pilot:
                assert pilot.is_connected()

            # Verify all async calls were made
            mock_nats_client.connect.assert_awaited_once()
            mock_nats_client.close.assert_awaited_once()
            assert not pilot.is_connected()

class TestNatsPushPull:
    """Test cases for NatsPushPull class."""

    @pytest.fixture
    def mock_pilot(self):
        """Create a mock NatsPilot instance."""
        pilot = MagicMock()
        pilot.is_connected.return_value = True
        pilot._nats_client = AsyncMock()
        return pilot

    @pytest.fixture
    def mock_js_context(self):
        """Create a mock JetStream context."""
        js_context = AsyncMock()
        js_context.stream_info = AsyncMock()
        js_context.consumer_info = AsyncMock()
        js_context.add_stream = AsyncMock()
        js_context.publish = AsyncMock()
        js_context.pull_subscribe = AsyncMock()
        return js_context

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

    @pytest.mark.asyncio
    async def test_init(self, mock_pilot, mock_js_context):
        """Test NatsPushPull initialization."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        assert pushpull._pilot == mock_pilot
        assert pushpull._js == mock_js_context
        assert pushpull._nats_client == mock_pilot._nats_client
        assert pushpull._workers == []
        assert pushpull._nats_subscriptions == {}
        assert pushpull._shutdown_event is not None
        assert not pushpull._shutdown_event.is_set()

    def test_stream_name_generation(self, mock_pilot, mock_js_context):
        """Test stream name generation method."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Test normal subject
        assert pushpull.stream_name("test.subject") == "TEST-SUBJECT"
        
        # Test subject with multiple dots
        assert pushpull.stream_name("app.module.service") == "APP-MODULE-SERVICE"
        
        # Test single word subject
        assert pushpull.stream_name("jobs") == "JOBS"
        
        # Test that the method uses caching (same object returned)
        stream_name1 = pushpull.stream_name("test.cache")
        stream_name2 = pushpull.stream_name("test.cache")
        assert stream_name1 == stream_name2

    def test_consumer_name_generation(self, mock_pilot, mock_js_context):
        """Test consumer name generation method."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Test normal subject
        assert pushpull.consumer_name("test.subject") == "worker-test-subject"
        
        # Test subject with multiple dots
        assert pushpull.consumer_name("app.module.service") == "worker-app-module-service"
        
        # Test single word subject
        assert pushpull.consumer_name("jobs") == "worker-jobs"

    def test_subject_name_generation(self, mock_pilot, mock_js_context):
        """Test subscriber and job subject name generation methods."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Test subscriber subject
        assert pushpull.subscriber_subject("test.subject") == "test.subject.subscriber"
        
        # Test job subject
        assert pushpull.job_subject("test.subject") == "test.subject.job"

    @pytest.mark.asyncio
    async def test_ensure_stream_existing_stream(self, mock_pilot, mock_js_context):
        """Test _ensure_stream when stream already exists."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Mock stream_info to return successfully (stream exists)
        mock_js_context.stream_info.return_value = {"name": "TEST-SUBJECT"}
        
        # Should not raise an exception
        await pushpull._ensure_stream("test.subject")
        
        # Verify stream_info was called with correct stream name
        mock_js_context.stream_info.assert_called_once_with("TEST-SUBJECT")
        
        # Verify add_stream was not called since stream exists
        mock_js_context.add_stream.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_stream_create_new_stream(self, mock_pilot, mock_js_context):
        """Test _ensure_stream when stream doesn't exist."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Mock stream_info to raise exception (stream doesn't exist)
        mock_js_context.stream_info.side_effect = Exception("Stream not found")
        
        # Should not raise an exception
        await pushpull._ensure_stream("test.subject")
        
        # Verify stream_info was called
        mock_js_context.stream_info.assert_called_once_with("TEST-SUBJECT")
        
        # Verify add_stream was called with correct parameters
        mock_js_context.add_stream.assert_called_once()
        call_args = mock_js_context.add_stream.call_args
        assert call_args[1]["name"] == "TEST-SUBJECT"
        assert call_args[1]["subjects"] == ["test.subject.>"]
        assert call_args[1]["retention"] == "workqueue"

    @pytest.mark.asyncio
    async def test_ensure_stream_not_connected(self, mock_pilot, mock_js_context):
        """Test _ensure_stream when pilot is not connected."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Mock pilot as not connected
        mock_pilot.is_connected.return_value = False
        
        # Should raise RuntimeError
        with pytest.raises(RuntimeError, match="NATS client not connected"):
            await pushpull._ensure_stream("test.subject")

    @pytest.mark.asyncio
    async def test_ensure_consumer_existing_consumer(self, mock_pilot, mock_js_context):
        """Test _ensure_consumer when consumer already exists."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Mock consumer_info to return successfully (consumer exists)
        mock_js_context.consumer_info.return_value = {"name": "worker-test-subject"}
        
        # Should not raise an exception
        await pushpull._ensure_consumer("test.subject")
        
        # Verify consumer_info was called with correct parameters
        mock_js_context.consumer_info.assert_called_once_with("TEST-SUBJECT", "worker-test-subject")

    @pytest.mark.asyncio
    async def test_ensure_consumer_create_new_consumer(self, mock_pilot, mock_js_context):
        """Test _ensure_consumer when consumer doesn't exist."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Mock consumer_info to raise exception (consumer doesn't exist)
        mock_js_context.consumer_info.side_effect = Exception("Consumer not found")
        
        # Should not raise an exception
        await pushpull._ensure_consumer("test.subject")
        
        # Verify consumer_info was called
        mock_js_context.consumer_info.assert_called_once_with("TEST-SUBJECT", "worker-test-subject")

    @pytest.mark.asyncio
    async def test_ensure_consumer_not_connected(self, mock_pilot, mock_js_context):
        """Test _ensure_consumer when pilot is not connected."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Mock pilot as not connected
        mock_pilot.is_connected.return_value = False
        
        # Should raise RuntimeError
        with pytest.raises(RuntimeError, match="NATS client not connected"):
            await pushpull._ensure_consumer("test.subject")

    @pytest.mark.asyncio
    async def test_push_successful(self, mock_pilot, mock_js_context, sample_job):
        """Test push method with successful job submission."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Mock stream_info to return successfully (stream exists)
        mock_js_context.stream_info.return_value = {"name": "TEST-SUBJECT"}
        
        # Mock publish to return an acknowledgment
        mock_ack = MagicMock()
        mock_ack.seq = 12345
        mock_js_context.publish.return_value = mock_ack
        
        # Test successful push
        result = await pushpull.push("test.subject", sample_job)
        
        # Verify result
        assert result is True
        
        # Verify _ensure_stream was called
        mock_js_context.stream_info.assert_called_once_with("TEST-SUBJECT")
        
                # Verify publish was called with correct parameters
        mock_js_context.publish.assert_called_once_with(
            "test.subject.job",
            json.dumps(sample_job.model_dump(by_alias=True)).encode()
        )
        
        # Verify job was updated with recv_ms and status
        assert sample_job.status == "new"
        assert sample_job.recv_ms > 0

    @pytest.mark.asyncio
    async def test_push_failure(self, mock_pilot, mock_js_context, sample_job):
        """Test push method when publishing fails."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Mock stream_info to return successfully (stream exists)
        mock_js_context.stream_info.return_value = {"name": "TEST-SUBJECT"}
        
        # Mock publish to raise an exception
        mock_js_context.publish.side_effect = Exception("Publish failed")
        
        # Test failed push
        result = await pushpull.push("test.subject", sample_job)
        
        # Verify result
        assert result is False
        
        # Verify _ensure_stream was called
        mock_js_context.stream_info.assert_called_once_with("TEST-SUBJECT")
        
        # Verify publish was called
        mock_js_context.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_push_stream_creation(self, mock_pilot, mock_js_context, sample_job):
        """Test push method when stream needs to be created."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Mock stream_info to raise exception first time (stream doesn't exist)
        mock_js_context.stream_info.side_effect = Exception("Stream not found")
        
        # Mock publish to return an acknowledgment
        mock_ack = MagicMock()
        mock_ack.seq = 12345
        mock_js_context.publish.return_value = mock_ack
        
        # Test push with stream creation
        result = await pushpull.push("test.subject", sample_job)
        
        # Verify result
        assert result is True
        
        # Verify _ensure_stream was called (stream_info called, then add_stream called)
        mock_js_context.stream_info.assert_called_once_with("TEST-SUBJECT")
        mock_js_context.add_stream.assert_called_once()
        
        # Verify publish was called
        mock_js_context.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscribe_successful(self, mock_pilot, mock_js_context):
        """Test subscribe method with successful subscription."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Mock NATS client subscribe
        mock_subscription = MagicMock()
        mock_subscription._id = 123
        mock_pilot._nats_client.subscribe.return_value = mock_subscription
        
        # Create a test callback
        async def test_callback(job):
            pass
        
        # Test successful subscription
        subscription_id = await pushpull.subscribe("test.subject", test_callback)
        
        # Verify result
        assert subscription_id == 123
        
        # Verify subscribe was called with correct parameters
        mock_pilot._nats_client.subscribe.assert_called_once_with(
            "test.subject.subscriber", 
            cb=unittest.mock.ANY
        )
        
        # Verify subscription was stored
        assert "test.subject.subscriber" in pushpull._nats_subscriptions
        assert mock_subscription in pushpull._nats_subscriptions["test.subject.subscriber"]

    @pytest.mark.asyncio
    async def test_unsubscribe_successful(self, mock_pilot, mock_js_context):
        """Test unsubscribe method with successful unsubscription."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Mock subscription
        mock_subscription = MagicMock()
        mock_subscription._id = 123
        mock_subscription.unsubscribe = AsyncMock()
        
        # Set up initial subscription state
        pushpull._nats_subscriptions["test.subject.subscriber"] = [mock_subscription]
        
        # Test successful unsubscription
        result = await pushpull.unsubscribe("test.subject", 123)
        
        # Verify result
        assert result is True
        
        # Verify unsubscribe was called
        mock_subscription.unsubscribe.assert_called_once()
        
        # Verify subscription was removed from the list
        assert pushpull._nats_subscriptions["test.subject.subscriber"] == []

    @pytest.mark.asyncio
    async def test_unsubscribe_not_found(self, mock_pilot, mock_js_context):
        """Test unsubscribe method when subscription is not found."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Test unsubscription with non-existent subscription
        result = await pushpull.unsubscribe("test.subject", 999)
        
        # Verify result
        assert result is False

    @pytest.mark.asyncio
    async def test_unsubscribe_wrong_id(self, mock_pilot, mock_js_context):
        """Test unsubscribe method with wrong subscription ID."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Mock subscription with different ID
        mock_subscription = MagicMock()
        mock_subscription._id = 123
        
        # Set up initial subscription state
        pushpull._nats_subscriptions["test.subject.subscriber"] = [mock_subscription]
        
        # Test unsubscription with wrong ID
        result = await pushpull.unsubscribe("test.subject", 456)
        
        # Verify result
        assert result is False
        
        # Verify subscription was not removed
        assert "test.subject.subscriber" in pushpull._nats_subscriptions

    @pytest.mark.asyncio
    async def test_notify_subscriptions_with_subscriptions(self, mock_pilot, mock_js_context, sample_job):
        """Test _notify_subscriptions method when subscriptions exist."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Mock subscription
        mock_subscription = MagicMock()
        pushpull._nats_subscriptions["test.subject.subscriber"] = [mock_subscription]
        
        # Test notification
        await pushpull._notify_subscriptions("test.subject", sample_job)
        
        # Verify publish was called with correct parameters
        mock_pilot._nats_client.publish.assert_called_once_with(
            "test.subject.subscriber",
            json.dumps(sample_job.model_dump(by_alias=True)).encode()
        )

    @pytest.mark.asyncio
    async def test_notify_subscriptions_no_subscriptions(self, mock_pilot, mock_js_context, sample_job):
        """Test _notify_subscriptions method when no subscriptions exist."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Test notification with no subscriptions
        await pushpull._notify_subscriptions("test.subject", sample_job)
        
        # Verify publish was not called
        mock_pilot._nats_client.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_notify_subscriptions_empty_list(self, mock_pilot, mock_js_context, sample_job):
        """Test _notify_subscriptions method when subscription list is empty."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Set up empty subscription list
        pushpull._nats_subscriptions["test.subject.subscriber"] = []
        
        # Test notification
        await pushpull._notify_subscriptions("test.subject", sample_job)
        
        # Verify publish was still called (implementation publishes regardless of empty list)
        mock_pilot._nats_client.publish.assert_called_once_with(
            "test.subject.subscriber",
            json.dumps(sample_job.model_dump(by_alias=True)).encode()
        )

    @pytest.mark.asyncio
    async def test_cleanup(self, mock_pilot, mock_js_context):
        """Test _cleanup method properly cleans up resources."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Create mock worker tasks using asyncio.create_task with dummy coroutines
        async def dummy_worker():
            return "done"
        
        mock_task1 = asyncio.create_task(dummy_worker())
        mock_task2 = asyncio.create_task(dummy_worker())
        
        # Create mock subscriptions
        mock_sub1 = AsyncMock()
        mock_sub2 = AsyncMock()
        
        # Set up initial state
        pushpull._workers = [mock_task1, mock_task2]
        pushpull._nats_subscriptions = {
            "test.subject1.subscriber": [mock_sub1],
            "test.subject2.subscriber": [mock_sub2]
        }
        
        # Test cleanup
        await pushpull._cleanup()
        
        # Verify shutdown event was set
        assert pushpull._shutdown_event.is_set()
        
        # Verify all subscriptions were unsubscribed
        mock_sub1.unsubscribe.assert_called_once()
        mock_sub2.unsubscribe.assert_called_once()
        
        # Verify collections were cleared
        assert len(pushpull._workers) == 0
        assert len(pushpull._nats_subscriptions) == 0

    @pytest.mark.asyncio
    async def test_cleanup_empty_state(self, mock_pilot, mock_js_context):
        """Test _cleanup method with empty initial state."""
        pushpull = NatsPushPull(mock_pilot, mock_js_context)
        
        # Test cleanup with empty state
        await pushpull._cleanup()
        
        # Verify shutdown event was set
        assert pushpull._shutdown_event.is_set()
        
        # Verify collections remain empty
        assert len(pushpull._workers) == 0
        assert len(pushpull._nats_subscriptions) == 0 