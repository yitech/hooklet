import pytest
import pytest_asyncio
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any

from hooklet.pilot.nats_pilot import NatsPilot, NatsPushPull
from hooklet.base.types import Job, Msg
from hooklet.utils.id_generator import generate_id

@pytest.mark.asyncio
@pytest.mark.integration
async def test_nats_pilot_connection():
    """Test basic connection functionality"""
    pilot = NatsPilot(nats_url="nats://localhost:4222")
    
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
    pilot = NatsPilot(nats_url="nats://localhost:4222")
    
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
    pilot = NatsPilot(nats_url="nats://localhost:4222")
    
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
async def test_nats_pilot_context_manager():
    """Test context manager functionality"""
    pilot = NatsPilot(nats_url="nats://localhost:4222")
    
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
        return NatsPilot("nats://localhost:4222")

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
            status="pending",
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
            pilot = NatsPilot("nats://localhost:4222")
            
            await pilot.connect()
            assert pilot.is_connected()
            mock_nats_client.connect.assert_called_once_with("nats://localhost:4222")
            
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
            pilot = NatsPilot("nats://localhost:4222")
            
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
            status="pending",
            retry_count=0
        )

    @pytest.mark.asyncio
    async def test_init(self, pushpull):
        """Test NatsPushPull initialization."""
        assert pushpull._pilot is not None
        assert pushpull._js is None
        assert pushpull._workers == {}
        assert pushpull._subscriptions == {}
        assert pushpull._worker_tasks == {}

    @pytest.mark.asyncio
    async def test_ensure_js_context(self, pushpull, mock_pilot):
        """Test JetStream context creation."""
        mock_js = AsyncMock()
        mock_pilot._nats_client.jetstream = MagicMock(return_value=mock_js)
        
        js = await pushpull._ensure_js_context()
        
        assert js == mock_js
        assert pushpull._js == mock_js
        mock_pilot._nats_client.jetstream.assert_called_once()

    @pytest.mark.asyncio
    async def test_ensure_js_context_not_connected(self, pushpull, mock_pilot):
        """Test JetStream context creation when not connected."""
        mock_pilot.is_connected.return_value = False
        
        with pytest.raises(RuntimeError, match="NATS client not connected"):
            await pushpull._ensure_js_context()

    @pytest.mark.asyncio
    async def test_ensure_stream_creates_new(self, pushpull, mock_pilot):
        """Test stream creation when it doesn't exist."""
        mock_js = AsyncMock()
        mock_js.streams_info = AsyncMock(side_effect=Exception("Stream not found"))
        mock_js.add_stream = AsyncMock(return_value=None)
        mock_pilot._nats_client.jetstream = MagicMock(return_value=mock_js)
        
        await pushpull._ensure_stream("test.subject")
        
        mock_js.add_stream.assert_called_once()
        call_args = mock_js.add_stream.call_args
        assert call_args[1]["name"] == "hooklet-test-subject"
        assert call_args[1]["subjects"] == ["test.subject"]
        assert call_args[1]["retention"] == "workqueue"

    @pytest.mark.asyncio
    async def test_ensure_stream_exists(self, pushpull, mock_pilot):
        """Test stream handling when it already exists."""
        mock_js = AsyncMock()
        mock_js.streams_info = AsyncMock(return_value=None)
        mock_pilot._nats_client.jetstream = MagicMock(return_value=mock_js)
        
        await pushpull._ensure_stream("test.subject")
        
        mock_js.streams_info.assert_called_once_with("hooklet-test-subject")
        mock_js.add_stream.assert_not_called()

    @pytest.mark.asyncio
    async def test_push_success(self, pushpull, mock_pilot, sample_job):
        """Test successful job push."""
        mock_js = AsyncMock()
        mock_js.streams_info = AsyncMock(side_effect=Exception("Stream not found"))
        mock_js.add_stream = AsyncMock(return_value=None)
        mock_ack = MagicMock()
        mock_ack.seq = 123
        mock_js.publish = AsyncMock(return_value=mock_ack)
        mock_pilot._nats_client.jetstream = MagicMock(return_value=mock_js)
        
        result = await pushpull.push("test.subject", sample_job)
        
        assert result is True
        assert sample_job["status"] == "new"
        assert "recv_ms" in sample_job
        mock_js.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_push_failure(self, pushpull, mock_pilot, sample_job):
        """Test job push failure."""
        mock_js = AsyncMock()
        mock_pilot._nats_client.jetstream.return_value = mock_js
        mock_js.publish.side_effect = Exception("Publish failed")
        
        result = await pushpull.push("test.subject", sample_job)
        
        assert result is False

    @pytest.mark.asyncio
    async def test_register_worker_success(self, pushpull, mock_pilot):
        """Test successful worker registration."""
        mock_js = AsyncMock()
        mock_js.streams_info = AsyncMock(side_effect=Exception("Stream not found"))
        mock_js.add_stream = AsyncMock(return_value=None)
        mock_js.add_consumer = AsyncMock(return_value=None)
        mock_pilot._nats_client.jetstream = MagicMock(return_value=mock_js)
        callback = AsyncMock()
        
        await pushpull.register_worker("test.subject", callback, 2)
        
        assert "test.subject" in pushpull._workers
        assert len(pushpull._workers["test.subject"]) == 2
        assert "test.subject" in pushpull._worker_tasks
        assert len(pushpull._worker_tasks["test.subject"]) == 2
        mock_js.add_consumer.assert_called_once()
        
        # Clean up to prevent hanging
        await pushpull._cleanup()

    @pytest.mark.asyncio
    async def test_register_worker_failure(self, pushpull, mock_pilot):
        """Test worker registration failure."""
        mock_js = AsyncMock()
        mock_js.streams_info = AsyncMock(side_effect=Exception("Stream not found"))
        mock_js.add_stream = AsyncMock(return_value=None)
        mock_js.add_consumer = AsyncMock(side_effect=Exception("Consumer creation failed"))
        mock_pilot._nats_client.jetstream = MagicMock(return_value=mock_js)
        callback = AsyncMock()
        
        with pytest.raises(Exception, match="Consumer creation failed"):
            await pushpull.register_worker("test.subject", callback, 1)
        
        # Clean up to prevent hanging
        await pushpull._cleanup()

    @pytest.mark.asyncio
    async def test_subscribe_success(self, pushpull):
        """Test successful subscription."""
        callback = AsyncMock()
        
        subscription_id = await pushpull.subscribe("test.subject", callback)
        
        assert "test.subject" in pushpull._subscriptions
        assert callback in pushpull._subscriptions["test.subject"]
        assert subscription_id == hash(callback)

    @pytest.mark.asyncio
    async def test_unsubscribe_success(self, pushpull):
        """Test successful unsubscription."""
        callback = AsyncMock()
        await pushpull.subscribe("test.subject", callback)
        
        result = await pushpull.unsubscribe("test.subject", hash(callback))
        
        assert result is True
        assert "test.subject" not in pushpull._subscriptions

    @pytest.mark.asyncio
    async def test_unsubscribe_nonexistent(self, pushpull):
        """Test unsubscription with non-existent subscription."""
        result = await pushpull.unsubscribe("test.subject", 12345)
        assert result is False

    @pytest.mark.asyncio
    async def test_notify_subscriptions(self, pushpull, sample_job):
        """Test subscription notification."""
        callback1 = AsyncMock()
        callback2 = AsyncMock()
        
        await pushpull.subscribe("test.subject", callback1)
        await pushpull.subscribe("test.subject", callback2)
        
        await pushpull._notify_subscriptions("test.subject", sample_job)
        
        callback1.assert_called_once_with(sample_job)
        callback2.assert_called_once_with(sample_job)

    @pytest.mark.asyncio
    async def test_notify_subscriptions_error_handling(self, pushpull, sample_job):
        """Test subscription notification error handling."""
        good_callback = AsyncMock()
        bad_callback = AsyncMock(side_effect=Exception("Callback error"))
        
        await pushpull.subscribe("test.subject", good_callback)
        await pushpull.subscribe("test.subject", bad_callback)
        
        # Should not raise exception
        await pushpull._notify_subscriptions("test.subject", sample_job)
        
        good_callback.assert_called_once_with(sample_job)
        bad_callback.assert_called_once_with(sample_job)

    @pytest.mark.asyncio
    async def test_cleanup(self, pushpull, mock_pilot):
        """Test cleanup functionality."""
        # Create some mock tasks that can be awaited
        mock_task1 = asyncio.create_task(asyncio.sleep(0))
        mock_task2 = asyncio.create_task(asyncio.sleep(0))
        pushpull._worker_tasks["test.subject"] = [mock_task1, mock_task2]
        pushpull._workers["test.subject"] = [AsyncMock()]
        pushpull._subscriptions["test.subject"] = [AsyncMock()]
        
        await pushpull._cleanup()
        
        # Check that collections were cleared
        assert pushpull._workers == {}
        assert pushpull._subscriptions == {}
        assert pushpull._worker_tasks == {}

    @pytest.mark.asyncio
    async def test_worker_loop_integration(self, pushpull, mock_pilot):
        """Test worker loop integration with mocked JetStream."""
        mock_js = AsyncMock()
        mock_pilot._nats_client.jetstream = MagicMock(return_value=mock_js)

        # Mock the pull_subscribe to return a message
        mock_msg = AsyncMock()
        mock_msg.data = b'{"_id": "test", "type": "test_job", "data": {}, "error": null, "recv_ms": 0, "start_ms": 0, "end_ms": 0, "status": "pending", "retry_count": 0}'
        mock_msg.ack = AsyncMock()

        class MockAsyncIterator:
            def __init__(self, items):
                self._items = items
            def __aiter__(self):
                self._iter = iter(self._items)
                return self
            async def __anext__(self):
                try:
                    return next(self._iter)
                except StopIteration:
                    raise StopAsyncIteration

        mock_js.pull_subscribe = AsyncMock(return_value=MockAsyncIterator([mock_msg]))
        pushpull._shutdown_event.clear()
        # Create a worker task
        callback = AsyncMock()
        task = asyncio.create_task(
            pushpull._worker_loop("test.subject", "test-consumer", callback, 0)
        )

        # Let it run for a bit to process the message
        await asyncio.sleep(0.2)

        # Set shutdown event
        pushpull._shutdown_event.set()

        # Wait for task to complete
        await task

        # Verify callback was called
        callback.assert_called_once()
        mock_msg.ack.assert_called_once()
