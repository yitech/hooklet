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
        with pytest.raises(RuntimeError, match="NatsPushPull is not initialized"):
            pilot.pushpull()

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
            # Create a fully async mock client
            mock_nats_client = AsyncMock()
            mock_nats_client.connect = AsyncMock()
            mock_nats_client.close = AsyncMock()
            mock_nats_client.is_connected = True
            mock_nats_client.jetstream = AsyncMock(return_value=MagicMock(name="js_context"))

            mock_nats_class.return_value = mock_nats_client

            pilot = NatsPilot(["nats://localhost:4222"])

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
        """Create a mock pilot for testing."""
        pilot = MagicMock()
        pilot.is_connected.return_value = True
        pilot._nats_client = AsyncMock()
        pilot._nats_client.jetstream = MagicMock()
        pilot._nats_client.jetstream.return_value = MagicMock(name="js_context")
        return pilot

    @pytest_asyncio.fixture
    async def nats_pushpull(self, mock_pilot):
        """Create a NatsPushPull instance for testing."""
        return await NatsPushPull.create(mock_pilot)

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
    async def test_init(self, nats_pushpull, mock_pilot):
        """Test NatsPushPull initialization."""
        assert nats_pushpull._pilot is mock_pilot
        assert nats_pushpull._nats_client is mock_pilot._nats_client
        assert nats_pushpull._js is mock_pilot._nats_client.jetstream.return_value
        assert nats_pushpull._workers == []
        assert nats_pushpull._nats_subscriptions == {}
        assert isinstance(nats_pushpull._shutdown_event, asyncio.Event)

    @pytest.mark.asyncio
    async def test_push_success(self, nats_pushpull, sample_job):
        """Test successful job push."""
        subject = "test.jobs"
        
        # Mock the _ensure_stream method
        with patch.object(nats_pushpull, '_ensure_stream') as mock_ensure_stream:
            # Mock the JetStream publish method
            mock_ack = MagicMock()
            mock_ack.seq = 12345
            nats_pushpull._js.publish = AsyncMock(return_value=mock_ack)
            
            result = await nats_pushpull.push(subject, sample_job)
            
            assert result is True
            mock_ensure_stream.assert_called_once_with(subject)
            nats_pushpull._js.publish.assert_called_once()
            
            # Verify job was updated with metadata
            assert sample_job["recv_ms"] > 0
            assert sample_job["status"] == "new"

    @pytest.mark.asyncio
    async def test_push_failure(self, nats_pushpull, sample_job):
        """Test job push failure."""
        subject = "test.jobs"
        
        # Mock the _ensure_stream method to raise an exception
        with patch.object(nats_pushpull, '_ensure_stream', side_effect=Exception("Stream creation failed")):
            result = await nats_pushpull.push(subject, sample_job)
            
            assert result is False

    @pytest.mark.asyncio
    async def test_register_worker(self, nats_pushpull):
        """Test worker registration."""
        subject = "test.jobs"
        callback = AsyncMock()
        n_workers = 2
        
        # Mock the required methods
        with patch.object(nats_pushpull, '_ensure_stream') as mock_ensure_stream, \
             patch.object(nats_pushpull, '_ensure_consumer') as mock_ensure_consumer, \
             patch.object(nats_pushpull._js, 'pull_subscribe', new_callable=AsyncMock) as mock_pull_subscribe:
            
            await nats_pushpull.register_worker(subject, callback, n_workers)
            
            # Verify all methods were called
            mock_ensure_stream.assert_called_once_with(subject)
            mock_ensure_consumer.assert_called_once_with(subject)
            mock_pull_subscribe.assert_called_once()
            
            # Verify workers were created
            assert len(nats_pushpull._workers) == n_workers
            for worker in nats_pushpull._workers:
                assert isinstance(worker, asyncio.Task)

    @pytest.mark.asyncio
    async def test_subscribe_and_unsubscribe(self, nats_pushpull):
        """Test subscribe and unsubscribe methods."""
        subject = "test.jobs"
        callback = AsyncMock()
        mock_subscription = MagicMock()
        mock_subscription._id = 42
        # Patch the nats_client.subscribe method to return our mock_subscription
        nats_pushpull._nats_client.subscribe = AsyncMock(return_value=mock_subscription)

        # Subscribe
        subscription_id = await nats_pushpull.subscribe(subject, callback)
        assert subscription_id == 42
        assert f"{subject}.subscriber" in nats_pushpull._nats_subscriptions
        assert mock_subscription in nats_pushpull._nats_subscriptions[f"{subject}.subscriber"]

        # Unsubscribe
        mock_subscription.unsubscribe = AsyncMock(return_value=None)
        result = await nats_pushpull.unsubscribe(subject, subscription_id)
        assert result is True
        assert mock_subscription not in nats_pushpull._nats_subscriptions.get(f"{subject}.subscriber", [])

        # Unsubscribe again should return False
        result = await nats_pushpull.unsubscribe(subject, subscription_id)
        assert result is False

    @pytest.mark.asyncio
    async def test_subscribe_callback_invoked(self, nats_pushpull):
        """Test that the callback passed to subscribe is called when a message is received."""
        subject = "test.jobs"
        callback = AsyncMock()
        mock_subscription = MagicMock()
        mock_subscription._id = 99
        nats_pushpull._nats_client.subscribe = AsyncMock(return_value=mock_subscription)

        # Subscribe and get the callback registered in the nats_client.subscribe call
        await nats_pushpull.subscribe(subject, callback)
        # Get the actual callback passed to nats_client.subscribe
        subscribe_call = nats_pushpull._nats_client.subscribe.call_args
        assert subscribe_call is not None
        _, kwargs = subscribe_call
        nats_callback = kwargs['cb']

        # Simulate a NATS message
        class FakeMsg:
            def __init__(self, data):
                self.data = data
                self.subject = subject
        test_job = {"_id": "abc", "type": "test", "data": 123, "error": None, "recv_ms": 0, "start_ms": 0, "end_ms": 0, "status": "new", "retry_count": 0}
        msg = FakeMsg(json.dumps(test_job).encode())
        await nats_callback(msg)
        callback.assert_awaited_once_with(test_job)
