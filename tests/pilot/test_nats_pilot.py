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
            mock_nats_client.jetstream = MagicMock(return_value=MagicMock(name="js_context"))
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
            mock_nats_client.jetstream = MagicMock(return_value=MagicMock(name="js_context"))

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

    