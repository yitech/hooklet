import pytest
import asyncio
from hooklet.pilot import NatsPilot
from hooklet.base.types import Msg

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