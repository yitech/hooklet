#!/usr/bin/env python3
"""
Unit tests for the NatsManager class.
"""

import asyncio
import json
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from ems.nats_manager import NatsManager

pytestmark = pytest.mark.asyncio  # Mark all tests in this module as asyncio tests

class TestNatsManager:
    """Test cases for the NatsManager class."""
    
    @pytest_asyncio.fixture
    async def mock_nats(self):
        """Fixture that mocks the NATS client."""
        with patch('ems.nats_manager.NATS') as mock_nats_class:
            mock_nc = MagicMock()
            mock_nc.connect = AsyncMock()
            mock_nc.publish = AsyncMock()
            mock_nc.subscribe = AsyncMock()
            mock_nc.close = AsyncMock()
            
            # Set up the subscription mock
            mock_sub = MagicMock()
            mock_sub.unsubscribe = AsyncMock()
            mock_nc.subscribe.return_value = mock_sub
            
            # Return the mock client
            mock_nats_class.return_value = mock_nc
            yield mock_nc
    
    async def test_init(self, mock_nats):
        """Test NatsManager initialization."""
        nm = NatsManager(nats_url="nats://test-server:4222")
        assert nm.nats_url == "nats://test-server:4222"
        assert not nm._connected
        assert nm._subscriptions == {}
        assert nm._handlers == {}
    
    @pytest.mark.asyncio
    async def test_connect(self, mock_nats):
        """Test connecting to NATS."""
        nm = NatsManager()
        await nm.connect()
        
        mock_nats.connect.assert_called_once_with("nats://localhost:4222")
        assert nm._connected is True
    
    @pytest.mark.asyncio
    async def test_close(self, mock_nats):
        """Test closing NATS connection."""
        nm = NatsManager()
        await nm.connect()
        await nm.close()
        
        mock_nats.close.assert_called_once()
        assert nm._connected is False
        assert nm._subscriptions == {}
        assert nm._handlers == {}
    
    @pytest.mark.asyncio
    async def test_register_handler(self, mock_nats):
        """Test registering a handler function."""
        nm = NatsManager()
        await nm.connect()
        
        # Define a test handler
        async def test_handler(data):
            pass
        
        # Register the handler
        handler_id = await nm.register_handler("test.subject", test_handler)
        
        # Check that subscribe was called
        mock_nats.subscribe.assert_called_once()
        
        # Check that handler was registered
        assert "test.subject" in nm._subscriptions
        assert handler_id in nm._subscriptions["test.subject"]
        assert "test.subject" in nm._handlers
        assert handler_id in nm._handlers["test.subject"]
        assert nm._handlers["test.subject"][handler_id] == test_handler
    
    @pytest.mark.asyncio
    async def test_register_handler_with_custom_id(self, mock_nats):
        """Test registering a handler with a custom ID."""
        nm = NatsManager()
        await nm.connect()
        
        # Define a test handler
        async def test_handler(data):
            pass
        
        # Register the handler with a custom ID
        await nm.register_handler("test.subject", test_handler, handler_id="custom_id")
        
        # Check that handler was registered with the custom ID
        assert "test.subject" in nm._handlers
        assert "custom_id" in nm._handlers["test.subject"]
        assert nm._handlers["test.subject"]["custom_id"] == test_handler
    
    @pytest.mark.asyncio
    async def test_register_duplicate_handler_id(self, mock_nats):
        """Test registering a handler with a duplicate ID."""
        nm = NatsManager()
        await nm.connect()
        
        # Define test handlers
        async def test_handler1(data):
            pass
        
        async def test_handler2(data):
            pass
        
        # Register the first handler
        await nm.register_handler("test.subject", test_handler1, handler_id="same_id")
        
        # Try to register another handler with the same ID
        with pytest.raises(ValueError):
            await nm.register_handler("test.subject", test_handler2, handler_id="same_id")
    
    @pytest.mark.asyncio
    async def test_unregister_handler(self, mock_nats):
        """Test unregistering a handler."""
        nm = NatsManager()
        await nm.connect()
        
        # Define a test handler
        async def test_handler(data):
            pass
        
        # Register the handler
        await nm.register_handler("test.subject", test_handler, handler_id="test_id")
        
        # Unregister the handler
        success = await nm.unregister_handler("test.subject", "test_id")
        
        # Check results
        assert success is True
        mock_nats.subscribe.return_value.unsubscribe.assert_called_once()
        assert "test.subject" not in nm._subscriptions
        assert "test.subject" not in nm._handlers
    
    @pytest.mark.asyncio
    async def test_unregister_nonexistent_handler(self, mock_nats):
        """Test unregistering a non-existent handler."""
        nm = NatsManager()
        await nm.connect()
        
        # Try to unregister a handler that doesn't exist
        success = await nm.unregister_handler("test.subject", "nonexistent_id")
        
        # Check results
        assert success is False
    
    @pytest.mark.asyncio
    async def test_publish(self, mock_nats):
        """Test publishing a message."""
        nm = NatsManager()
        await nm.connect()
        
        # Test data
        test_data = {"key": "value"}
        
        # Publish the data
        await nm.publish("test.subject", test_data)
        
        # Check that publish was called with the correct arguments
        mock_nats.publish.assert_called_once_with("test.subject", json.dumps(test_data).encode())
    
    def test_get_registered_handlers(self, mock_nats):
        """Test getting registered handlers."""
        nm = NatsManager()
        nm._handlers = {
            "subject1": {"handler1": AsyncMock(), "handler2": AsyncMock()},
            "subject2": {"handler3": AsyncMock()}
        }
        
        handlers = nm.get_registered_handlers()
        
        assert handlers == {
            "subject1": ["handler1", "handler2"],
            "subject2": ["handler3"]
        }
