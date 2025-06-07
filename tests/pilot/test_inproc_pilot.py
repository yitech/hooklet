import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from hooklet.pilot.inproc_pilot import InProcPilot


@pytest.fixture
def inproc_pilot():
    pilot = InProcPilot()
    return pilot


@pytest_asyncio.fixture
async def connected_pilot(inproc_pilot):
    await inproc_pilot.connect()
    yield inproc_pilot
    await inproc_pilot.close()


class TestInProcPilot:
    
    @pytest.mark.asyncio
    async def test_init(self, inproc_pilot):
        """Test that the pilot initializes correctly."""
        assert not inproc_pilot.is_connected()
        assert inproc_pilot._handlers == {}
        assert inproc_pilot._consumer_task is None
        
    @pytest.mark.asyncio
    async def test_connect(self, inproc_pilot):
        """Test connecting to the pilot."""
        await inproc_pilot.connect()
        assert inproc_pilot.is_connected()
        assert inproc_pilot._consumer_task is not None
        # Clean up
        await inproc_pilot.close()
        
    @pytest.mark.asyncio
    async def test_connect_idempotent(self, connected_pilot):
        """Test that connecting multiple times doesn't cause issues."""
        # Should already be connected from fixture
        assert connected_pilot.is_connected()
        # Connect again
        await connected_pilot.connect()
        # Should still be connected with no errors
        assert connected_pilot.is_connected()
        
    @pytest.mark.asyncio
    async def test_close(self, connected_pilot):
        """Test closing the connection."""
        assert connected_pilot.is_connected()
        await connected_pilot.close()
        assert not connected_pilot.is_connected()
        assert connected_pilot._consumer_task is None
        assert connected_pilot._handlers == {}
        
    @pytest.mark.asyncio
    async def test_close_idempotent(self, inproc_pilot):
        """Test that closing when not connected doesn't cause issues."""
        assert not inproc_pilot.is_connected()
        await inproc_pilot.close()
        assert not inproc_pilot.is_connected()
        
    @pytest.mark.asyncio
    async def test_register_handler(self, connected_pilot):
        """Test registering a handler."""
        handler = AsyncMock()
        handler_id = await connected_pilot.register_handler("test_subject", handler)
        
        # Check the handler was registered
        assert "test_subject" in connected_pilot._handlers
        assert handler_id in connected_pilot._handlers["test_subject"]
        assert connected_pilot._handlers["test_subject"][handler_id] == handler
        
    @pytest.mark.asyncio
    async def test_register_handler_with_custom_id(self, connected_pilot):
        """Test registering a handler with a custom ID."""
        handler = AsyncMock()
        custom_id = "my_custom_handler_id"
        returned_id = await connected_pilot.register_handler("test_subject", handler, custom_id)
        
        # Check the handler was registered with the custom ID
        assert returned_id == custom_id
        assert "test_subject" in connected_pilot._handlers
        assert custom_id in connected_pilot._handlers["test_subject"]
        
    @pytest.mark.asyncio
    async def test_register_duplicate_handler_id(self, connected_pilot):
        """Test that registering a handler with an existing ID raises an error."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        custom_id = "duplicate_id"
        
        await connected_pilot.register_handler("test_subject1", handler1, custom_id)
        
        with pytest.raises(ValueError, match=f"Handler ID {custom_id} already exists"):
            await connected_pilot.register_handler("test_subject2", handler2, custom_id)
            
    @pytest.mark.asyncio
    async def test_unregister_handler(self, connected_pilot):
        """Test unregistering a handler."""
        handler = AsyncMock()
        custom_id = "handler_to_remove"
        await connected_pilot.register_handler("test_subject", handler, custom_id)
        
        # Verify it's registered
        assert custom_id in connected_pilot._handlers["test_subject"]
        
        # Unregister and verify it's removed
        await connected_pilot.unregister_handler(custom_id)
        assert "test_subject" not in connected_pilot._handlers or custom_id not in connected_pilot._handlers["test_subject"]
        
    @pytest.mark.asyncio
    async def test_unregister_nonexistent_handler(self, connected_pilot):
        """Test unregistering a handler that doesn't exist doesn't cause issues."""
        with patch("hooklet.pilot.inproc_pilot.logger.warning") as mock_warning:
            await connected_pilot.unregister_handler("nonexistent_id")
            mock_warning.assert_called_once()
            
    @pytest.mark.asyncio
    async def test_publish_and_handle(self, connected_pilot):
        """Test publishing a message and handling it."""
        test_data = {"key": "value"}
        handler = AsyncMock()
        
        await connected_pilot.register_handler("test_subject", handler)
        await connected_pilot._publish("test_subject", test_data)
        
        # Wait a bit for the message to be processed
        await asyncio.sleep(0.1)
        
        # Check if handler was called with the correct data
        handler.assert_called_once_with(test_data)
        
    @pytest.mark.asyncio
    async def test_publish_to_multiple_handlers(self, connected_pilot):
        """Test publishing a message to multiple handlers."""
        test_data = {"key": "value"}
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        
        await connected_pilot.register_handler("test_subject", handler1, "handler1")
        await connected_pilot.register_handler("test_subject", handler2, "handler2")
        await connected_pilot._publish("test_subject", test_data)
        
        # Wait a bit for the message to be processed
        await asyncio.sleep(0.1)
        
        # Check if both handlers were called with the correct data
        handler1.assert_called_once_with(test_data)
        handler2.assert_called_once_with(test_data)
        
    @pytest.mark.asyncio
    async def test_publish_no_matching_handlers(self, connected_pilot):
        """Test publishing to a subject with no handlers."""
        test_data = {"key": "value"}
        handler = AsyncMock()
        
        # Register handler for a different subject
        await connected_pilot.register_handler("other_subject", handler)
        
        # Publish to a subject with no handlers
        await connected_pilot._publish("test_subject", test_data)
        
        # Wait a bit, but the handler should not be called
        await asyncio.sleep(0.1)
        handler.assert_not_called()
        
    @pytest.mark.asyncio
    async def test_handler_exception(self, connected_pilot):
        """Test that exceptions in handlers are caught and don't affect other handlers."""
        test_data = {"key": "value"}
        error_handler = AsyncMock(side_effect=Exception("Test exception"))
        normal_handler = AsyncMock()
        
        await connected_pilot.register_handler("test_subject", error_handler, "error_handler")
        await connected_pilot.register_handler("test_subject", normal_handler, "normal_handler")
        
        with patch("hooklet.pilot.inproc_pilot.logger.error") as mock_error:
            await connected_pilot._publish("test_subject", test_data)
            # Wait for processing
            await asyncio.sleep(0.1)
            
            # Error should be logged
            mock_error.assert_called_once()
            # But normal handler should still be called
            normal_handler.assert_called_once_with(test_data)
            
    @pytest.mark.asyncio
    async def test_publish_auto_connect(self, inproc_pilot):
        """Test that publishing automatically connects if not already connected."""
        test_data = {"key": "value"}
        handler = AsyncMock()
        
        assert not inproc_pilot.is_connected()
        
        await inproc_pilot.register_handler("test_subject", handler)
        await inproc_pilot._publish("test_subject", test_data)
        
        assert inproc_pilot.is_connected()
        
        # Wait a bit for the message to be processed
        await asyncio.sleep(0.1)
        
        # Check if handler was called
        handler.assert_called_once_with(test_data)
        
        # Clean up
        await inproc_pilot.close()