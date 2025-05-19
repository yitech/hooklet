import asyncio
import pytest
import pytest_asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

from hooklet.eventrix.collection.handler.example import ExampleHandler
from hooklet.pilot.inproc_pilot import InProcPilot


@pytest_asyncio.fixture
async def inproc_pilot():
    pilot = InProcPilot()
    await pilot.connect()
    yield pilot
    await pilot.close()


@pytest_asyncio.fixture
async def example_handler(inproc_pilot):
    handler = ExampleHandler(pilot=inproc_pilot)
    yield handler
    # Ensure cleanup
    if handler.is_running():
        handler._shutdown_event.set()
        await asyncio.sleep(0.1)


class TestExampleHandler:
    
    @pytest.mark.asyncio
    async def test_init(self, example_handler):
        """Test that the handler initializes correctly."""
        assert example_handler.pilot is not None
        assert not example_handler._shutdown_event.is_set()
        assert example_handler._registered_handlers == {}
    
    @pytest.mark.asyncio
    async def test_get_handlers(self, example_handler):
        """Test that get_handlers returns the expected handlers."""
        handlers = example_handler.get_handlers()
        
        # Check that it returns the expected structure
        assert isinstance(handlers, dict)
        assert "example" in handlers
        assert callable(handlers["example"])
    
    @pytest.mark.asyncio
    async def test_handler_processes_events(self, example_handler):
        """Test that the handler correctly processes events."""
        handlers = example_handler.get_handlers()
        example_handler_func = handlers["example"]
        
        # Create a test event
        test_event = {"id": "test-id-123"}
        
        # Patch the logger to verify the log message
        with patch.object(logging.getLogger("hooklet.eventrix.collection.handler.example"), "info") as mock_log:
            # Call the handler
            await example_handler_func(test_event)
            
            # Verify the log message
            mock_log.assert_called_once_with("Received event with id: test-id-123")
    
    @pytest.mark.asyncio
    async def test_handler_registration(self, example_handler, inproc_pilot):
        """Test that the handler properly registers with the pilot."""
        # Patch the pilot's register_handler method to verify calls
        with patch.object(inproc_pilot, 'register_handler', new_callable=AsyncMock) as mock_register:
            # Start the handler
            await example_handler.on_start()
            
            # Verify register_handler was called with expected arguments
            mock_register.assert_called_once()
            subject, handler_func, handler_id = mock_register.call_args[0]
            
            assert subject == "example"
            assert callable(handler_func)
            assert handler_id.startswith(f"{example_handler.executor_id}_example")
            
            # Cleanup
            await example_handler.on_finish()
    
    @pytest.mark.asyncio
    async def test_handler_unregistration(self, example_handler, inproc_pilot):
        """Test that the handler properly unregisters from the pilot."""
        # Start the handler first to register handlers
        await example_handler.on_start()
        
        # Patch the pilot's unregister_handler method to verify calls
        with patch.object(inproc_pilot, 'unregister_handler', new_callable=AsyncMock, return_value=True) as mock_unregister:
            # Finish the handler
            await example_handler.on_finish()
            
            # Verify unregister_handler was called with expected arguments
            mock_unregister.assert_called_once()
            subject, handler_id = mock_unregister.call_args[0]
            
            assert subject == "example"
            assert handler_id.startswith(f"{example_handler.executor_id}_example")
            
            # Verify that registered handlers were cleared
            assert example_handler._registered_handlers == {}
    
    @pytest.mark.asyncio
    async def test_is_running(self, example_handler):
        """Test that is_running correctly reflects the handler state."""
        assert example_handler.is_running()  # Default is running until shutdown_event is set
        
        example_handler._shutdown_event.set()
        assert not example_handler.is_running()
    
    @pytest.mark.asyncio
    async def test_on_execute_with_shutdown(self, example_handler):
        """Test the on_execute method with shutdown."""
        # Set up a task to shut down the handler after 1 second
        async def shutdown_after_delay():
            await asyncio.sleep(1)
            example_handler._shutdown_event.set()
        
        # Run both tasks
        shutdown_task = asyncio.create_task(shutdown_after_delay())
        
        # on_execute should return when the shutdown event is set
        await example_handler.on_execute()
        
        # Wait for the shutdown task to complete
        await asyncio.gather(shutdown_task)
        
        # Verify it's stopped
        assert not example_handler.is_running()
    
    @pytest.mark.asyncio
    async def test_handler_status(self, example_handler):
        """Test that the handler status is correctly reported."""
        # Check initial status
        initial_status = example_handler.status
        assert initial_status["type"] == "ExampleHandler"
        assert initial_status["status"] == "running"  # Default is running until shutdown_event is set
        assert initial_status["registered_handlers"] == {}
        
        # Start the handler
        await example_handler.on_start()
        
        # Check status after starting
        running_status = example_handler.status
        assert running_status["type"] == "ExampleHandler"
        assert running_status["status"] == "running"
        assert "example" in running_status["registered_handlers"]
        
        # Shut down the handler
        example_handler._shutdown_event.set()
        await example_handler.on_finish()
        
        # Check status after shutdown
        stopped_status = example_handler.status
        assert stopped_status["type"] == "ExampleHandler"
        assert stopped_status["status"] == "stopped"
        assert stopped_status["registered_handlers"] == {}