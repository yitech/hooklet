import asyncio
import pytest
import pytest_asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

from hooklet.eventrix.collection.emitter.example import ExampleEmitter
from hooklet.pilot.inproc_pilot import InProcPilot


@pytest_asyncio.fixture
async def inproc_pilot():
    pilot = InProcPilot()
    await pilot.connect()
    yield pilot
    await pilot.close()


@pytest_asyncio.fixture
async def example_emitter(inproc_pilot):
    emitter = ExampleEmitter(pilot=inproc_pilot)
    yield emitter
    # Ensure cleanup
    if emitter.is_running():
        emitter._shutdown_event.set()
        await asyncio.sleep(0.1)


class TestExampleEmitter:
    
    @pytest.mark.asyncio
    async def test_init(self, example_emitter):
        """Test that the emitter initializes correctly."""
        assert example_emitter.pilot is not None
        assert not example_emitter._shutdown_event.is_set()
        assert len(example_emitter._generator_tasks) == 0
    
    @pytest.mark.asyncio
    async def test_get_generators(self, example_emitter):
        """Test that get_generators returns the expected generators."""
        generators = await example_emitter.get_generators()
        
        # Check that it returns the expected structure
        assert isinstance(generators, dict)
        assert "example" in generators
        assert callable(generators["example"])
    
    @pytest.mark.asyncio
    async def test_generator_yields_events(self, example_emitter):
        """Test that the generator yields events with the expected structure."""
        generators = await example_emitter.get_generators()
        generator_func = generators["example"]
        
        # Get the first event from the generator
        generator = generator_func()
        event = await anext(generator)  # Use anext for async iterator
        
        # Check the event structure
        assert isinstance(event, dict)
        assert "id" in event
        
        # UUID validation - should not raise an exception if valid
        uuid.UUID(event["id"])
        
        # Clean up
        await generator.aclose()  # Close the async generator
    
    @pytest.mark.asyncio
    async def test_emitter_execution(self, example_emitter, inproc_pilot):
        """Test that the emitter properly publishes events."""
        # Register a handler to receive events
        received_events = []
        
        async def test_handler(data):
            received_events.append(data)
        
        await inproc_pilot.register_handler("example", test_handler)
        
        # Start the emitter in a task
        async def run_emitter():
            await example_emitter.on_start()
            await example_emitter._register_generators()
            await asyncio.sleep(2.5)  # Allow time for at least 2 events
            await example_emitter._unregister_generators()
            await example_emitter.on_finish()
        
        await asyncio.create_task(run_emitter())
        
        # We should have at least 2 events
        assert len(received_events) >= 2
        
        # Each event should have an ID
        for event in received_events:
            assert "id" in event
            uuid.UUID(event["id"])  # Should be a valid UUID
    
    @pytest.mark.asyncio
    async def test_emitter_shutdown(self, example_emitter):
        """Test that the emitter properly shuts down."""
        # Start the emitter
        await example_emitter.on_start()
        await example_emitter._register_generators()
        
        # Verify it's running
        assert example_emitter.is_running()
        assert len(example_emitter._generator_tasks) > 0
        
        # Trigger shutdown
        example_emitter._shutdown_event.set()
        await example_emitter._unregister_generators()
        await example_emitter.on_finish()
        
        # Verify it's stopped
        assert not example_emitter.is_running()
        assert all(task.done() for task in example_emitter._generator_tasks)
    
    @pytest.mark.asyncio
    async def test_on_execute_with_shutdown(self, example_emitter):
        """Test the on_execute method with shutdown."""
        # Set up a task to shut down the emitter after 1 second
        async def shutdown_after_delay():
            await asyncio.sleep(1)
            example_emitter._shutdown_event.set()
        
        # Run both tasks
        shutdown_task = asyncio.create_task(shutdown_after_delay())
        
        # on_execute should return when the shutdown event is set
        await example_emitter.on_execute()
        
        # Wait for the shutdown task to complete
        await asyncio.gather(shutdown_task)
        
        # Verify it's stopped
        assert not example_emitter.is_running()
    
    @pytest.mark.asyncio
    async def test_emitter_status(self, example_emitter):
        """Test that the emitter status is correctly reported."""
        # Check initial status
        initial_status = example_emitter.status
        assert initial_status["type"] == "ExampleEmitter"
        assert initial_status["status"] == "running"  # Default is running until shutdown_event is set
        
        # Start the emitter
        await example_emitter.on_start()
        await example_emitter._register_generators()
        
        # Check status after starting
        running_status = example_emitter.status
        assert running_status["type"] == "ExampleEmitter"
        assert running_status["status"] == "running"
        assert len(running_status["generator_tasks"]) > 0
        
        # Shut down the emitter
        example_emitter._shutdown_event.set()
        await example_emitter._unregister_generators()
        
        # Check status after shutdown
        stopped_status = example_emitter.status
        assert stopped_status["type"] == "ExampleEmitter"
        assert stopped_status["status"] == "stopped"