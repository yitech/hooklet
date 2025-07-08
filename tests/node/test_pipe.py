import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch
from typing import AsyncGenerator

from hooklet.node.pipe import Pipe
from hooklet.pilot.inproc_pilot import InprocPilot
from hooklet.base.types import Msg
from hooklet.utils.id_generator import generate_id


class TestPipe:
    """Test cases for Pipe class."""
    
    class MockPipe(Pipe):
        """Concrete implementation of Pipe for testing."""
        
        def __init__(self, name: str, subscribes: list[str], pubsub, router):
            super().__init__(name, subscribes, pubsub, router)
            self.processed_messages = []
            self.process_count = 0
        
        async def on_message(self, msg: Msg) -> AsyncGenerator[Msg, None]:
            """Mock on_message implementation that transforms the input message."""
            self.processed_messages.append(msg)
            self.process_count += 1
            
            # Transform the message
            transformed_msg: Msg = {
                "_id": generate_id(),
                "type": f"transformed_{msg.get('type', 'unknown')}",
                "data": f"processed_{msg.get('data', 'no_data')}",
                "error": msg.get('error'),
                "original_id": msg.get('_id')
            }
            yield transformed_msg
    
    class MockRouter:
        """Mock router for testing."""
        
        def __init__(self, subject_prefix: str = "test.output"):
            self.subject_prefix = subject_prefix
            self.routed_messages = []
        
        def __call__(self, msg: Msg) -> str:
            """Route message to a subject based on message type."""
            subject = f"{self.subject_prefix}.{msg.get('type', 'unknown')}"
            self.routed_messages.append((subject, msg))
            return subject
    
    @pytest.fixture
    def pilot(self):
        """Create an InprocPilot instance for testing."""
        return InprocPilot()
    
    @pytest.fixture
    def pubsub(self, pilot):
        """Get the pubsub interface from the pilot."""
        return pilot.pubsub()
    
    @pytest.fixture
    def router(self):
        """Create a mock router for testing."""
        return self.MockRouter()
    
    @pytest.fixture
    def subscribes(self):
        """List of subjects to subscribe to."""
        return ["test.input.topic1", "test.input.topic2"]
    
    @pytest.fixture
    def pipe(self, pubsub, router, subscribes):
        """Create a test Pipe instance."""
        return self.MockPipe("test-pipe", subscribes, pubsub, router)
    
    @pytest.mark.asyncio
    async def test_pipe_initialization(self, pipe, pubsub, router, subscribes):
        """Test Pipe initialization."""
        assert pipe.name == "test-pipe"
        assert pipe.subscribes == subscribes
        assert pipe.pubsub == pubsub
        assert pipe.router == router
        assert pipe.subscriber_id is None
        assert pipe.processed_messages == []
        assert pipe.process_count == 0
    
    @pytest.mark.asyncio
    async def test_pipe_on_start(self, pipe, pilot):
        """Test Pipe on_start method sets up subscriptions."""
        await pilot.connect()
        
        # Initially no subscriber_id
        assert pipe.subscriber_id is None
        
        # Start the pipe
        await pipe.on_start()
        
        # Should have subscriber_id after start
        assert pipe.subscriber_id is not None
        
        # Cleanup
        await pipe.on_close()
        await pilot.disconnect()
    