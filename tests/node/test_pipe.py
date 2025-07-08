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
    
    @pytest.mark.asyncio
    async def test_pipe_message_processing(self, pipe, pilot, router):
        """Test Pipe processes and routes messages correctly."""
        await pilot.connect()
        
        # Track output messages
        output_messages = []
        
        async def output_handler(msg):
            output_messages.append(msg)
        
        # Subscribe to output subjects
        await pilot.pubsub().subscribe("test.output.transformed_test_type", output_handler)
        
        # Start the pipe
        await pipe.on_start()
        
        # Create and send an input message
        input_msg: Msg = {
            "_id": generate_id(),
            "type": "test_type",
            "data": "test_data",
            "error": None
        }
        
        # Publish to one of the subscribed subjects
        await pilot.pubsub().publish("test.input.topic1", input_msg)
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Verify message was processed
        assert pipe.process_count == 1
        assert len(pipe.processed_messages) == 1
        assert pipe.processed_messages[0] == input_msg
        
        # Verify output message was published and routed correctly
        assert len(output_messages) == 1
        output_msg = output_messages[0]
        assert output_msg["type"] == "transformed_test_type"
        assert output_msg["data"] == "processed_test_data"
        assert output_msg["original_id"] == input_msg["_id"]
        
        # Verify router was called
        assert len(router.routed_messages) == 1
        routed_subject, routed_msg = router.routed_messages[0]
        assert routed_subject == "test.output.transformed_test_type"
        assert routed_msg == output_msg
        
        # Cleanup
        await pipe.on_close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_on_close(self, pipe, pilot):
        """Test Pipe on_close method cleans up subscriptions."""
        await pilot.connect()
        
        # Start the pipe to set up subscriptions
        await pipe.on_start()
        subscriber_id = pipe.subscriber_id
        assert subscriber_id is not None
        
        # Close the pipe
        await pipe.on_close()
        
        # subscriber_id should still be available for verification
        # but subscriptions should be cleaned up
        assert pipe.subscriber_id == subscriber_id
        
        # Verify subscriptions are cleaned up by publishing a message
        # and ensuring it's not processed
        pipe.process_count = 0  # Reset counter
        pipe.processed_messages.clear()
        
        input_msg: Msg = {
            "_id": generate_id(),
            "type": "test_type", 
            "data": "test_data",
            "error": None
        }
        
        # Publish to subscribed subject
        await pilot.pubsub().publish("test.input.topic1", input_msg)
        
        # Wait a bit
        await asyncio.sleep(0.1)
        
        # Should not have processed the message after close
        assert pipe.process_count == 0
        assert len(pipe.processed_messages) == 0
        
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_error_handling(self, pilot, pubsub, router, subscribes):
        """Test Pipe error handling when message processing fails."""
        
        class ErrorPipe(self.MockPipe):
            """Pipe that raises an error during message processing."""
            
            async def on_message(self, msg: Msg) -> AsyncGenerator[Msg, None]:
                self.processed_messages.append(msg)
                self.process_count += 1
                raise RuntimeError("Test error during processing")
                yield  # This line will never be reached
        
        await pilot.connect()
        
        error_pipe = ErrorPipe("error-pipe", subscribes, pubsub, router)
        
        # Mock the on_error method to track errors
        error_pipe.on_error = AsyncMock()
        
        # Start the pipe
        await error_pipe.on_start()
        
        # Send a message that will cause an error
        input_msg: Msg = {
            "_id": generate_id(),
            "type": "test_type",
            "data": "test_data", 
            "error": None
        }
        
        await pilot.pubsub().publish("test.input.topic1", input_msg)
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Verify message was received and error occurred
        assert error_pipe.process_count == 1
        assert len(error_pipe.processed_messages) == 1
        
        # Verify on_error was called
        error_pipe.on_error.assert_called_once()
        error_args = error_pipe.on_error.call_args[0]
        assert len(error_args) == 1
        assert isinstance(error_args[0], RuntimeError)
        assert str(error_args[0]) == "Test error during processing"
        
        # Cleanup
        await error_pipe.on_close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_multiple_messages(self, pipe, pilot, router):
        """Test Pipe processes multiple messages from different subjects."""
        await pilot.connect()
        
        # Track output messages
        output_messages = []
        
        async def output_handler(msg):
            output_messages.append(msg)
        
        # Subscribe to output subjects
        await pilot.pubsub().subscribe("test.output.transformed_type1", output_handler)
        await pilot.pubsub().subscribe("test.output.transformed_type2", output_handler)
        
        # Start the pipe
        await pipe.on_start()
        
        # Create and send multiple messages to different subjects
        input_msgs = [
            {
                "_id": generate_id(),
                "type": "type1",
                "data": "data1",
                "error": None
            },
            {
                "_id": generate_id(),
                "type": "type2", 
                "data": "data2",
                "error": None
            },
            {
                "_id": generate_id(),
                "type": "type1",
                "data": "data3",
                "error": None
            }
        ]
        
        # Publish to both subscribed subjects
        await pilot.pubsub().publish("test.input.topic1", input_msgs[0])
        await pilot.pubsub().publish("test.input.topic2", input_msgs[1])
        await pilot.pubsub().publish("test.input.topic1", input_msgs[2])
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Verify all messages were processed
        assert pipe.process_count == 3
        assert len(pipe.processed_messages) == 3
        
        # Verify all input messages were received
        for input_msg in input_msgs:
            assert input_msg in pipe.processed_messages
        
        # Verify all output messages were published
        assert len(output_messages) == 3
        
        # Verify correct transformation and routing
        expected_outputs = [
            ("transformed_type1", "processed_data1", input_msgs[0]["_id"]),
            ("transformed_type2", "processed_data2", input_msgs[1]["_id"]), 
            ("transformed_type1", "processed_data3", input_msgs[2]["_id"])
        ]
        
        for i, (expected_type, expected_data, expected_original_id) in enumerate(expected_outputs):
            output_msg = output_messages[i]
            assert output_msg["type"] == expected_type
            assert output_msg["data"] == expected_data
            assert output_msg["original_id"] == expected_original_id
        
        # Verify router was called for each message
        assert len(router.routed_messages) == 3
        
        # Cleanup
        await pipe.on_close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_full_lifecycle(self, pipe, pilot, router):
        """Test Pipe with full Node lifecycle (start/close methods)."""
        await pilot.connect()
        
        # Track output messages
        output_messages = []
        
        async def output_handler(msg):
            output_messages.append(msg)
        
        # Subscribe to output subject
        await pilot.pubsub().subscribe("test.output.transformed_test_type", output_handler)
        
        # Initially pipe should not be running
        assert not pipe.is_running
        assert pipe.subscriber_id is None
        
        # Start the pipe using the Node's start method
        await pipe.start()
        
        # Now pipe should be running
        assert pipe.is_running
        assert pipe.subscriber_id is not None
        
        # Send a message
        input_msg: Msg = {
            "_id": generate_id(),
            "type": "test_type",
            "data": "lifecycle_test",
            "error": None
        }
        
        await pilot.pubsub().publish("test.input.topic1", input_msg)
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Verify message was processed
        assert pipe.process_count == 1
        assert len(output_messages) == 1
        
        # Close the pipe using the Node's close method
        await pipe.close()
        
        # Pipe should no longer be running
        assert not pipe.is_running
        
        # Reset counters and send another message
        pipe.process_count = 0
        pipe.processed_messages.clear()
        output_messages.clear()
        
        await pilot.pubsub().publish("test.input.topic1", input_msg)
        
        # Wait a bit
        await asyncio.sleep(0.1)
        
        # Should not have processed the message after close
        assert pipe.process_count == 0
        assert len(output_messages) == 0
        
        await pilot.disconnect()
    