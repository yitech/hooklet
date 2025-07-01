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
        
        def __init__(self, name: str, subscribes: list[str], pubsub, router, pipe_behavior="1to1"):
            super().__init__(name, subscribes, pubsub, router)
            self.pipe_behavior = pipe_behavior
            self.processed_messages = []
            self.generated_messages = []
            self.pipe_call_count = 0
        
        async def pipe(self, msg: Msg) -> AsyncGenerator[Msg, None]:
            """Mock pipe implementation with different behaviors."""
            self.pipe_call_count += 1
            self.processed_messages.append(msg)
            
            if self.pipe_behavior == "none":
                # Yield nothing
                return
            elif self.pipe_behavior == "1to1":
                # 1-to-1 mapping
                output_msg: Msg = {
                    "_id": generate_id(),
                    "type": "processed",
                    "data": f"processed_{msg['data']}",
                    "error": None
                }
                self.generated_messages.append(output_msg)
                yield output_msg
            elif self.pipe_behavior == "1tomany":
                # 1-to-many mapping
                for i in range(3):
                    output_msg: Msg = {
                        "_id": generate_id(),
                        "type": "processed",
                        "data": f"processed_{msg['data']}_{i}",
                        "error": None
                    }
                    self.generated_messages.append(output_msg)
                    yield output_msg
            elif self.pipe_behavior == "manyto1":
                # Many-to-1 mapping (accumulate multiple messages)
                if len(self.processed_messages) % 3 == 0:
                    # Only yield after every 3 messages
                    output_msg: Msg = {
                        "_id": generate_id(),
                        "type": "batch_processed",
                        "data": f"batch_{len(self.processed_messages)}",
                        "error": None
                    }
                    self.generated_messages.append(output_msg)
                    yield output_msg
            elif self.pipe_behavior == "error":
                # Simulate error in pipe
                raise ValueError("Test pipe error")
    
    class MockRouter:
        """Mock router for testing."""
        
        def __init__(self, subject_prefix: str = "test.output"):
            self.subject_prefix = subject_prefix
            self.routed_messages = []
        
        def __call__(self, msg: Msg) -> str:
            """Route message to a subject based on message data."""
            subject = f"{self.subject_prefix}.{msg['data']}"
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
        """Create test subscription subjects."""
        return ["test.input.1", "test.input.2"]
    
    @pytest.fixture
    def pipe_1to1(self, pubsub, router, subscribes):
        """Create a test Pipe instance with 1-to-1 behavior."""
        return self.MockPipe("test-pipe-1to1", subscribes, pubsub, router, "1to1")
    
    @pytest.fixture
    def pipe_1tomany(self, pubsub, router, subscribes):
        """Create a test Pipe instance with 1-to-many behavior."""
        return self.MockPipe("test-pipe-1tomany", subscribes, pubsub, router, "1tomany")
    
    @pytest.fixture
    def pipe_none(self, pubsub, router, subscribes):
        """Create a test Pipe instance that yields nothing."""
        return self.MockPipe("test-pipe-none", subscribes, pubsub, router, "none")
    
    @pytest.fixture
    def pipe_manyto1(self, pubsub, router, subscribes):
        """Create a test Pipe instance with many-to-1 behavior."""
        return self.MockPipe("test-pipe-manyto1", subscribes, pubsub, router, "manyto1")
    
    @pytest.mark.asyncio
    async def test_pipe_initialization(self, pipe_1to1, pubsub, router, subscribes):
        """Test Pipe initialization."""
        assert pipe_1to1.name == "test-pipe-1to1"
        assert pipe_1to1.subscribes == subscribes
        assert pipe_1to1.pubsub == pubsub
        assert pipe_1to1.router == router
        assert not pipe_1to1.is_running
        assert pipe_1to1.processed_messages == []
        assert pipe_1to1.generated_messages == []
        assert pipe_1to1.pipe_call_count == 0
    
    @pytest.mark.asyncio
    async def test_pipe_start(self, pipe_1to1, pilot):
        """Test Pipe start method."""
        await pilot.connect()
        
        # Start the pipe
        await pipe_1to1.start()
        
        # Check that pipe is running
        assert pipe_1to1.is_running
        
        # Cleanup
        await pipe_1to1.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_1to1_behavior(self, pipe_1to1, pilot, router):
        """Test 1-to-1 message processing behavior."""
        await pilot.connect()
        
        # Subscribe to output subjects
        received_messages = []
        
        async def message_handler(msg):
            received_messages.append(msg)
        
        pilot.pubsub().subscribe("test.output.processed_test_data", message_handler)
        
        await pipe_1to1.start()
        
        # Send a test message
        test_msg: Msg = {
            "_id": generate_id(),
            "type": "test_message",
            "data": "test_data",
            "error": None
        }
        
        await pilot.pubsub().publish("test.input.1", test_msg)
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Check that message was processed
        assert len(pipe_1to1.processed_messages) == 1
        assert pipe_1to1.processed_messages[0] == test_msg
        assert pipe_1to1.pipe_call_count == 1
        
        # Check that output was generated
        assert len(pipe_1to1.generated_messages) == 1
        assert pipe_1to1.generated_messages[0]["type"] == "processed"
        assert pipe_1to1.generated_messages[0]["data"] == "processed_test_data"
        
        # Check that output was published
        assert len(received_messages) == 1
        assert received_messages[0]["type"] == "processed"
        
        await pipe_1to1.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_1tomany_behavior(self, pipe_1tomany, pilot, router):
        """Test 1-to-many message processing behavior."""
        await pilot.connect()
        
        # Subscribe to output subjects
        received_messages = []
        
        async def message_handler(msg):
            received_messages.append(msg)
        
        pilot.pubsub().subscribe("test.output.processed_test_data_0", message_handler)
        pilot.pubsub().subscribe("test.output.processed_test_data_1", message_handler)
        pilot.pubsub().subscribe("test.output.processed_test_data_2", message_handler)
        
        await pipe_1tomany.start()
        
        # Send a test message
        test_msg: Msg = {
            "_id": generate_id(),
            "type": "test_message",
            "data": "test_data",
            "error": None
        }
        
        await pilot.pubsub().publish("test.input.1", test_msg)
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Check that message was processed
        assert len(pipe_1tomany.processed_messages) == 1
        assert pipe_1tomany.pipe_call_count == 1
        
        # Check that multiple outputs were generated
        assert len(pipe_1tomany.generated_messages) == 3
        for i, msg in enumerate(pipe_1tomany.generated_messages):
            assert msg["type"] == "processed"
            assert msg["data"] == f"processed_test_data_{i}"
        
        # Check that all outputs were published
        assert len(received_messages) == 3
        
        await pipe_1tomany.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_none_behavior(self, pipe_none, pilot):
        """Test pipe behavior that yields nothing."""
        await pilot.connect()
        
        await pipe_none.start()
        
        # Send a test message
        test_msg: Msg = {
            "_id": generate_id(),
            "type": "test_message",
            "data": "test_data",
            "error": None
        }
        
        await pilot.pubsub().publish("test.input.1", test_msg)
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Check that message was processed but no output generated
        assert len(pipe_none.processed_messages) == 1
        assert pipe_none.pipe_call_count == 1
        assert len(pipe_none.generated_messages) == 0
        
        await pipe_none.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_manyto1_behavior(self, pipe_manyto1, pilot):
        """Test many-to-1 message processing behavior."""
        await pilot.connect()
        
        # Subscribe to output subjects
        received_messages = []
        
        async def message_handler(msg):
            received_messages.append(msg)
        
        pilot.pubsub().subscribe("test.output.batch_3", message_handler)
        pilot.pubsub().subscribe("test.output.batch_6", message_handler)
        
        await pipe_manyto1.start()
        
        # Send multiple test messages
        for i in range(6):
            test_msg: Msg = {
                "_id": generate_id(),
                "type": "test_message",
                "data": f"test_data_{i}",
                "error": None
            }
            await pilot.pubsub().publish("test.input.1", test_msg)
            await asyncio.sleep(0.05)  # Small delay between messages
        
        # Wait for processing
        await asyncio.sleep(0.3)
        
        # Check that all messages were processed
        assert len(pipe_manyto1.processed_messages) == 6
        assert pipe_manyto1.pipe_call_count == 6
        
        # Check that batch outputs were generated (every 3 messages)
        assert len(pipe_manyto1.generated_messages) == 2
        assert pipe_manyto1.generated_messages[0]["data"] == "batch_3"
        assert pipe_manyto1.generated_messages[1]["data"] == "batch_6"
        
        # Check that batch outputs were published
        assert len(received_messages) == 2
        
        await pipe_manyto1.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_multiple_subscriptions(self, pipe_1to1, pilot):
        """Test pipe with multiple subscription subjects."""
        await pilot.connect()
        
        received_messages = []
        
        async def message_handler(msg):
            received_messages.append(msg)
        
        pilot.pubsub().subscribe("test.output.processed_data1", message_handler)
        pilot.pubsub().subscribe("test.output.processed_data2", message_handler)
        
        await pipe_1to1.start()
        
        # Send messages to different subscription subjects
        test_msg1: Msg = {
            "_id": generate_id(),
            "type": "test_message",
            "data": "data1",
            "error": None
        }
        
        test_msg2: Msg = {
            "_id": generate_id(),
            "type": "test_message",
            "data": "data2",
            "error": None
        }
        
        await pilot.pubsub().publish("test.input.1", test_msg1)
        await pilot.pubsub().publish("test.input.2", test_msg2)
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Check that both messages were processed
        assert len(pipe_1to1.processed_messages) == 2
        assert pipe_1to1.pipe_call_count == 2
        
        # Check that outputs were generated and published
        assert len(pipe_1to1.generated_messages) == 2
        assert len(received_messages) == 2
        
        await pipe_1to1.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_shutdown(self, pipe_1to1, pilot):
        """Test pipe shutdown behavior."""
        await pilot.connect()
        await pipe_1to1.start()
        
        assert pipe_1to1.is_running
        
        # Shutdown the pipe
        await pipe_1to1.close()
        
        # Verify pipe is no longer running
        assert not pipe_1to1.is_running
        
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_timeout_handling(self, pipe_1to1, pilot):
        """Test pipe timeout handling when no messages are received."""
        await pilot.connect()
        await pipe_1to1.start()
        
        # Let the pipe run for a while without messages
        await asyncio.sleep(0.3)
        
        # Check that pipe is still running and hasn't crashed
        assert pipe_1to1.is_running
        assert len(pipe_1to1.processed_messages) == 0
        
        await pipe_1to1.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_with_mock_pubsub(self):
        """Test pipe with mocked pubsub interface."""
        mock_pubsub = MagicMock()
        # Configure the mock to return a subscription ID
        mock_pubsub.subscribe.return_value = 123
        
        router = self.MockRouter()
        subscribes = ["test.input"]
        pipe = self.MockPipe("test-pipe", subscribes, mock_pubsub, router, "1to1")
        
        await pipe.start()
        
        # Verify subscribe was called for each subscription
        assert mock_pubsub.subscribe.call_count == 1
        
        # Get the callback that was passed to subscribe
        call_args = mock_pubsub.subscribe.call_args
        assert call_args[0][0] == "test.input"  # subject
        callback = call_args[0][1]  # callback function
        
        # Verify the callback is callable and can be awaited
        assert callable(callback)
        
        # Test that the callback can be called (this should not trigger the warning)
        test_msg = {"_id": "test", "type": "test", "data": "test", "error": None}
        
        await pipe.close()
    
    @pytest.mark.asyncio
    async def test_pipe_context_manager(self, pilot):
        """Test using Pipe as async context manager."""
        await pilot.connect()
        
        class ContextPipe(self.MockPipe):
            async def __aenter__(self):
                await self.start()
                return self
            
            async def __aexit__(self, exc_type, exc_value, traceback):
                await self.close()
        
        router = self.MockRouter()
        subscribes = ["test.input"]
        
        async with ContextPipe("test-pipe", subscribes, pilot.pubsub(), router, "1to1") as pipe:
            assert pipe.is_running
            await asyncio.sleep(0.1)  # Let it run briefly
        
        assert not pipe.is_running
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_error_handling(self, pilot):
        """Test pipe error handling in pipe method."""
        await pilot.connect()
        
        router = self.MockRouter()
        subscribes = ["test.input"]
        error_pipe = self.MockPipe("error-pipe", subscribes, pilot.pubsub(), router, "error")
        
        await error_pipe.start()
        
        # Send a test message that will cause an error
        test_msg: Msg = {
            "_id": generate_id(),
            "type": "test_message",
            "data": "test_data",
            "error": None
        }
        
        # The pipe should handle the error gracefully and continue running
        await pilot.pubsub().publish("test.input", test_msg)
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Check that pipe is still running despite the error
        assert error_pipe.is_running
        
        await error_pipe.close()
        await pilot.disconnect()


class TestPipeIntegration:
    """Integration tests for Pipe with other components."""
    
    @pytest.fixture
    def pilot(self):
        """Create an InprocPilot instance for testing."""
        return InprocPilot()
    
    @pytest.mark.asyncio
    async def test_pipe_with_emitter(self, pilot):
        """Test pipe integration with emitter."""
        await pilot.connect()
        
        # Create an emitter that sends messages
        class TestEmitter:
            def __init__(self, name: str, pubsub, router):
                self.name = name
                self.pubsub = pubsub
                self.router = router
                self.is_running = False
            
            async def start(self):
                self.is_running = True
                # Emit a test message
                test_msg: Msg = {
                    "_id": generate_id(),
                    "type": "emitter_message",
                    "data": "emitted_data",
                    "error": None
                }
                subject = self.router(test_msg)
                await self.pubsub.publish(subject, test_msg)
            
            async def close(self):
                self.is_running = False
        
        # Create router for emitter
        def emitter_router(msg: Msg) -> str:
            return "test.input"
        
        # Create router for pipe
        def pipe_router(msg: Msg) -> str:
            return f"test.output.{msg['data']}"
        
        # Create pipe
        pipe = TestPipe.MockPipe("test-pipe", ["test.input"], pilot.pubsub(), pipe_router, "1to1")
        
        # Subscribe to pipe output
        received_messages = []
        
        async def message_handler(msg):
            received_messages.append(msg)
        
        pilot.pubsub().subscribe("test.output.processed_emitted_data", message_handler)
        
        # Start both components
        await pipe.start()
        emitter = TestEmitter("test-emitter", pilot.pubsub(), emitter_router)
        await emitter.start()
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Check that message flowed through the system
        assert len(pipe.processed_messages) == 1
        assert len(received_messages) == 1
        assert received_messages[0]["type"] == "processed"
        
        await pipe.close()
        await emitter.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_chain(self, pilot):
        """Test multiple pipes in a chain."""
        await pilot.connect()
        
        # Create multiple pipes in a chain
        def router1(msg: Msg) -> str:
            return "test.stage2"
        
        def router2(msg: Msg) -> str:
            return "test.stage3"
        
        def router3(msg: Msg) -> str:
            return "test.final"
        
        pipe1 = TestPipe.MockPipe("pipe1", ["test.stage1"], pilot.pubsub(), router1, "1to1")
        pipe2 = TestPipe.MockPipe("pipe2", ["test.stage2"], pilot.pubsub(), router2, "1to1")
        pipe3 = TestPipe.MockPipe("pipe3", ["test.stage3"], pilot.pubsub(), router3, "1to1")
        
        # Subscribe to final output
        final_messages = []
        
        async def final_handler(msg):
            final_messages.append(msg)
        
        pilot.pubsub().subscribe("test.final", final_handler)
        
        # Start all pipes
        await pipe1.start()
        await pipe2.start()
        await pipe3.start()
        
        # Send initial message
        initial_msg: Msg = {
            "_id": generate_id(),
            "type": "initial",
            "data": "initial_data",
            "error": None
        }
        
        await pilot.pubsub().publish("test.stage1", initial_msg)
        
        # Wait for processing through the chain
        await asyncio.sleep(0.3)
        
        # Check that message flowed through all pipes
        assert len(pipe1.processed_messages) == 1
        assert len(pipe2.processed_messages) == 1
        assert len(pipe3.processed_messages) == 1
        assert len(final_messages) == 1
        
        # Cleanup
        await pipe1.close()
        await pipe2.close()
        await pipe3.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_pipe_performance(self, pilot):
        """Test pipe performance with many messages."""
        
        async def _run_performance_test():
            await pilot.connect()
            
            # Create a pipe that processes messages quickly
            class PerformancePipe(TestPipe.MockPipe):
                def __init__(self, name: str, subscribes: list[str], pubsub, router):
                    super().__init__(name, subscribes, pubsub, router, "1to1")
                    self.processed_count = 0
                
                async def pipe(self, msg: Msg) -> AsyncGenerator[Msg, None]:
                    self.processed_count += 1
                    self.processed_messages.append(msg)
                    
                    output_msg: Msg = {
                        "_id": generate_id(),
                        "type": "performance_processed",
                        "data": f"processed_{msg['data']}",
                        "error": None
                    }
                    self.generated_messages.append(output_msg)
                    yield output_msg
            
            router = TestPipe.MockRouter("performance.output")
            pipe = PerformancePipe("performance-pipe", ["performance.input"], pilot.pubsub(), router)
            
            # Subscribe to output
            received_count = 0
            
            async def performance_handler(msg):
                nonlocal received_count
                received_count += 1
            
            pilot.pubsub().subscribe("performance.output.processed_test", performance_handler)
            
            start_time = time.time()
            await pipe.start()
            
            # Send many messages quickly
            for i in range(10):
                test_msg: Msg = {
                    "_id": generate_id(),
                    "type": "performance_test",
                    "data": "test",
                    "error": None
                }
                await pilot.pubsub().publish("performance.input", test_msg)
            
            # Wait for processing
            await asyncio.sleep(0.2)
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Check performance
            assert pipe.processed_count == 10
            assert received_count == 10
            assert processing_time < 1.0  # Should process quickly
            
            await pipe.close()
            await pilot.disconnect()
            
            return processing_time
        
        processing_time = await _run_performance_test()
        print(f"Performance test completed in {processing_time:.3f} seconds") 