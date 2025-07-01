import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch
from typing import AsyncGenerator

from hooklet.node.sinker import Sinker
from hooklet.pilot.inproc_pilot import InprocPilot
from hooklet.base.types import Msg
from hooklet.utils.id_generator import generate_id


class TestSinker:
    """Test cases for Sinker class."""
    
    class MockSinker(Sinker):
        """Concrete implementation of Sinker for testing."""
        
        def __init__(self, name: str, subscribes: list[str], pubsub):
            super().__init__(name, subscribes, pubsub)
            self.sunk_messages = []
            self.sink_call_count = 0
            self.sink_behavior = "normal"  # normal, error, slow
        
        async def sink(self, msg: Msg) -> None:
            """Mock sink implementation with different behaviors."""
            self.sink_call_count += 1
            self.sunk_messages.append(msg)
            
            if self.sink_behavior == "error":
                raise ValueError("Test sink error")
            elif self.sink_behavior == "slow":
                await asyncio.sleep(0.1)  # Simulate slow processing
    
    @pytest.fixture
    def pilot(self):
        """Create an InprocPilot instance for testing."""
        return InprocPilot()
    
    @pytest.fixture
    def pubsub(self, pilot):
        """Get the pubsub interface from the pilot."""
        return pilot.pubsub()
    
    @pytest.fixture
    def subscribes(self):
        """Create test subscription subjects."""
        return ["test.input.1", "test.input.2"]
    
    @pytest.fixture
    def sinker(self, pubsub, subscribes):
        """Create a test Sinker instance."""
        return self.MockSinker("test-sinker", subscribes, pubsub)
    
    @pytest.fixture
    def error_sinker(self, pubsub, subscribes):
        """Create a test Sinker instance that raises errors."""
        sinker = self.MockSinker("test-error-sinker", subscribes, pubsub)
        sinker.sink_behavior = "error"
        return sinker
    
    @pytest.fixture
    def slow_sinker(self, pubsub, subscribes):
        """Create a test Sinker instance with slow processing."""
        sinker = self.MockSinker("test-slow-sinker", subscribes, pubsub)
        sinker.sink_behavior = "slow"
        return sinker
    
    @pytest.mark.asyncio
    async def test_sinker_initialization(self, sinker, pubsub, subscribes):
        """Test Sinker initialization."""
        assert sinker.name == "test-sinker"
        assert sinker.subscribes == subscribes
        assert sinker.pubsub == pubsub
        assert not sinker.is_running
        assert sinker.sunk_messages == []
        assert sinker.sink_call_count == 0
        assert isinstance(sinker.queue, asyncio.Queue)
    
    @pytest.mark.asyncio
    async def test_sinker_start(self, sinker, pilot):
        """Test Sinker start method."""
        await pilot.connect()
        
        # Start the sinker
        await sinker.start()
        
        # Check that sinker is running
        assert sinker.is_running
        
        # Cleanup
        await sinker.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_sinker_message_processing(self, sinker, pilot):
        """Test that sinker processes messages correctly."""
        await pilot.connect()
        
        await sinker.start()
        
        # Send test messages
        test_msg1: Msg = {
            "_id": generate_id(),
            "type": "test_message",
            "data": "test_data_1",
            "error": None
        }
        
        test_msg2: Msg = {
            "_id": generate_id(),
            "type": "test_message",
            "data": "test_data_2",
            "error": None
        }
        
        await pilot.pubsub().publish("test.input.1", test_msg1)
        await pilot.pubsub().publish("test.input.2", test_msg2)
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Check that messages were processed
        assert len(sinker.sunk_messages) == 2
        assert sinker.sink_call_count == 2
        
        # Check that both messages were received
        msg_data = [msg["data"] for msg in sinker.sunk_messages]
        assert "test_data_1" in msg_data
        assert "test_data_2" in msg_data
        
        await sinker.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_sinker_multiple_subscriptions(self, sinker, pilot):
        """Test sinker with multiple subscription subjects."""
        await pilot.connect()
        
        await sinker.start()
        
        # Send messages to different subjects
        test_messages = []
        for i in range(5):
            msg: Msg = {
                "_id": generate_id(),
                "type": "test_message",
                "data": f"test_data_{i}",
                "error": None
            }
            test_messages.append(msg)
            
            # Alternate between the two subscription subjects
            subject = "test.input.1" if i % 2 == 0 else "test.input.2"
            await pilot.pubsub().publish(subject, msg)
        
        # Wait for processing
        await asyncio.sleep(0.3)
        
        # Check that all messages were processed
        assert len(sinker.sunk_messages) == 5
        assert sinker.sink_call_count == 5
        
        await sinker.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_sinker_shutdown(self, sinker, pilot):
        """Test sinker shutdown behavior."""
        await pilot.connect()
        await sinker.start()
        
        assert sinker.is_running
        
        # Shutdown the sinker
        await sinker.close()
        
        # Verify sinker is no longer running
        assert not sinker.is_running
        
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_sinker_with_mock_pubsub(self):
        """Test sinker with mocked pubsub interface."""
        mock_pubsub = MagicMock()
        subscribes = ["test.input.1", "test.input.2"]
        sinker = self.MockSinker("test-sinker", subscribes, mock_pubsub)
        
        await sinker.start()
        
        # Verify subscribe was called for each subscription
        assert mock_pubsub.subscribe.call_count == 2
        
        # Check that the correct subjects were subscribed to
        call_args = [call[0][0] for call in mock_pubsub.subscribe.call_args_list]
        assert "test.input.1" in call_args
        assert "test.input.2" in call_args
        
        await sinker.close()
        
        # Verify unsubscribe was called
        assert mock_pubsub.unsubscribe.call_count == 2
    
    @pytest.mark.asyncio
    async def test_sinker_context_manager(self, pilot):
        """Test using Sinker as async context manager."""
        await pilot.connect()
        
        class ContextSinker(self.MockSinker):
            async def __aenter__(self):
                await self.start()
                return self
            
            async def __aexit__(self, exc_type, exc_value, traceback):
                await self.close()
        
        subscribes = ["test.input.1"]
        async with ContextSinker("test-sinker", subscribes, pilot.pubsub()) as sinker:
            assert sinker.is_running
            await asyncio.sleep(0.1)  # Let it run for a bit
        
        assert not sinker.is_running
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_sinker_error_handling(self, error_sinker, pilot):
        """Test sinker error handling."""
        await pilot.connect()
        
        # Mock the on_error method to capture errors
        error_calls = []
        original_on_error = error_sinker.on_error
        
        async def mock_on_error(error):
            error_calls.append(error)
            await original_on_error(error)
        
        error_sinker.on_error = mock_on_error
        
        await error_sinker.start()
        
        # Send a test message that will cause an error
        test_msg: Msg = {
            "_id": generate_id(),
            "type": "test_message",
            "data": "test_data",
            "error": None
        }
        
        await pilot.pubsub().publish("test.input.1", test_msg)
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Check that error was handled
        assert len(error_calls) == 1
        assert isinstance(error_calls[0], ValueError)
        assert str(error_calls[0]) == "Test sink error"
        
        # Check that sinker is still running despite the error
        assert error_sinker.is_running
        
        await error_sinker.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_sinker_slow_processing(self, slow_sinker, pilot):
        """Test sinker with slow processing behavior."""
        await pilot.connect()
        
        await slow_sinker.start()
        
        # Send multiple messages
        for i in range(3):
            msg: Msg = {
                "_id": generate_id(),
                "type": "test_message",
                "data": f"test_data_{i}",
                "error": None
            }
            await pilot.pubsub().publish("test.input.1", msg)
        
        # Wait for processing (should take longer due to slow processing)
        await asyncio.sleep(0.5)
        
        # Check that all messages were eventually processed
        assert len(slow_sinker.sunk_messages) == 3
        assert slow_sinker.sink_call_count == 3
        
        await slow_sinker.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_sinker_queue_behavior(self, sinker, pilot):
        """Test sinker queue behavior under load."""
        await pilot.connect()
        
        await sinker.start()
        
        # Send many messages rapidly
        messages_sent = 10
        for i in range(messages_sent):
            msg: Msg = {
                "_id": generate_id(),
                "type": "test_message",
                "data": f"test_data_{i}",
                "error": None
            }
            await pilot.pubsub().publish("test.input.1", msg)
        
        # Wait for processing
        await asyncio.sleep(0.3)
        
        # Check that all messages were processed
        assert len(sinker.sunk_messages) == messages_sent
        assert sinker.sink_call_count == messages_sent
        
        await sinker.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_sinker_unsubscribe_on_close(self, sinker, pilot):
        """Test that sinker properly unsubscribes on close."""
        await pilot.connect()
        
        await sinker.start()
        
        # Verify subscriptions are active
        assert sinker.is_running
        
        # Close the sinker
        await sinker.close()
        
        # Verify sinker is no longer running
        assert not sinker.is_running
        
        # Send a message after close - should not be processed
        test_msg: Msg = {
            "_id": generate_id(),
            "type": "test_message",
            "data": "test_data_after_close",
            "error": None
        }
        
        await pilot.pubsub().publish("test.input.1", test_msg)
        
        # Wait a bit
        await asyncio.sleep(0.1)
        
        # Verify the message was not processed
        assert len(sinker.sunk_messages) == 0
        
        await pilot.disconnect()


class TestSinkerIntegration:
    """Integration tests for Sinker class."""
    
    @pytest.fixture
    def pilot(self):
        """Create an InprocPilot instance for testing."""
        return InprocPilot()
    
    @pytest.fixture
    def pubsub(self, pilot):
        """Get the pubsub interface from the pilot."""
        return pilot.pubsub()
    
    @pytest.mark.asyncio
    async def test_sinker_with_emitter(self, pilot):
        """Test sinker receiving messages from an emitter."""
        await pilot.connect()
        
        class TestEmitter:
            def __init__(self, name: str, pubsub, router):
                self.name = name
                self.pubsub = pubsub
                self.router = router
                self.is_running = False
                self.task = None
                self.shutdown_event = asyncio.Event()
            
            async def start(self):
                self.is_running = True
                self.task = asyncio.create_task(self.run())
            
            async def run(self):
                for i in range(3):
                    if self.shutdown_event.is_set():
                        break
                    msg: Msg = {
                        "_id": generate_id(),
                        "type": "emitted",
                        "data": f"emitted_data_{i}",
                        "error": None
                    }
                    subject = self.router(msg)
                    await self.pubsub.publish(subject, msg)
                    await asyncio.sleep(0.05)
            
            async def close(self):
                self.shutdown_event.set()
                if self.task:
                    self.task.cancel()
                self.is_running = False
        
        def emitter_router(msg: Msg) -> str:
            return f"test.input.{msg['data']}"
        
        # Create emitter and sinker
        emitter = TestEmitter("test-emitter", pilot.pubsub(), emitter_router)
        sinker = TestSinker.MockSinker("test-sinker", ["test.input.emitted_data_0", "test.input.emitted_data_1", "test.input.emitted_data_2"], pilot.pubsub())
        
        # Start both
        await emitter.start()
        await sinker.start()
        
        # Wait for messages to be emitted and processed
        await asyncio.sleep(0.3)
        
        # Check that messages were received by sinker
        assert len(sinker.sunk_messages) == 3
        assert sinker.sink_call_count == 3
        
        # Verify message content
        for i, msg in enumerate(sinker.sunk_messages):
            assert msg["type"] == "emitted"
            assert msg["data"] == f"emitted_data_{i}"
        
        # Cleanup
        await emitter.close()
        await sinker.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_sinker_with_pipe(self, pilot):
        """Test sinker receiving processed messages from a pipe."""
        await pilot.connect()
        
        class TestPipe:
            def __init__(self, name: str, subscribes: list[str], pubsub, router):
                self.name = name
                self.subscribes = subscribes
                self.pubsub = pubsub
                self.router = router
                self.is_running = False
                self.task = None
                self.shutdown_event = asyncio.Event()
                self.queue = asyncio.Queue()
            
            async def start(self):
                self.is_running = True
                for subscribe in self.subscribes:
                    self.pubsub.subscribe(subscribe, self.queue.put)
                self.task = asyncio.create_task(self.run())
            
            async def run(self):
                while self.is_running and not self.shutdown_event.is_set():
                    try:
                        msg = await asyncio.wait_for(self.queue.get(), timeout=0.1)
                        await self.process_message(msg)
                    except asyncio.TimeoutError:
                        continue
            
            async def process_message(self, msg: Msg):
                # Process the message and emit a new one
                processed_msg: Msg = {
                    "_id": generate_id(),
                    "type": "processed",
                    "data": f"processed_{msg['data']}",
                    "error": None
                }
                subject = self.router(processed_msg)
                await self.pubsub.publish(subject, processed_msg)
            
            async def close(self):
                self.shutdown_event.set()
                if self.task:
                    self.task.cancel()
                self.is_running = False
        
        def pipe_router(msg: Msg) -> str:
            return f"test.output.{msg['data']}"
        
        # Create pipe and sinker
        pipe = TestPipe("test-pipe", ["test.input"], pilot.pubsub(), pipe_router)
        sinker = TestSinker.MockSinker("test-sinker", ["test.output.processed_test_data"], pilot.pubsub())
        
        # Start both
        await pipe.start()
        await sinker.start()
        
        # Send a message to the pipe
        test_msg: Msg = {
            "_id": generate_id(),
            "type": "test_message",
            "data": "test_data",
            "error": None
        }
        
        await pilot.pubsub().publish("test.input", test_msg)
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Check that processed message was received by sinker
        assert len(sinker.sunk_messages) == 1
        assert sinker.sunk_messages[0]["type"] == "processed"
        assert sinker.sunk_messages[0]["data"] == "processed_test_data"
        
        # Cleanup
        await pipe.close()
        await sinker.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_sinker_performance(self, pilot):
        """Test sinker performance under high message load."""
        await pilot.connect()
        
        async def _run_performance_test():
            # Create a high-performance sinker
            class PerformanceSinker(TestSinker.MockSinker):
                def __init__(self, name: str, subscribes: list[str], pubsub):
                    super().__init__(name, subscribes, pubsub)
                    self.processed_count = 0
                
                async def sink(self, msg: Msg) -> None:
                    self.processed_count += 1
                    self.sunk_messages.append(msg)
                    # Minimal processing for performance test
            
            sinker = PerformanceSinker("perf-sinker", ["test.perf"], pilot.pubsub())
            await sinker.start()
            
            # Send many messages rapidly
            message_count = 100
            start_time = time.time()
            
            for i in range(message_count):
                msg: Msg = {
                    "_id": generate_id(),
                    "type": "perf_test",
                    "data": f"perf_data_{i}",
                    "error": None
                }
                await pilot.pubsub().publish("test.perf", msg)
            
            # Wait for all messages to be processed
            await asyncio.sleep(0.5)
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Verify all messages were processed
            assert sinker.processed_count == message_count
            assert len(sinker.sunk_messages) == message_count
            
            # Performance should be reasonable (less than 1 second for 100 messages)
            assert processing_time < 1.0
            
            await sinker.close()
        
        await _run_performance_test()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_multiple_sinkers(self, pilot):
        """Test multiple sinkers working together."""
        await pilot.connect()
        
        # Create multiple sinkers with different subscriptions
        sinker1 = TestSinker.MockSinker("sinker-1", ["test.type1"], pilot.pubsub())
        sinker2 = TestSinker.MockSinker("sinker-2", ["test.type2"], pilot.pubsub())
        sinker3 = TestSinker.MockSinker("sinker-3", ["test.type1", "test.type2"], pilot.pubsub())
        
        # Start all sinkers
        await sinker1.start()
        await sinker2.start()
        await sinker3.start()
        
        # Send messages to different types
        for i in range(5):
            msg1: Msg = {
                "_id": generate_id(),
                "type": "type1",
                "data": f"data1_{i}",
                "error": None
            }
            msg2: Msg = {
                "_id": generate_id(),
                "type": "type2",
                "data": f"data2_{i}",
                "error": None
            }
            
            await pilot.pubsub().publish("test.type1", msg1)
            await pilot.pubsub().publish("test.type2", msg2)
        
        # Wait for processing
        await asyncio.sleep(0.3)
        
        # Verify message distribution
        assert len(sinker1.sunk_messages) == 5  # Only type1 messages
        assert len(sinker2.sunk_messages) == 5  # Only type2 messages
        assert len(sinker3.sunk_messages) == 10  # Both type1 and type2 messages
        
        # Verify message types
        for msg in sinker1.sunk_messages:
            assert msg["type"] == "type1"
        
        for msg in sinker2.sunk_messages:
            assert msg["type"] == "type2"
        
        # Cleanup
        await sinker1.close()
        await sinker2.close()
        await sinker3.close()
        await pilot.disconnect() 