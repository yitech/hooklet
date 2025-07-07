import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch
from typing import AsyncGenerator

from hooklet.node.emitter import Emitter
from hooklet.base.pilot import Msg
from hooklet.pilot.inproc_pilot import InprocPilot
from hooklet.utils.id_generator import generate_id


class TestEmitter:
    """Test cases for Emitter class."""
    
    class MockEmitter(Emitter):
        """Concrete implementation of Emitter for testing."""
        
        def __init__(self, name: str, pubsub, router):
            super().__init__(name, pubsub, router)
            self.emitted_messages = []
            self.emit_count = 0
            self.max_emits = 3  # Limit for testing
        
        async def emit(self) -> AsyncGenerator[Msg, None]:
            """Mock emit implementation that yields test messages."""
            for i in range(self.max_emits):
                if self.shutdown_event.is_set():
                    break
                msg: Msg = {
                    "_id": generate_id(),
                    "type": "test_message",
                    "data": f"test_data_{i}",
                    "error": None
                }
                self.emitted_messages.append(msg)
                self.emit_count += 1
                yield msg
                await asyncio.sleep(0.05)  # Shorter delay for faster tests
    
    class MockRouter:
        """Mock router for testing."""
        
        def __init__(self, subject_prefix: str = "test.subject"):
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
    def emitter(self, pubsub, router):
        """Create a test Emitter instance."""
        return self.MockEmitter("test-emitter", pubsub, router)
    
    @pytest.mark.asyncio
    async def test_emitter_initialization(self, emitter, pubsub, router):
        """Test Emitter initialization."""
        assert emitter.name == "test-emitter"
        assert emitter.pubsub == pubsub
        assert emitter.router == router
        assert not emitter.is_running
        assert emitter.emitted_messages == []
        assert emitter.emit_count == 0
    
    @pytest.mark.asyncio
    async def test_emitter_start(self, emitter, pilot):
        """Test Emitter start method."""
        await pilot.connect()
        
        # Start the emitter
        await emitter.start()
        
        # Check that emitter is running
        assert emitter.is_running
        
        # Wait a bit for messages to be emitted
        await asyncio.sleep(0.3)
        
        # Check that messages were emitted
        assert len(emitter.emitted_messages) > 0
        assert emitter.emit_count > 0
        
        # Cleanup
        await emitter.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_emitter_message_routing(self, emitter, pilot, router):
        """Test that emitted messages are properly routed."""
        await pilot.connect()
        
        # Subscribe to test subjects
        received_messages = []
        
        async def message_handler(msg):
            received_messages.append(msg)
        
        # Subscribe to the expected subjects
        for i in range(3):
            subject = f"test.subject.test_data_{i}"
            pilot.pubsub().subscribe(subject, message_handler)
        
        await emitter.start()
        
        # Wait for messages to be emitted and processed
        await asyncio.sleep(0.3)
        
        # Check that messages were routed correctly
        assert len(router.routed_messages) > 0
        
        # Check that messages were published to correct subjects
        for subject, msg in router.routed_messages:
            assert subject.startswith("test.subject.test_data_")
            assert msg["type"] == "test_message"
        
        await emitter.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_emitter_shutdown(self, emitter, pilot):
        """Test emitter shutdown behavior."""
        await pilot.connect()
        await emitter.start()
        
        assert emitter.is_running
        
        # Shutdown the emitter
        await emitter.close()
        
        # Verify emitter is no longer running
        assert not emitter.is_running
        
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_emitter_with_mock_pubsub(self):
        """Test emitter with mocked pubsub interface."""
        mock_pubsub = AsyncMock()
        router = self.MockRouter()
        emitter = self.MockEmitter("test-emitter", mock_pubsub, router)
        
        await emitter.start()
        
        # Wait a bit for messages to be emitted
        await asyncio.sleep(0.3)
        
        # Verify publish was called
        assert mock_pubsub.publish.call_count > 0
        
        await emitter.close()
    
    @pytest.mark.asyncio
    async def test_emitter_context_manager(self, pilot):
        """Test using Emitter as async context manager."""
        await pilot.connect()
        
        class ContextEmitter(self.MockEmitter):
            async def __aenter__(self):
                await self.start()
                return self
            
            async def __aexit__(self, exc_type, exc_value, traceback):
                await self.close()
        
        router = self.MockRouter()
        async with ContextEmitter("test-emitter", pilot.pubsub(), router) as emitter:
            assert emitter.is_running
            await asyncio.sleep(0.2)  # Let it emit some messages
        
        assert not emitter.is_running
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_emitter_error_handling(self, pilot):
        """Test emitter error handling."""
        await pilot.connect()
        
        class ErrorEmitter(self.MockEmitter):
            async def emit(self) -> AsyncGenerator[Msg, None]:
                """Emit method that raises an exception after yielding one message."""
                # Yield one message first to ensure the coroutine is properly awaited
                msg: Msg = {
                    "_id": generate_id(),
                    "type": "error_test",
                    "data": "test_data",
                    "error": None
                }
                yield msg
                # Then raise the exception
                raise RuntimeError("Test error")
        
        router = self.MockRouter()
        emitter = ErrorEmitter("test-emitter", pilot.pubsub(), router)
        
        # Should not raise exception, but should handle it gracefully
        await emitter.start()
        await asyncio.sleep(0.1)
        
        # Emitter should NOT be running after the error
        assert not emitter.is_running
        
        # Should still be able to close without error
        await emitter.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_emitter_multiple_instances(self, pilot):
        """Test multiple emitter instances working together."""
        await pilot.connect()
        
        router1 = self.MockRouter("emitter1.subject")
        router2 = self.MockRouter("emitter2.subject")
        
        emitter1 = self.MockEmitter("emitter1", pilot.pubsub(), router1)
        emitter2 = self.MockEmitter("emitter2", pilot.pubsub(), router2)
        
        # Start both emitters
        await emitter1.start()
        await emitter2.start()
        
        # Wait for messages to be emitted
        await asyncio.sleep(0.3)
        
        # Check that both emitters emitted messages
        assert len(emitter1.emitted_messages) > 0
        assert len(emitter2.emitted_messages) > 0
        
        # Check that routers handled messages correctly
        assert len(router1.routed_messages) > 0
        assert len(router2.routed_messages) > 0
        
        # Cleanup
        await emitter1.close()
        await emitter2.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_emitter_custom_router(self, pilot):
        """Test emitter with custom router function."""
        await pilot.connect()
        
        def custom_router(msg: Msg) -> str:
            """Custom router that routes based on message type."""
            if msg["type"] == "test_message":
                return "custom.test"
            return "custom.other"
        
        emitter = self.MockEmitter("test-emitter", pilot.pubsub(), custom_router)
        
        # Subscribe to the custom subject
        received_messages = []
        
        async def message_handler(msg):
            received_messages.append(msg)
        
        await pilot.pubsub().subscribe("custom.test", message_handler)
        await pilot.pubsub().publish("test-emitter", {"type": "test_message", "data": "test_data"})
        
        await emitter.start()
        await asyncio.sleep(0.3)
        
        # Check that messages were routed to custom subject
        assert len(received_messages) > 0
        
        await emitter.close()
        await pilot.disconnect()


class TestEmitterIntegration:
    """Integration tests for Emitter with other components."""
    
    @pytest.fixture
    def pilot(self):
        """Create an InprocPilot instance for testing."""
        return InprocPilot()
    
    @pytest.mark.asyncio
    async def test_emitter_with_subscribers(self, pilot):
        """Test emitter with multiple subscribers."""
        await pilot.connect()
        
        class IntegrationEmitter(TestEmitter.MockEmitter):
            async def emit(self) -> AsyncGenerator[Msg, None]:
                """Emit messages that will be received by subscribers."""
                for i in range(2):
                    if self.shutdown_event.is_set():
                        break
                    msg: Msg = {
                        "_id": generate_id(),
                        "type": "integration_test",
                        "data": f"data_{i}",
                        "error": None
                    }
                    self.emitted_messages.append(msg)
                    self.emit_count += 1
                    yield msg
                    await asyncio.sleep(0.05)
        
        router = TestEmitter.MockRouter("integration.subject")
        emitter = IntegrationEmitter("integration-emitter", pilot.pubsub(), router)
        
        # Create subscribers
        subscriber1_messages = []
        subscriber2_messages = []
        
        async def subscriber1_handler(msg):
            subscriber1_messages.append(msg)
        
        async def subscriber2_handler(msg):
            subscriber2_messages.append(msg)
        
        # Subscribe to subjects
        await pilot.pubsub().subscribe("integration.subject.data_0", subscriber1_handler)
        await pilot.pubsub().subscribe("integration.subject.data_1", subscriber2_handler)
        
        # Start emitter
        await emitter.start()
        await asyncio.sleep(0.2)
        
        # Check that subscribers received messages
        assert len(subscriber1_messages) > 0
        assert len(subscriber2_messages) > 0
        
        await emitter.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_emitter_performance(self, pilot):
        """Test emitter performance with many messages."""
        
        async def _run_performance_test():
            await pilot.connect()
            
            class PerformanceEmitter(TestEmitter.MockEmitter):
                def __init__(self, name: str, pubsub, router):
                    super().__init__(name, pubsub, router)
                    self.max_emits = 10  # More messages for performance test
                    self._emitted_once = False
                
                async def emit(self) -> AsyncGenerator[Msg, None]:
                    """Emit messages quickly for performance testing."""
                    if self._emitted_once:
                        return  # Stop after first emission
                    
                    self._emitted_once = True
                    for i in range(self.max_emits):
                        if self.shutdown_event.is_set():
                            break
                        msg: Msg = {
                            "_id": generate_id(),
                            "type": "performance_test",
                            "data": f"perf_data_{i}",
                            "error": None
                        }
                        self.emitted_messages.append(msg)
                        self.emit_count += 1
                        yield msg
                        # No sleep for performance test
            
            router = TestEmitter.MockRouter("performance.subject")
            emitter = PerformanceEmitter("performance-emitter", pilot.pubsub(), router)
            
            start_time = time.time()
            await emitter.start()
            
            # Wait for messages to be emitted
            await asyncio.sleep(0.1)
            
            await emitter.close()
            end_time = time.time()
            
            # Check performance - should have emitted exactly 10 messages
            assert len(emitter.emitted_messages) == 10
            assert end_time - start_time < 5.0  # Should complete within 5 seconds
            
            await pilot.disconnect()
        
        # Run the test with a 10-second timeout
        await asyncio.wait_for(_run_performance_test(), timeout=10.0)
