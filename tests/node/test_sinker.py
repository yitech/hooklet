import pytest
import asyncio

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
            self.received_messages = []

        async def on_message(self, msg: Msg) -> None:
            """Mock on_message implementation that stores received messages."""
            self.received_messages.append(msg)

    @pytest.fixture
    def pilot(self):
        """Create an InprocPilot instance for testing."""
        return InprocPilot()

    @pytest.fixture
    def pubsub(self, pilot):
        """Get the pubsub interface from the pilot."""
        return pilot.pubsub()

    @pytest.fixture
    def sinker(self, pubsub):
        """Create a test Sinker instance."""
        return self.MockSinker("test-sinker", ["test.subject1", "test.subject2"], pubsub)

    @pytest.mark.asyncio
    async def test_sinker_initialization(self, sinker, pubsub):
        """Test Sinker initialization."""
        assert sinker.name == "test-sinker"
        assert sinker.subscribes == ["test.subject1", "test.subject2"]
        assert sinker.pubsub == pubsub
        assert sinker.received_messages == []
        assert not sinker.is_running

    @pytest.mark.asyncio
    async def test_sinker_start(self, sinker, pilot):
        """Test Sinker start method and subscription setup."""
        await pilot.connect()

        # Start the sinker
        await sinker.start()

        # Check that sinker is running
        assert sinker.is_running

        # Verify subscriptions were set up by checking pubsub directly
        pubsub = sinker.pubsub
        for subject in sinker.subscribes:
            subscriptions = pubsub.get_subscriptions(subject)
            assert len(subscriptions) > 0

        # Cleanup
        await sinker.close()
        await pilot.disconnect()

    @pytest.mark.asyncio
    async def test_sinker_message_reception(self, sinker, pilot):
        """Test that Sinker receives and processes messages correctly."""
        await pilot.connect()
        await sinker.start()

        # Create test messages
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

        # Publish messages to the subscribed subjects
        await sinker.pubsub.publish("test.subject1", test_msg1)
        await sinker.pubsub.publish("test.subject2", test_msg2)

        # Wait for message processing
        await asyncio.sleep(0.1)

        # Verify messages were received
        assert len(sinker.received_messages) == 2
        assert test_msg1 in sinker.received_messages
        assert test_msg2 in sinker.received_messages

        # Cleanup
        await sinker.close()
        await pilot.disconnect()

    @pytest.mark.asyncio
    async def test_sinker_close_and_unsubscription(self, sinker, pilot):
        """Test Sinker close method and unsubscription."""
        await pilot.connect()
        await sinker.start()

        # Verify subscriptions exist
        pubsub = sinker.pubsub
        for subject in sinker.subscribes:
            subscriptions = pubsub.get_subscriptions(subject)
            assert len(subscriptions) > 0

        # Close the sinker
        await sinker.close()

        # Check that sinker is no longer running
        assert not sinker.is_running

        # Verify subscriptions were removed
        for subject in sinker.subscribes:
            subscriptions = pubsub.get_subscriptions(subject)
            assert len(subscriptions) == 0

        # Verify that messages published after close are not received
        initial_msg_count = len(sinker.received_messages)
        test_msg: Msg = {
            "_id": generate_id(),
            "type": "test_message",
            "data": "after_close",
            "error": None
        }

        await sinker.pubsub.publish("test.subject1", test_msg)
        await asyncio.sleep(0.1)

        # Message count should not have increased
        assert len(sinker.received_messages) == initial_msg_count

        await pilot.disconnect()

    @pytest.mark.asyncio
    async def test_sinker_error_handling(self, pilot):
        """Test Sinker error handling during message processing."""
        class ErrorSinker(self.MockSinker):
            """Sinker that raises an exception during message processing."""

            def __init__(self, name: str, subscribes: list[str], pubsub):
                super().__init__(name, subscribes, pubsub)
                self.error_raised = False

            async def on_message(self, msg: Msg) -> None:
                self.error_raised = True
                raise RuntimeError("Test error in message processing")

        await pilot.connect()
        error_sinker = ErrorSinker("error-sinker", ["test.subject1"], pilot.pubsub())
        await error_sinker.start()

        # Create and publish a test message
        test_msg: Msg = {
            "_id": generate_id(),
            "type": "test_message",
            "data": "error_test",
            "error": None
        }

        # Publish message - this should trigger the error
        await error_sinker.pubsub.publish("test.subject1", test_msg)
        await asyncio.sleep(0.1)

        # Verify that the error was raised (but sinker continues to work)
        assert error_sinker.error_raised
        assert error_sinker.is_running  # Sinker should still be running after error

        # Cleanup
        await error_sinker.close()
        await pilot.disconnect()

    @pytest.mark.asyncio
    async def test_sinker_selective_subscription(self, pilot):
        """Test that Sinker only receives messages from subscribed subjects."""
        await pilot.connect()

        # Create a sinker that only subscribes to one subject
        selective_sinker = self.MockSinker("selective-sinker", ["test.subject1"], pilot.pubsub())
        await selective_sinker.start()

        # Create test messages for different subjects
        msg_subscribed: Msg = {
            "_id": generate_id(),
            "type": "subscribed_message",
            "data": "data_for_subscribed",
            "error": None
        }

        msg_unsubscribed: Msg = {
            "_id": generate_id(),
            "type": "unsubscribed_message",
            "data": "data_for_unsubscribed",
            "error": None
        }

        # Publish to subscribed subject
        await selective_sinker.pubsub.publish("test.subject1", msg_subscribed)

        # Publish to unsubscribed subject
        await selective_sinker.pubsub.publish("test.subject.unsubscribed", msg_unsubscribed)

        # Wait for message processing
        await asyncio.sleep(0.1)

        # Verify only the subscribed message was received
        assert len(selective_sinker.received_messages) == 1
        assert selective_sinker.received_messages[0] == msg_subscribed
        assert msg_unsubscribed not in selective_sinker.received_messages

        # Cleanup
        await selective_sinker.close()
        await pilot.disconnect()
