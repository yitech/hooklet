import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from hooklet.pilot.inproc_pilot import InprocPilot, InprocPubSub, InprocReqReply
from hooklet.base.types import Msg


class TestInprocPilot:
    """Test cases for InprocPilot class."""

    @pytest.fixture
    def pilot(self):
        """Create a fresh InprocPilot instance for each test."""
        return InprocPilot()

    @pytest.fixture
    def sample_msg(self):
        """Create a sample message for testing."""
        return {"data": "test_message", "id": "123"}

    @pytest_asyncio.fixture
    async def connected_pilot(self):
        """Create and connect an InprocPilot instance."""
        pilot = InprocPilot()
        await pilot.connect()
        yield pilot
        await pilot.disconnect()

    def test_init(self, pilot):
        """Test InprocPilot initialization."""
        assert pilot is not None
        assert not pilot.is_connected()
        assert pilot.pubsub() is not None
        assert pilot.reqreply() is not None

    @pytest.mark.asyncio
    async def test_connect(self, pilot):
        """Test connecting to the pilot."""
        assert not pilot.is_connected()
        await pilot.connect()
        assert pilot.is_connected()

    @pytest.mark.asyncio
    async def test_disconnect(self, connected_pilot):
        """Test disconnecting from the pilot."""
        assert connected_pilot.is_connected()
        await connected_pilot.disconnect()
        assert not connected_pilot.is_connected()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test using InprocPilot as async context manager."""
        async with InprocPilot() as pilot:
            assert pilot.is_connected()
        assert not pilot.is_connected()

    def test_pubsub_interface(self, pilot):
        """Test that pubsub() returns the correct interface."""
        pubsub = pilot.pubsub()
        assert hasattr(pubsub, 'publish')
        assert hasattr(pubsub, 'subscribe')
        assert hasattr(pubsub, 'unsubscribe')

    def test_reqreply_interface(self, pilot):
        """Test that reqreply() returns the correct interface."""
        reqreply = pilot.reqreply()
        assert hasattr(reqreply, 'request')
        assert hasattr(reqreply, 'register_callback')
        assert hasattr(reqreply, 'unregister_callback')


class TestInprocPubSub:
    """Test cases for InprocPubSub class."""

    @pytest.fixture
    def pubsub(self):
        """Create an InprocPubSub instance."""
        return InprocPubSub()

    @pytest.fixture
    def sample_msg(self):
        """Create a sample message for testing."""
        return {"data": "test_message", "id": "123"}

    @pytest.fixture
    def mock_callback(self):
        """Create a mock async callback."""
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_init(self):
        """Test InprocPubSub initialization."""
        pubsub = InprocPubSub()
        assert pubsub._subscriptions == {}

    @pytest.mark.asyncio
    async def test_publish(self, pubsub, sample_msg, mock_callback):
        """Test publishing a message."""
        subject = "test.subject"
        pubsub.subscribe(subject, mock_callback)
        await pubsub.publish(subject, sample_msg)
        mock_callback.assert_awaited_once_with(sample_msg)

    def test_subscribe(self, pubsub, mock_callback):
        """Test subscribing to a subject."""
        subject = "test.subject"
        subscription_id = pubsub.subscribe(subject, mock_callback)
        
        assert subscription_id == hash(mock_callback)
        assert mock_callback in pubsub._subscriptions[subject]

    def test_subscribe_multiple_callbacks(self, pubsub):
        """Test subscribing multiple callbacks to the same subject."""
        subject = "test.subject"
        callback1 = AsyncMock()
        callback2 = AsyncMock()
        
        sub_id1 = pubsub.subscribe(subject, callback1)
        sub_id2 = pubsub.subscribe(subject, callback2)
        
        assert sub_id1 == hash(callback1)
        assert sub_id2 == hash(callback2)
        assert len(pubsub._subscriptions[subject]) == 2
        assert callback1 in pubsub._subscriptions[subject]
        assert callback2 in pubsub._subscriptions[subject]

    def test_unsubscribe_success(self, pubsub, mock_callback):
        """Test successful unsubscription."""
        subject = "test.subject"
        subscription_id = pubsub.subscribe(subject, mock_callback)
        
        result = pubsub.unsubscribe(subject, subscription_id)
        
        assert result is True
        assert mock_callback not in pubsub._subscriptions[subject]

    def test_unsubscribe_nonexistent_subject(self, pubsub):
        """Test unsubscription from non-existent subject."""
        result = pubsub.unsubscribe("nonexistent.subject", 123)
        assert result is False

    def test_unsubscribe_nonexistent_id(self, pubsub, mock_callback):
        """Test unsubscription with non-existent subscription ID."""
        subject = "test.subject"
        pubsub.subscribe(subject, mock_callback)
        
        result = pubsub.unsubscribe(subject, 99999)  # Non-existent ID
        
        assert result is False
        assert mock_callback in pubsub._subscriptions[subject]  # Should still be subscribed

    def test_get_subscriptions(self, pubsub, mock_callback):
        """Test getting subscriptions for a subject."""
        subject = "test.subject"
        pubsub.subscribe(subject, mock_callback)
        
        subscriptions = pubsub.get_subscriptions(subject)
        
        assert mock_callback in subscriptions
        assert len(subscriptions) == 1

    def test_get_subscriptions_empty(self, pubsub):
        """Test getting subscriptions for a subject with no subscriptions."""
        subject = "test.subject"
        subscriptions = pubsub.get_subscriptions(subject)
        assert subscriptions == []


class TestInprocReqReply:
    """Test cases for InprocReqReply class."""

    @pytest.fixture
    def reqreply(self):
        """Create an InprocReqReply instance."""
        return InprocReqReply()

    @pytest.fixture
    def sample_msg(self):
        """Create a sample message for testing."""
        return {"data": "test_message", "id": "123"}

    @pytest.fixture
    def mock_callback(self):
        """Create a mock async callback."""
        return AsyncMock(return_value={"response": "success"})

    def test_init(self, reqreply):
        """Test InprocReqReply initialization."""
        assert reqreply._callbacks == {}

    @pytest.mark.asyncio
    async def test_register_callback(self, reqreply, mock_callback):
        """Test registering a callback."""
        subject = "test.subject"
        result = await reqreply.register_callback(subject, mock_callback)
        
        assert result == subject
        assert reqreply._callbacks[subject] == mock_callback

    @pytest.mark.asyncio
    async def test_request_success(self, reqreply, mock_callback, sample_msg):
        """Test successful request."""
        subject = "test.subject"
        await reqreply.register_callback(subject, mock_callback)
        
        result = await reqreply.request(subject, sample_msg)
        
        mock_callback.assert_called_once_with(sample_msg)
        assert result == {"response": "success"}

    @pytest.mark.asyncio
    async def test_request_nonexistent_subject(self, reqreply, sample_msg):
        """Test request to non-existent subject."""
        subject = "nonexistent.subject"
        
        with pytest.raises(ValueError, match=f"No callback registered for {subject}"):
            await reqreply.request(subject, sample_msg)

    @pytest.mark.asyncio
    async def test_unregister_callback(self, reqreply, mock_callback):
        """Test unregistering a callback."""
        subject = "test.subject"
        await reqreply.register_callback(subject, mock_callback)
        
        assert subject in reqreply._callbacks
        
        await reqreply.unregister_callback(subject)
        
        assert subject not in reqreply._callbacks

    @pytest.mark.asyncio
    async def test_unregister_nonexistent_callback(self, reqreply):
        """Test unregistering a non-existent callback."""
        subject = "nonexistent.subject"
        # Should not raise an exception
        await reqreply.unregister_callback(subject)


class TestInprocPilotIntegration:
    """Integration tests for InprocPilot with PubSub and ReqReply."""

    @pytest_asyncio.fixture
    async def pilot(self):
        """Create and connect an InprocPilot instance."""
        pilot = InprocPilot()
        await pilot.connect()
        yield pilot
        await pilot.disconnect()

    @pytest.fixture
    def sample_msg(self):
        """Create a sample message for testing."""
        return {"data": "test_message", "id": "123"}

    @pytest.mark.asyncio
    async def test_pubsub_integration(self, pilot, sample_msg):
        """Test pub/sub functionality integration."""
        pubsub = pilot.pubsub()
        received_messages = []
        
        async def message_handler(msg):
            received_messages.append(msg)
        
        subject = "test.subject"
        subscription_id = pubsub.subscribe(subject, message_handler)
        
        await pubsub.publish(subject, sample_msg)
        
        # Give some time for async processing
        await asyncio.sleep(0.1)
        
        assert len(received_messages) == 1
        assert received_messages[0] == sample_msg
        
        # Test unsubscription
        success = pubsub.unsubscribe(subject, subscription_id)
        assert success is True

    @pytest.mark.asyncio
    async def test_reqreply_integration(self, pilot, sample_msg):
        """Test request/reply functionality integration."""
        reqreply = pilot.reqreply()
        response_data = {"response": "success", "processed": True}
        
        async def request_handler(msg):
            return response_data
        
        subject = "test.request"
        await reqreply.register_callback(subject, request_handler)
        
        result = await reqreply.request(subject, sample_msg)
        
        assert result == response_data
        
        # Test unregistration
        await reqreply.unregister_callback(subject)
        
        with pytest.raises(ValueError, match=f"No callback registered for {subject}"):
            await reqreply.request(subject, sample_msg)

    @pytest.mark.asyncio
    async def test_multiple_subscribers(self, pilot, sample_msg):
        """Test multiple subscribers to the same subject."""
        pubsub = pilot.pubsub()
        received_messages1 = []
        received_messages2 = []
        
        async def handler1(msg):
            received_messages1.append(msg)
        
        async def handler2(msg):
            received_messages2.append(msg)
        
        subject = "test.subject"
        pubsub.subscribe(subject, handler1)
        pubsub.subscribe(subject, handler2)
        
        await pubsub.publish(subject, sample_msg)
        
        # Give some time for async processing
        await asyncio.sleep(0.1)
        
        assert len(received_messages1) == 1
        assert len(received_messages2) == 1
        assert received_messages1[0] == sample_msg
        assert received_messages2[0] == sample_msg

