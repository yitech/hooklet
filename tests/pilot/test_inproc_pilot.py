import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from hooklet.pilot.inproc_pilot import InprocPilot, InprocPubSub, InprocReqReply, SimplePushPull, InprocPushPull
from hooklet.base.types import Msg, Job


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
        
        assert subscription_id == id(mock_callback)
        assert mock_callback in pubsub._subscriptions[subject]

    def test_subscribe_multiple_callbacks(self, pubsub):
        """Test subscribing multiple callbacks to the same subject."""
        subject = "test.subject"
        callback1 = AsyncMock()
        callback2 = AsyncMock()
        
        sub_id1 = pubsub.subscribe(subject, callback1)
        sub_id2 = pubsub.subscribe(subject, callback2)
        
        assert sub_id1 == id(callback1)
        assert sub_id2 == id(callback2)
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

    @pytest.mark.asyncio
    async def test_publish_with_failing_subscription(self, pubsub, sample_msg):
        """Test that publish continues even when a subscription fails."""
        subject = "test.subject"
        successful_callback = AsyncMock()
        failing_callback = AsyncMock(side_effect=Exception("Test error"))
        
        pubsub.subscribe(subject, successful_callback)
        pubsub.subscribe(subject, failing_callback)
        
        # Publish should not raise an exception
        await pubsub.publish(subject, sample_msg)
        
        # Successful callback should still be called
        successful_callback.assert_awaited_once_with(sample_msg)
        failing_callback.assert_awaited_once_with(sample_msg)


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


class MockPilot:
    def __init__(self):
        self.queues = {}
    def get_job_queue(self, subject):
        if subject not in self.queues:
            self.queues[subject] = asyncio.Queue(maxsize=2)
        return self.queues[subject]

class TestSimplePushPull:
    @pytest.mark.asyncio
    async def test_push_success(self):
        pilot = MockPilot()
        spp = SimplePushPull('test', pilot)
        job = {'_id': '1'}
        result = await spp.push(job)
        assert result is True
        assert job['status'] == 'new'
        assert not spp._job_queue.empty()

    @pytest.mark.asyncio
    async def test_push_queue_full(self):
        pilot = MockPilot()
        spp = SimplePushPull('test', pilot)
        for i in range(2):
            await spp.push({'_id': str(i)})
        result = await spp.push({'_id': 'full'})
        assert result is False

    @pytest.mark.asyncio
    async def test_subscribe_and_unsubscribe(self):
        pilot = MockPilot()
        spp = SimplePushPull('test', pilot)
        cb = AsyncMock()
        await spp.subscribe(cb)
        assert cb in spp._subscriptions
        sid = id(cb)
        result = await spp.unsubscribe(sid)
        assert result is True
        assert cb not in spp._subscriptions
        # Unsubscribe non-existent
        result2 = await spp.unsubscribe(99999)
        assert result2 is False

    @pytest.mark.asyncio
    async def test_worker_loop_success(self):
        pilot = MockPilot()
        spp = SimplePushPull('test', pilot)
        job = {'_id': 'w1'}
        processed = []
        async def worker(job):
            processed.append(job['_id'])
        await spp.register_worker(worker, 1)
        await spp.push(job)
        await asyncio.sleep(0.05)
        assert 'w1' in processed
        await spp._cleanup()

    @pytest.mark.asyncio
    async def test_worker_loop_error_and_cancel(self):
        pilot = MockPilot()
        spp = SimplePushPull('test', pilot)
        job = {'_id': 'err'}
        async def worker(job):
            raise Exception('fail')
        await spp.register_worker(worker, 1)
        await spp.push(job)
        await asyncio.sleep(0.05)
        # Should mark job as failed
        assert job['status'] == 'failed'
        assert job['error'] == 'fail'
        # Test cancellation
        await spp._cleanup()  # Should cancel the worker loop cleanly

    @pytest.mark.asyncio
    async def test_notify_subscriptions_error(self):
        pilot = MockPilot()
        spp = SimplePushPull('test', pilot)
        job = {'_id': 'notify'}
        called = []
        async def good_cb(job):
            called.append('good')
        async def bad_cb(job):
            raise Exception('bad')
        await spp.subscribe(good_cb)
        await spp.subscribe(bad_cb)
        await spp._notify_subscriptions(job)
        assert 'good' in called

    @pytest.mark.asyncio
    async def test_cleanup(self):
        pilot = MockPilot()
        spp = SimplePushPull('test', pilot)
        await spp.register_worker(lambda x: None, 2)
        await spp.push({'_id': 'test'})
        await asyncio.sleep(0.05)
        await spp._cleanup()
        assert spp._worker_loops == []


class TestInprocPushPull:
    @pytest.fixture
    def mock_pilot(self):
        pilot = MagicMock()
        pilot.get_job_queue.return_value = asyncio.Queue(maxsize=10)
        return pilot

    @pytest.fixture
    def inproc_pushpull(self, mock_pilot):
        return InprocPushPull(mock_pilot)

    @pytest.mark.asyncio
    async def test_init(self, inproc_pushpull):
        """Test InprocPushPull initialization."""
        assert inproc_pushpull._pushpulls == {}
        assert inproc_pushpull._pilot is not None
        await inproc_pushpull._cleanup()

    @pytest.mark.asyncio
    async def test_push_creates_simplepushpull(self, inproc_pushpull):
        """Test that push creates SimplePushPull for new subject."""
        subject = "test.subject"
        job = {"_id": "test_job"}
        
        result = await inproc_pushpull.push(subject, job)
        
        assert result is True
        assert subject in inproc_pushpull._pushpulls
        assert isinstance(inproc_pushpull._pushpulls[subject], SimplePushPull)
        await inproc_pushpull._cleanup()

    @pytest.mark.asyncio
    async def test_push_multiple_subjects(self, inproc_pushpull):
        """Test pushing to multiple subjects creates separate SimplePushPull instances."""
        job1 = {"_id": "job1"}
        job2 = {"_id": "job2"}
        
        await inproc_pushpull.push("subject1", job1)
        await inproc_pushpull.push("subject2", job2)
        
        assert "subject1" in inproc_pushpull._pushpulls
        assert "subject2" in inproc_pushpull._pushpulls
        assert inproc_pushpull._pushpulls["subject1"] is not inproc_pushpull._pushpulls["subject2"]

    @pytest.mark.asyncio
    async def test_register_worker_creates_simplepushpull(self, inproc_pushpull):
        """Test that register_worker creates SimplePushPull for new subject."""
        subject = "test.subject"
        callback = AsyncMock()
        
        await inproc_pushpull.register_worker(subject, callback, 2)
        
        assert subject in inproc_pushpull._pushpulls
        simple_pushpull = inproc_pushpull._pushpulls[subject]
        assert len(simple_pushpull._worker_loops) == 2
        await inproc_pushpull._cleanup()

    @pytest.mark.asyncio
    async def test_subscribe_creates_simplepushpull(self, inproc_pushpull):
        """Test that subscribe creates SimplePushPull for new subject."""
        subject = "test.subject"
        callback = AsyncMock()
        
        await inproc_pushpull.subscribe(subject, callback)
        
        assert subject in inproc_pushpull._pushpulls
        simple_pushpull = inproc_pushpull._pushpulls[subject]
        assert callback in simple_pushpull._subscriptions

    @pytest.mark.asyncio
    async def test_unsubscribe_existing_subject(self, inproc_pushpull):
        """Test unsubscribe on existing subject."""
        subject = "test.subject"
        callback = AsyncMock()
        
        # First subscribe to create the SimplePushPull
        await inproc_pushpull.subscribe(subject, callback)
        subscription_id = id(callback)
        
        # Then unsubscribe
        result = await inproc_pushpull.unsubscribe(subject, subscription_id)
        
        assert result is True
        simple_pushpull = inproc_pushpull._pushpulls[subject]
        assert callback not in simple_pushpull._subscriptions

    @pytest.mark.asyncio
    async def test_unsubscribe_nonexistent_subject(self, inproc_pushpull):
        """Test unsubscribe on non-existent subject."""
        result = await inproc_pushpull.unsubscribe("nonexistent", 123)
        assert result is False

    @pytest.mark.asyncio
    async def test_unsubscribe_nonexistent_subscription(self, inproc_pushpull):
        """Test unsubscribe with non-existent subscription ID."""
        subject = "test.subject"
        callback = AsyncMock()
        
        # Subscribe to create the SimplePushPull
        await inproc_pushpull.subscribe(subject, callback)
        
        # Try to unsubscribe with wrong ID
        result = await inproc_pushpull.unsubscribe(subject, 99999)
        assert result is False

    @pytest.mark.asyncio
    async def test_cleanup_multiple_subjects(self, inproc_pushpull):
        """Test cleanup with multiple subjects."""
        # Create multiple subjects
        await inproc_pushpull.push("subject1", {"_id": "job1"})
        await inproc_pushpull.push("subject2", {"_id": "job2"})
        
        assert len(inproc_pushpull._pushpulls) == 2
        
        # Cleanup
        await inproc_pushpull._cleanup()
        
        assert len(inproc_pushpull._pushpulls) == 0

    @pytest.mark.asyncio
    async def test_cleanup_empty(self, inproc_pushpull):
        """Test cleanup when no subjects exist."""
        assert len(inproc_pushpull._pushpulls) == 0
        await inproc_pushpull._cleanup()
        assert len(inproc_pushpull._pushpulls) == 0

    @pytest.mark.asyncio
    async def test_reuse_existing_simplepushpull(self, inproc_pushpull):
        """Test that operations reuse existing SimplePushPull for same subject."""
        subject = "test.subject"
        job = {"_id": "test_job"}
        callback = AsyncMock()
        
        # Push first to create SimplePushPull
        await inproc_pushpull.push(subject, job)
        simple_pushpull1 = inproc_pushpull._pushpulls[subject]
        
        # Register worker - should reuse same SimplePushPull
        await inproc_pushpull.register_worker(subject, callback, 1)
        simple_pushpull2 = inproc_pushpull._pushpulls[subject]
        
        # Subscribe - should reuse same SimplePushPull
        await inproc_pushpull.subscribe(subject, callback)
        simple_pushpull3 = inproc_pushpull._pushpulls[subject]
        
        # All should be the same instance
        assert simple_pushpull1 is simple_pushpull2
        assert simple_pushpull2 is simple_pushpull3
        assert len(inproc_pushpull._pushpulls) == 1
        await inproc_pushpull._cleanup()

    @pytest.mark.asyncio
    async def test_push_through_inproc_to_simple(self, inproc_pushpull):
        """Test that push through InprocPushPull correctly delegates to SimplePushPull."""
        subject = "test.subject"
        job = {"_id": "test_job", "status": "pending"}
        
        result = await inproc_pushpull.push(subject, job)
        
        assert result is True
        assert job["status"] == "new"  # Should be updated by SimplePushPull
        assert "recv_ms" in job  # Should be added by SimplePushPull