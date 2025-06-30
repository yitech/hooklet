import asyncio
import uuid
from collections import defaultdict
from typing import Any, Dict, Tuple
from hooklet.base import Pilot, PubSub, ReqReply, Callback, AsyncCallback, Headers, Msg
from hooklet.logger import get_logger
from hooklet.utils import HeaderValidator

logger = get_logger(__name__)

class InprocPubSub(PubSub):
    def __init__(self, pilot: 'InprocPilot') -> None:
        self._pilot = pilot
        self._validator = HeaderValidator(pilot)
        # Data processing queue
        self._queue: asyncio.Queue[Tuple[str, Msg]] = asyncio.Queue()
        self._subscriptions: Dict[str, Dict[str, AsyncCallback]] = defaultdict(dict)
        self._consumer_task: asyncio.Task[None] = asyncio.create_task(self._consume())
        self._shutdown_event = asyncio.Event()

    async def publish(self, subject: str, data: Msg, headers: Headers = {}) -> None:
        await self._pilot._handle_publish(subject, data, headers)

    def subscribe(self, subject: str, callback: AsyncCallback) -> str:
        subscription_id = str(uuid.uuid4())
        self._subscriptions[subject][subscription_id] = callback
        logger.info(f"Subscribed to {subject} with ID {subscription_id}")
        return subscription_id
    
    def unsubscribe(self, subject: str, subscription_id: str) -> bool:
        if subject in self._subscriptions and subscription_id in self._subscriptions[subject]:
            del self._subscriptions[subject][subscription_id]
            logger.info(f"Unsubscribed from {subject} with ID {subscription_id}")
            return True
        return False
    
    def _get_subscriptions(self, subject: str) -> Dict[str, AsyncCallback]:
        return self._subscriptions[subject]

class InprocReqReply(ReqReply):
    def __init__(self) -> None:
        self._callbacks: Dict[str, AsyncCallback] = {}

    async def request(self, subject: str, data: dict[str, Any]) -> Any:
        if subject not in self._callbacks:
            raise ValueError(f"No callback registered for {subject}")
        return await self._callbacks[subject](**data)
    
    def register_callback(self, subject: str, callback: AsyncCallback) -> str:
        self._callbacks[subject] = callback
        return subject

class InprocPilot(Pilot):
    def __init__(self) -> None:
        super().__init__()
        self._connected = False
        self._pubsub = InprocPubSub(self)
        self._reqreply = InprocReqReply(self)

    def is_connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        self._connected = True
        self._pubsub._shutdown_event.clear()
        logger.info("InProcPilot connected")

    async def disconnect(self) -> None:
        self._connected = False
        self._pubsub._shutdown_event.set()
        logger.info("InProcPilot disconnected")

    def pubsub(self) -> PubSub:
        return self._pubsub

    def reqreply(self) -> ReqReply:
        return self._reqreply
    
    async def _handle_publish(self, subject: str, data: Msg) -> None:
        try:
            subscriptions = self._pubsub._get_subscriptions(subject)
            tasks = [callback(data) for _, callback in subscriptions.items()]
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Error publishing to {subject}: {e}", exc_info=True)
            raise

    async def _handle_request(self, subject: str, data: Msg) -> Msg:
        try:
            return await self._reqreply.request(subject, data)
        except Exception as e:
            logger.error(f"Error requesting from {subject}: {e}", exc_info=True)
            raise

