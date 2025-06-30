import asyncio
import uuid
from collections import defaultdict
from typing import Any, Dict, Tuple, Callable, Awaitable
from hooklet.base import Pilot, PubSub, ReqReply, Msg
from hooklet.logger import get_logger

logger = get_logger(__name__)

class InprocPubSub(PubSub):
    def __init__(self, pilot: 'InprocPilot') -> None:
        self._pilot = pilot
        self._subscriptions: Dict[str, list[Callable[[Msg], Awaitable[Any]]]] = defaultdict(list)

    async def publish(self, subject: str, data: Msg) -> None:
        await self._pilot._handle_publish(subject, data)

    def subscribe(self, subject: str, callback: Callable[[Msg], Awaitable[Any]]) -> int:
        self._subscriptions[subject].append(callback)
        subscription_id = hash(callback)
        logger.info(f"Subscribed to {subject} with ID {subscription_id}")
        return subscription_id
    
    def unsubscribe(self, subject: str, subscription_id: int) -> bool:
        if subject in self._subscriptions:
            try:
                callback_to_remove = next(
                    callback for callback in self._subscriptions[subject] 
                    if hash(callback) == subscription_id
                )
                self._subscriptions[subject].remove(callback_to_remove)
                logger.info(f"Unsubscribed from {subject} with ID {subscription_id}")
                return True
            except StopIteration:
                pass
        return False
    
    def get_subscriptions(self, subject: str) -> list[Callable[[Msg], Awaitable[Any]]]:
        return self._subscriptions[subject]

class InprocReqReply(ReqReply):
    def __init__(self) -> None:
        self._callbacks: Dict[str, Callable[[Any], Awaitable[Any]]] = {}

    async def request(self, subject: str, data: Msg) -> Any:
        if subject not in self._callbacks:
            raise ValueError(f"No callback registered for {subject}")
        return await self._callbacks[subject](data)
    
    def register_callback(self, subject: str, callback: Callable[[Any], Awaitable[Any]]) -> str:
        self._callbacks[subject] = callback
        return subject
    
    def unregister_callback(self, subject: str) -> None:
        if subject in self._callbacks:
            del self._callbacks[subject]

class InprocPilot(Pilot):
    def __init__(self) -> None:
        super().__init__()
        self._connected = False
        self._pubsub = InprocPubSub(self)
        self._reqreply = InprocReqReply()

    def is_connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        self._connected = True
        logger.info("InProcPilot connected")

    async def disconnect(self) -> None:
        self._connected = False
        logger.info("InProcPilot disconnected")

    def pubsub(self) -> PubSub:
        return self._pubsub

    def reqreply(self) -> ReqReply:
        return self._reqreply
    
    async def _handle_publish(self, subject: str, data: Msg) -> None:
        try:
            subscriptions = self._pubsub.get_subscriptions(subject)
            tasks = [callback(data) for callback in subscriptions]
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

