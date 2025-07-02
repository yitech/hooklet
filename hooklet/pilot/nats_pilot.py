from typing import Any, Awaitable, Callable, Dict

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg as NatsMsg
from nats.aio.subscription import Subscription

from hooklet.base import Job, Msg, Pilot, PubSub, PushPull, ReqReply
from hooklet.logger import get_logger

logger = get_logger(__name__)


class NatsPubSub(PubSub):
    def __init__(self, pilot: "NatsPilot") -> None:
        self._pilot = pilot
        self._subscriptions: Dict[str, list[Callable[[Msg], Awaitable[Any]]]] = {}
        self._nats_subscriptions: Dict[str, list[Subscription]] = {}

    async def publish(self, subject: str, data: Msg) -> None:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")
        import json

        payload = json.dumps(data).encode()
        await self._pilot._nats_client.publish(subject, payload)
        logger.debug(f"Published to {subject}: {data}")

    async def subscribe(self, subject: str, callback: Callable[[Msg], Awaitable[Any]]) -> int:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")
        subscription_id = hash(callback)

        async def nats_callback(msg: NatsMsg):
            try:
                import json

                data = json.loads(msg.data.decode())
                await callback(data)
            except Exception as e:
                logger.error(
                    f"Error in NATS subscription callback for {subject}: {e}", exc_info=True
                )

        sub = await self._pilot._nats_client.subscribe(subject, cb=nats_callback)
        if subject not in self._subscriptions:
            self._subscriptions[subject] = []
            self._nats_subscriptions[subject] = []
        self._subscriptions[subject].append(callback)
        self._nats_subscriptions[subject].append(sub)
        logger.info(f"Subscribed to {subject} with ID {subscription_id}")
        return subscription_id

    async def unsubscribe(self, subject: str, subscription_id: int) -> bool:
        if subject not in self._subscriptions:
            return False
        try:
            callback_to_remove = next(
                callback
                for callback in self._subscriptions[subject]
                if hash(callback) == subscription_id
            )
            index = self._subscriptions[subject].index(callback_to_remove)
            self._subscriptions[subject].pop(index)
            nats_sub = self._nats_subscriptions[subject].pop(index)
            await nats_sub.unsubscribe()
            if not self._subscriptions[subject]:
                del self._subscriptions[subject]
                del self._nats_subscriptions[subject]
            logger.info(f"Unsubscribed from {subject} with ID {subscription_id}")
            return True
        except (StopIteration, ValueError):
            return False

    async def _cleanup(self) -> None:
        for subject in list(self._subscriptions.keys()):
            for nats_sub in self._nats_subscriptions[subject]:
                await nats_sub.unsubscribe()
            self._subscriptions[subject].clear()
            self._nats_subscriptions[subject].clear()
        self._subscriptions.clear()
        self._nats_subscriptions.clear()


class NatsReqReply(ReqReply):
    def __init__(self, pilot: "NatsPilot") -> None:
        self._pilot = pilot
        self._callbacks: Dict[str, Callable[[Any], Awaitable[Any]]] = {}
        self._nats_subscriptions: Dict[str, Subscription] = {}

    async def request(self, subject: str, data: Msg) -> Any:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")
        import json

        payload = json.dumps(data).encode()
        try:
            response = await self._pilot._nats_client.request(subject, payload, timeout=30.0)
            return json.loads(response.data.decode())
        except Exception as e:
            logger.error(f"Error making request to {subject}: {e}", exc_info=True)
            raise

    async def register_callback(
        self, subject: str, callback: Callable[[Any], Awaitable[Any]]
    ) -> str:
        if not self._pilot.is_connected():
            raise RuntimeError("NATS client not connected")

        async def nats_callback(msg: NatsMsg):
            try:
                import json

                data = json.loads(msg.data.decode())
                response = await callback(data)
                response_payload = json.dumps(response).encode()
                await msg.respond(response_payload)
            except Exception as e:
                logger.error(f"Error in NATS request callback for {subject}: {e}", exc_info=True)
                error_response = {"error": str(e)}
                await msg.respond(json.dumps(error_response).encode())

        sub = await self._pilot._nats_client.subscribe(subject, cb=nats_callback)
        self._callbacks[subject] = callback
        self._nats_subscriptions[subject] = sub
        logger.info(f"Registered callback for {subject}")
        return subject

    async def unregister_callback(self, subject: str) -> None:
        if subject in self._callbacks:
            if subject in self._nats_subscriptions:
                sub = self._nats_subscriptions[subject]
                await sub.unsubscribe()
                del self._nats_subscriptions[subject]
            del self._callbacks[subject]
            logger.info(f"Unregistered callback for {subject}")

    async def _cleanup(self) -> None:
        for subject in list(self._callbacks.keys()):
            await self.unregister_callback(subject)


class NatsPushPull(PushPull):
    def __init__(self, pilot: "NatsPilot") -> None:
        self._pilot = pilot
        self._workers: Dict[str, list[Callable[[Job], Awaitable[Any]]]] = {}

    async def push(self, subject: str, job: Job) -> bool:
        raise NotImplementedError("NatsPushPull.push is not implemented")

    async def register_worker(self, subject: str, callback: Callable[[Job], Awaitable[Any]]) -> int:
        raise NotImplementedError("NatsPushPull.register_worker is not implemented")

    async def subscribe(self, subject: str, callback: Callable[[Job], Awaitable[Any]]) -> int:
        raise NotImplementedError("NatsPushPull.subscribe is not implemented")

    async def unsubscribe(self, subject: str, subscription_id: int) -> bool:
        raise NotImplementedError("NatsPushPull.unsubscribe is not implemented")


class NatsPilot(Pilot):
    def __init__(self, nats_url: str = "nats://localhost:4222", **kwargs) -> None:
        super().__init__()
        self._nats_url = nats_url
        self._nats_client = NATS()
        self._connected = False
        self._pubsub = NatsPubSub(self)
        self._reqreply = NatsReqReply(self)
        self._kwargs = kwargs

    def is_connected(self) -> bool:
        return self._connected and self._nats_client.is_connected

    async def connect(self) -> None:
        try:
            await self._nats_client.connect(self._nats_url, **self._kwargs)
            self._connected = True
            logger.info(f"NatsPilot connected to {self._nats_url}")
        except Exception as e:
            logger.error(f"Failed to connect to NATS at {self._nats_url}: {e}", exc_info=True)
            raise

    async def disconnect(self) -> None:
        try:
            await self._pubsub._cleanup()
            await self._reqreply._cleanup()
            await self._nats_client.close()
            self._connected = False
            logger.info("NatsPilot disconnected")
        except Exception as e:
            logger.error(f"Error disconnecting from NATS: {e}", exc_info=True)
            raise

    def pubsub(self) -> NatsPubSub:
        return self._pubsub

    def reqreply(self) -> NatsReqReply:
        return self._reqreply

    def pushpull(self) -> NatsPushPull:
        return self._pushpull
