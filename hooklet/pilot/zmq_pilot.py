import asyncio
import logging
import pickle
import uuid
from typing import Any, Dict, Optional

import zmq
from zmq.asyncio import Context, Socket

from hooklet.base import BasePilot
from hooklet.types import MessageHandlerCallback

logger = logging.getLogger(__name__)


class ZmqBroker:
    """
    ZeroMQ broker implementation using XPUB/XSUB pattern.
    Provides message distribution between publishers and subscribers.
    """

    def __init__(self, pub_address="tcp://*:5555", sub_address="tcp://*:5556"):
        """
        Initialize the ZMQ broker with configurable addresses.

        Args:
            pub_address: Address where publishers connect (XSUB socket)
            sub_address: Address where subscribers connect (XPUB socket)
        """
        self.pub_address = pub_address
        self.sub_address = sub_address
        self._context = zmq.asyncio.Context.instance()
        self._xsub_socket = None
        self._xpub_socket = None
        self._poller = None
        self._running = False
        self._broker_task = None

    async def connect(self):
        """
        Establish broker connections and start the message forwarding loop.
        """
        if self._running:
            logger.warning("Broker is already running")
            return

        self._xsub_socket = self._context.socket(zmq.XSUB)
        self._xsub_socket.bind(self.pub_address)
        logger.info(f"Publisher socket bound to {self.pub_address}")

        self._xpub_socket = self._context.socket(zmq.XPUB)
        self._xpub_socket.bind(self.sub_address)
        logger.info(f"Subscriber socket bound to {self.sub_address}")

        # Allow multicast subscriptions
        self._xpub_socket.setsockopt(zmq.XPUB_VERBOSE, 1)

        self._poller = zmq.asyncio.Poller()
        self._poller.register(self._xsub_socket, zmq.POLLIN)
        self._poller.register(self._xpub_socket, zmq.POLLIN)

        self._running = True
        self._broker_task = asyncio.create_task(self._broker_loop())
        logger.info("ZMQ Broker started")

    async def _broker_loop(self):
        """
        Main broker loop that forwards messages between publishers and subscribers.
        """
        try:
            while self._running:
                events = dict(await self._poller.poll(1000))  # Poll with timeout

                if self._xsub_socket in events:
                    message = await self._xsub_socket.recv_multipart()
                    await self._xpub_socket.send_multipart(message)

                if self._xpub_socket in events:
                    message = await self._xpub_socket.recv_multipart()
                    await self._xsub_socket.send_multipart(message)

        except asyncio.CancelledError:
            logger.info("Broker loop cancelled")
        except Exception as e:
            logger.error(f"Error in broker loop: {e}", exc_info=True)
            raise

    async def close(self):
        """
        Gracefully close broker connections.
        """
        if not self._running:
            logger.warning("Broker is not running")
            return

        self._running = False

        # Cancel the broker task
        if self._broker_task:
            self._broker_task.cancel()
            try:
                await self._broker_task
            except asyncio.CancelledError:
                pass

        # Close sockets
        if self._xpub_socket:
            self._xpub_socket.close()
            self._xpub_socket = None

        if self._xsub_socket:
            self._xsub_socket.close()
            self._xsub_socket = None

        logger.info("ZMQ Broker stopped")


class ZmqPilot(BasePilot):
    """
    ZeroMQ implementation of BasePilot using PUB/SUB with broker pattern.
    Requires separate broker running with XSUB/XPUB or SUB/PUB proxy.
    """

    def __init__(
        self, pub_address: str = "tcp://localhost:5555", sub_address: str = "tcp://localhost:5556"
    ):
        super().__init__()
        self.pub_address = pub_address
        self.sub_address = sub_address
        self._context: Optional[Context] = None
        self._pub_socket: Optional[Socket] = None
        self._sub_socket: Optional[Socket] = None
        self._connected = False
        self._handlers: Dict[str, Dict[str, MessageHandlerCallback]] = {}
        self._consumer_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        if self._connected:
            return

        self._context = Context.instance()
        self._pub_socket = self._context.socket(zmq.PUB)
        self._sub_socket = self._context.socket(zmq.SUB)

        # Remove the await keywords - these methods are not coroutines
        self._pub_socket.connect(self.pub_address)
        self._sub_socket.connect(self.sub_address)

        self._connected = True
        self._consumer_task = asyncio.create_task(self._consume_messages())
        logger.info("Connected to ZMQ broker")

    def is_connected(self) -> bool:
        return self._connected

    async def close(self) -> None:
        if not self._connected:
            return

        self._connected = False

        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                logger.debug("Consumer task cancelled")

        if self._pub_socket:
            self._pub_socket.close()
        if self._sub_socket:
            self._sub_socket.close()

        self._handlers.clear()
        logger.info("Disconnected from ZMQ broker")

    async def register_handler(
        self, subject: str, handler: MessageHandlerCallback, handler_id: Optional[str] = None
    ) -> str:
        if not handler_id:
            handler_id = str(uuid.uuid4())

        if subject not in self._handlers:
            self._handlers[subject] = {}
            # Add ZMQ subscription
            self._sub_socket.setsockopt_string(zmq.SUBSCRIBE, subject)

        if handler_id in self._handlers[subject]:
            raise ValueError(f"Handler ID {handler_id} already exists for subject {subject}")

        self._handlers[subject][handler_id] = handler
        logger.debug(f"Registered handler {handler_id} for subject {subject}")
        return handler_id

    async def unregister_handler(self, handler_id: str) -> bool:
        removed = False
        for subject in list(self._handlers.keys()):
            if handler_id in self._handlers.get(subject, {}):
                del self._handlers[subject][handler_id]
                removed = True

                # Remove subject if no more handlers
                if not self._handlers[subject]:
                    del self._handlers[subject]
                    # Remove ZMQ subscription
                    self._sub_socket.setsockopt_string(zmq.UNSUBSCRIBE, subject)

        if not removed:
            logger.warning(f"Handler {handler_id} not found")
        else:
            logger.debug(f"Unregistered handler {handler_id}")

        return removed

    async def publish(self, subject: str, data: Any) -> None:
        if not self._connected:
            await self.connect()

        try:
            serialized = pickle.dumps(data)
            # Remove the await keyword - send_multipart is not a coroutine
            self._pub_socket.send_multipart([subject.encode(), serialized])
            logger.debug(f"Published to {subject}")
        except Exception as e:
            logger.error(f"Publish error: {e}")
            raise

    async def _consume_messages(self) -> None:
        while self._connected:
            try:
                topic, message = await self._sub_socket.recv_multipart()
                subject = topic.decode()
                data = pickle.loads(message)

                if subject in self._handlers:
                    for handler_id, handler in self._handlers[subject].items():
                        try:
                            await handler(data)
                        except Exception as e:
                            logger.error(f"Handler {handler_id} error: {e}", exc_info=True)
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._connected:
                    logger.error(f"Message consumption error: {e}", exc_info=True)
                break
