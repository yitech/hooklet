import asyncio
import logging
import uuid
from typing import Any, Dict

from hooklet.base import BasePilot
from hooklet.types import MessageHandlerCallback

logger = logging.getLogger(__name__)


class InProcPilot(BasePilot):
    def __init__(self) -> None:
        super().__init__()
        self._queue: asyncio.Queue = asyncio.Queue()
        self._connected: bool = False
        self._handlers: Dict[str, Dict[str, Any]] = {}
        self._consumer_task: asyncio.Task | None = None

    async def connect(self) -> None:
        if self._connected:
            return
        self._connected = True
        self._consumer_task = asyncio.create_task(self._consume_messages())
        logger.info("InProcPilot connected")

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
            self._consumer_task = None
        # Clear the queue
        while not self._queue.empty():
            self._queue.get_nowait()
            self._queue.task_done()
        self._handlers.clear()
        logger.info("InProcPilot closed")

    async def register_handler(
        self, subject: str, handler: MessageHandlerCallback, handler_id: str | None = None
    ) -> str:
        if handler_id is None:
            handler_id = str(uuid.uuid4())
        else:
            for existing_handlers in self._handlers.values():
                if handler_id in existing_handlers:
                    raise ValueError(f"Handler ID {handler_id} already exists")
        if subject not in self._handlers:
            self._handlers[subject] = {}
        self._handlers[subject][handler_id] = handler
        logger.info(f"Registered handler {handler_id} for subject {subject}")
        return handler_id

    async def unregister_handler(self, handler_id: str) -> bool:
        removed = False
        for subject in list(self._handlers.keys()):
            if handler_id in self._handlers.get(subject, {}):
                del self._handlers[subject][handler_id]
                removed = True
                if not self._handlers[subject]:
                    del self._handlers[subject]
        if removed:
            logger.info(f"Unregistered handler {handler_id}")
        else:
            logger.warning(f"Handler {handler_id} not found for unregistration")
        return removed

    async def publish(self, subject: str, data: Any) -> None:
        if not self.is_connected():
            await self.connect()
        await self._queue.put((subject, data))
        logger.debug(f"Published to subject {subject} with data {data}")

    async def _consume_messages(self) -> None:
        try:
            while True:
                subject, data = await self._queue.get()
                if subject in self._handlers:
                    handlers = list(self._handlers[subject].items())
                    for handler_id, handler in handlers:
                        try:
                            await handler(data)
                        except Exception as e:
                            logger.error(
                                f"Error in handler {handler_id} for subject {subject}: {e}"
                            )
                self._queue.task_done()
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled, stopping")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in consumer task: {e}")
