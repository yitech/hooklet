import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, List, Union, AsyncGenerator
from hooklet.base import Pilot
from hooklet.base.types import Headers, Msg
from hooklet.logger import get_logger
from aiocache import Cache


logger = get_logger(__name__)

class HeaderValidator:
    """
    A comprehensive header validator that handles various header operations
    including forwarding, validation, and processing.
    """
    
    # Standard header keys
    CORRELATION_ID = "Hooklet-Correlation-ID"
    TRACE_ID = "Hooklet-Trace-ID"

    # PubSub headers
    PUBSUB_FORWARD_TO = "Hooklet-Forward-To"

    # ReqReply headers
    REQ_REPLY_TIMEOUT = "Hooklet-Req-Reply-Timeout"

    def __init__(self, pilot: Pilot) -> None:
        self._pilot = pilot
        self._headers: Headers = {}
        self._forward_tasks: List[asyncio.Task] = []
        self._validation_errors: List[str] = []

        self._cache = Cache(method=Cache.METHOD_MEMORY, ttl=60)

    @asynccontextmanager
    async def validate(self, data: Msg, headers: Headers) -> AsyncGenerator["HeaderValidator", None]:
        self._headers = self._validate_headers(headers)
        yield self
        
    def _validate_headers(self, headers: Headers) -> Headers:
        """Validate all headers and collect any errors."""
        self._headers.clear()
        self._validation_errors.clear()

        if self.CORRELATION_ID in headers and not isinstance(headers[self.CORRELATION_ID], str):
            self._validation_errors.append(f"Correlation ID must be a string, got {type(headers[self.CORRELATION_ID])}")

        if self.TRACE_ID in headers and not isinstance(headers[self.TRACE_ID], str):
            self._validation_errors.append(f"Trace ID must be a string, got {type(headers[self.TRACE_ID])}")
        
        if self.PUBSUB_FORWARD_TO in headers and not isinstance(headers[self.PUBSUB_FORWARD_TO], str):
            self._validation_errors.append(f"Forward to must be a string, got {type(headers[self.PUBSUB_FORWARD_TO])}")
        
        if self.REQ_REPLY_TIMEOUT in headers and not isinstance(headers[self.REQ_REPLY_TIMEOUT], float):
            self._validation_errors.append(f"Request reply timeout must be a float, got {type(headers[self.REQ_REPLY_TIMEOUT])}")
        
        logger.warning(f"Validation errors: {self._validation_errors}")
        return headers
    
    async def is_duplicated(self) -> bool:
        """Check if the trace ID is duplicated."""
        if self.TRACE_ID in self._headers:
            if await self._cache.get(self._headers[self.TRACE_ID]):
                return True
            await self._cache.set(self._headers[self.TRACE_ID], True)
            return False
        return False
    
    async def forward_to(self, data: Msg, headers: Headers) -> None:
        """Forward the message to the specified topic."""
        if self.PUBSUB_FORWARD_TO in headers:
            await self._pilot.pubsub().publish(headers[self.PUBSUB_FORWARD_TO], data, headers)
    
        