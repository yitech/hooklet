import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, List, Union, AsyncGenerator
from hooklet.base import Pilot
from hooklet.base.types import Headers, Msg
from hooklet.logger import get_logger

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
    REQ_REPLY_RETRY = "Hooklet-Req-Reply-Retry"
    REQ_REPLY_DELAY = "Hooklet-Req-Reply-Delay"
    

    def __init__(self, pilot: Pilot) -> None:
        self._pilot = pilot
        self._headers: Headers = {}
        self._forward_tasks: List[asyncio.Task] = []
        self._validation_errors: List[str] = []

    @asynccontextmanager
    async def validate(self, headers: Headers) -> AsyncGenerator["HeaderValidator", None]:
        self._headers = await self._validate_headers(headers)
        yield self
        
    async def _validate_headers(self, headers: Headers) -> Headers:
        """Validate all headers and collect any errors."""
        self._headers.clear()
        self._validation_errors.clear()
        
        if self.PUBSUB_FORWARD_TO in headers:
            if isinstance(headers[self.PUBSUB_FORWARD_TO], str):
                self._headers[self.PUBSUB_FORWARD_TO] = [target.strip() for target in headers[self.PUBSUB_FORWARD_TO].split(',') if target.strip()]
            else:
                self._headers[self.PUBSUB_FORWARD_TO] = headers[self.PUBSUB_FORWARD_TO]
        if self.REQ_REPLY_TIMEOUT in headers:
            self._headers[self.REQ_REPLY_TIMEOUT] = headers[self.REQ_REPLY_TIMEOUT]
        if self.REQ_REPLY_RETRY in headers:
            self._headers[self.REQ_REPLY_RETRY] = headers[self.REQ_REPLY_RETRY]
        if self.REQ_REPLY_DELAY in headers:
            self._headers[self.REQ_REPLY_DELAY] = headers[self.REQ_REPLY_DELAY]
        if self.REQ_REPLY_MODE in headers:
            self._headers[self.REQ_REPLY_MODE] = headers[self.REQ_REPLY_MODE]
        if self.REQ_REPLY_VALIDATION_RULES in headers:
            self._headers[self.REQ_REPLY_VALIDATION_RULES] = headers[self.REQ_REPLY_VALIDATION_RULES]
        if self.PROCESSING_MODE in headers:
            self._headers[self.PROCESSING_MODE] = headers[self.PROCESSING_MODE]
        if self.CORRELATION_ID in headers:
            self._headers[self.CORRELATION_ID] = headers[self.CORRELATION_ID]
        if self.TRACE_ID in headers:
            self._headers[self.TRACE_ID] = headers[self.TRACE_ID]
        
        return self._headers

            
    def get_forward_targets(self) -> List[str]:
        """Get list of forward targets from headers."""
        if self.FORWARD_TO not in self._headers:
            return []
            
        forward_to = self._headers[self.FORWARD_TO]
        if isinstance(forward_to, str):
            # Support comma-separated multiple targets
            return [target.strip() for target in forward_to.split(',') if target.strip()]
        return []
        
    def get_forward_delay(self) -> float:
        """Get forward delay in seconds."""
        return float(self._headers.get(self.FORWARD_DELAY, 0))
        
    def get_forward_retry_count(self) -> int:
        """Get forward retry count."""
        return int(self._headers.get(self.FORWARD_RETRY, 0))
        
    def get_forward_timeout(self) -> Optional[float]:
        """Get forward timeout in seconds."""
        if self.FORWARD_TIMEOUT in self._headers:
            return float(self._headers[self.FORWARD_TIMEOUT])
        return None
        
    def get_processing_mode(self) -> str:
        """Get processing mode."""
        return self._headers.get(self.PROCESSING_MODE, "sync")
        
    def get_correlation_id(self) -> Optional[str]:
        """Get correlation ID for tracing."""
        return self._headers.get(self.CORRELATION_ID)
        
    def get_trace_id(self) -> Optional[str]:
        """Get trace ID for tracing."""
        return self._headers.get(self.TRACE_ID)
        
    def has_forward_headers(self) -> bool:
        """Check if headers contain forwarding information."""
        return any(key in self._headers for key in [self.FORWARD_TO, self.FORWARD_DELAY, self.FORWARD_RETRY])
        
    def is_async_processing(self) -> bool:
        """Check if processing should be asynchronous."""
        return self.get_processing_mode().lower() in ["async", "background"]
        
    async def process_forwarding(self, data: Msg, pilot) -> None:
        """Process forwarding logic based on headers."""
        if not self.has_forward_headers() or not pilot:
            return
            
        forward_targets = self.get_forward_targets()
        if not forward_targets:
            return
            
        delay = self.get_forward_delay()
        retry_count = self.get_forward_retry_count()
        timeout = self.get_forward_timeout()
        
        # Create forward task
        forward_task = asyncio.create_task(
            self._forward_with_retry(
                pilot, forward_targets, data, self._headers, 
                delay, retry_count, timeout
            )
        )
        self._forward_tasks.append(forward_task)
        
        # If async processing, don't wait for forward task
        if self.is_async_processing():
            logger.info(f"Queued async forward to {forward_targets}")
        else:
            # Wait for forward task to complete
            await forward_task
            
    async def _forward_with_retry(
        self, pilot, targets: List[str], data: Msg, headers: Headers,
        delay: float, retry_count: int, timeout: Optional[float]
    ) -> None:
        """Forward data to targets with retry logic."""
        if delay > 0:
            await asyncio.sleep(delay)
            
        for attempt in range(retry_count + 1):
            try:
                for target in targets:
                    if timeout:
                        await asyncio.wait_for(
                            pilot.pubsub().publish(target, data, headers),
                            timeout=timeout
                        )
                    else:
                        await pilot.pubsub().publish(target, data, headers)
                        
                logger.info(f"Successfully forwarded to {targets}")
                return
                
            except Exception as e:
                if attempt < retry_count:
                    logger.warning(f"Forward attempt {attempt + 1} failed: {e}, retrying...")
                    await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
                else:
                    logger.error(f"Failed to forward to {targets} after {retry_count + 1} attempts: {e}")
                    raise
                    
    async def _cleanup_forward_tasks(self) -> None:
        """Clean up any pending forward tasks."""
        if self._forward_tasks:
            # Cancel any pending tasks
            for task in self._forward_tasks:
                if not task.done():
                    task.cancel()
                    
            # Wait for all tasks to complete
            await asyncio.gather(*self._forward_tasks, return_exceptions=True)
            self._forward_tasks.clear()
            
    def get_validation_errors(self) -> List[str]:
        """Get list of validation errors."""
        return self._validation_errors.copy()
        
    def is_valid(self) -> bool:
        """Check if headers are valid."""
        return len(self._validation_errors) == 0
        
    def add_header(self, key: str, value: Any) -> None:
        """Add a header."""
        self._headers[key] = value
        
    def remove_header(self, key: str) -> None:
        """Remove a header."""
        self._headers.pop(key, None)
        
    def get_header(self, key: str, default: Any = None) -> Any:
        """Get a header value."""
        return self._headers.get(key, default)
        
    def get_all_headers(self) -> Headers:
        """Get all headers."""
        return self._headers.copy()


class HeaderBuilder:
    """Builder class for creating headers with common patterns."""
    
    @staticmethod
    def forward_to(targets: Union[str, List[str]], delay: float = 0, retry: int = 0, timeout: Optional[float] = None) -> Headers:
        """Create headers for forwarding."""
        if isinstance(targets, str):
            targets = [targets]
            
        headers = {
            HeaderValidator.FORWARD_TO: ",".join(targets),
            HeaderValidator.FORWARD_DELAY: str(delay),
            HeaderValidator.FORWARD_RETRY: str(retry)
        }
        
        if timeout is not None:
            headers[HeaderValidator.FORWARD_TIMEOUT] = str(timeout)
            
        return headers
        
    @staticmethod
    def async_processing() -> Headers:
        """Create headers for async processing."""
        return {HeaderValidator.PROCESSING_MODE: "async"}
        
    @staticmethod
    def with_correlation(correlation_id: str, trace_id: Optional[str] = None) -> Headers:
        """Create headers with correlation and trace IDs."""
        headers = {HeaderValidator.CORRELATION_ID: correlation_id}
        if trace_id:
            headers[HeaderValidator.TRACE_ID] = trace_id
        return headers
        
    @staticmethod
    def with_validation_rules(rules: Dict[str, Any]) -> Headers:
        """Create headers with validation rules."""
        return {HeaderValidator.VALIDATION_RULES: rules} 