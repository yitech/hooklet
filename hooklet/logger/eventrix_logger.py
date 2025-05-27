"""
Eventrix Logger Module

Extends the Hooklet logger with executor-specific context.
Each Eventrix instance gets its own logger with executor_id included in all messages.
"""

import logging
from typing import Optional

from .hooklet_logger import HookletLogger, HookletLoggerConfig


class EventrixLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds executor_id context to all log messages."""

    def __init__(self, logger: logging.Logger, executor_id: str):
        """Initialize the adapter with executor context.

        Args:
            logger: Base logger instance
            executor_id: Unique identifier for the executor
        """
        super().__init__(logger, {"executor_id": executor_id})
        self.executor_id = executor_id

    def process(self, msg: str, kwargs: dict) -> tuple[str, dict]:
        """Process the log message to include executor context.

        Args:
            msg: The log message
            kwargs: Additional logging arguments

        Returns:
            Tuple of (modified message, modified kwargs)
        """
        # Ensure extra dict exists
        kwargs["extra"] = kwargs.get("extra", {})

        # Add executor_id to extra fields
        kwargs["extra"]["executor_id"] = self.executor_id

        # Add executor context to message for non-JSON formats
        return f" {msg}", kwargs


def get_eventrix_logger(
    executor_id: str, config: Optional[HookletLoggerConfig] = None
) -> logging.LoggerAdapter:
    """Get a logger for an Eventrix instance with executor_id context.

    Args:
        executor_id: Unique identifier for the executor
        config: Optional logger configuration. If None, uses default configuration.

    Returns:
        Logger adapter with executor context
    """
    # Get or create base logger with executor namespace
    base_logger = HookletLogger(config).get_logger(f"eventrix.{executor_id}")

    # Create adapter with executor context
    return EventrixLoggerAdapter(base_logger, executor_id)
