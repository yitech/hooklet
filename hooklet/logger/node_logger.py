"""
Eventrix Logger Module

Extends the Hooklet logger with executor-specific context.
Each Eventrix instance gets its own logger with executor_id included in all messages.
"""

import logging
from typing import Any, MutableMapping, Optional

from .hooklet_logger import HookletLogger, HookletLoggerConfig


class NodeLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds executor_id context to all log messages."""

    def __init__(self, logger: logging.Logger, node_name: str):
        """Initialize the adapter with executor context.

        Args:
            logger: Base logger instance
            executor_id: Unique identifier for the executor
        """
        super().__init__(logger, {"node_name": node_name})
        self.node_name = node_name

    def process(
        self, msg: str, kwargs: MutableMapping[str, Any]
    ) -> tuple[str, MutableMapping[str, Any]]:
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
        kwargs["extra"]["node_name"] = self.node_name

        # Add executor context to message for non-JSON formats
        return f" {msg}", kwargs


def get_node_logger(
    node_name: str, config: Optional[HookletLoggerConfig] = None
) -> logging.LoggerAdapter:
    """Get a logger for an Node instance with node_name context.

    Args:
        node_name: Unique identifier for the node
        config: Optional logger configuration. If None, uses default configuration.

    Returns:
        Logger adapter with executor context
    """
    # Get or create base logger with executor namespace
    base_logger = HookletLogger(config).get_logger(f"node.{node_name}")

    # Create adapter with executor context
    return NodeLoggerAdapter(base_logger, node_name)
