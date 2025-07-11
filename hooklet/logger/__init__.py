"""
Hooklet Logger Package

A simple logging system for the Hooklet framework.
"""

from .hooklet_logger import (
    HookletLogger,
    HookletLoggerConfig,
    LogFormat,
    LogLevel,
    configure_logging,
    get_logger,
    setup_default_logging,
)
from .node_logger import get_node_logger

__all__ = [
    "LogLevel",
    "LogFormat",
    "HookletLoggerConfig",
    "HookletLogger",
    "setup_default_logging",
    "get_logger",
    "configure_logging",
    "get_node_logger",
]

# Default logger instance for convenience
default_logger = get_logger()
