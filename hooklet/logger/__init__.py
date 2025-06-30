"""
Hooklet Logger Package

A simple logging system for the Hooklet framework.
"""

from .node_logger import get_node_logger
from .hooklet_logger import (
    HookletLoggerConfig,
    HookletLogger,
    LogFormat,
    LogLevel,
    configure_logging,
    get_logger,
    log_performance,
    setup_default_logging,
)

__all__ = [
    "LogLevel",
    "LogFormat",
    "HookletLoggerConfig",
    "HookletLogger",
    "setup_default_logging",
    "get_logger",
    "configure_logging",
    "log_performance",
    "get_node_logger",
]

# Default logger instance for convenience
default_logger = get_logger()
