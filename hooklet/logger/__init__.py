"""
Hooklet Logger Package

A simple logging system for the Hooklet framework.
"""

from .hooklet_logger import (
    LogLevel,
    LogFormat,
    HookletLoggerConfig,
    setup_default_logging,
    get_logger,
    configure_logging,
    log_performance,
)

from .eventrix_logger import get_eventrix_logger

__all__ = [
    'LogLevel',
    'LogFormat',
    'HookletLoggerConfig',
    'setup_default_logging',
    'get_logger',
    'configure_logging',
    'log_performance',
    'get_eventrix_logger',
]

# Default logger instance for convenience
default_logger = get_logger()
