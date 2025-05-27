"""
Hooklet Logger Package

A comprehensive logging system for the Hooklet framework.
"""

from .hooklet_logger import (
    HookletLogger,
    HookletLoggerConfig,
    LogLevel,
    LogFormat,
    LogDestination,
    JSONFormatter,
    AsyncSafeFormatter,
    get_logger,
    configure_logging,
    setup_default_logging,
    log_performance,
)

__all__ = [
    'HookletLogger',
    'HookletLoggerConfig',
    'LogLevel',
    'LogFormat',
    'LogDestination',
    'JSONFormatter',
    'AsyncSafeFormatter',
    'get_logger',
    'configure_logging',
    'setup_default_logging',
    'log_performance',
]

# Default logger instance for convenience
default_logger = get_logger()
