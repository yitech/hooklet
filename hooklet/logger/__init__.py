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

__all__ = [
    'LogLevel',
    'LogFormat',
    'HookletLoggerConfig',
    'setup_default_logging',
    'get_logger',
    'configure_logging',
    'log_performance',
]

# Default logger instance for convenience
default_logger = get_logger()
