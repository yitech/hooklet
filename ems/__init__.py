#!/usr/bin/env python3
"""
EMS (Exchange Management System) - A tool for managing cryptocurrency exchange operations.
"""

from .config import (
    Account,
    ConfigError,
    ConfigManager,
)
from .core import EventExecutor
from .nats_manager import NatsManager

__all__ = [
    "ConfigManager",
    "Account",
    "ConfigError",
    "NatsManager",
    "EventExecutor",
]

__version__ = "0.1.0"
