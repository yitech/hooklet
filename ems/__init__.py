#!/usr/bin/env python3
"""
EMS (Exchange Management System) - A tool for managing cryptocurrency exchange operations.
"""
from .core import EventExecutor
from .nats_manager import NatsManager

__all__ = [
    "NatsManager",
    "EventExecutor",
]

__version__ = "0.1.0"
