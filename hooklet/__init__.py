#!/usr/bin/env python3
"""
NATSIO - An asynchronous, event-driven Python framework using NATS messaging system.
"""
from .core import EventExecutor
from .nats_manager import NatsManager

__version__ = "0.1.0"

__all__ = [
    "NatsManager",
    "EventExecutor",
]

