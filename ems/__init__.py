#!/usr/bin/env python3
"""
EMS (Exchange Management System) - A tool for managing cryptocurrency exchange operations.
"""

from .config import (
    ConfigManager, 
    Account, 
    ConfigError,
)

from .nats_manager import NatsManager
from .strategy import Strategy

__all__ = [
    'ConfigManager',
    'Account',
    'ConfigError',
    'NatsManager',
    'Strategy',
]

__version__ = '0.1.0'