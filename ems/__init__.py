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
from .publisher import Publisher, IntervalPublisher, StreamPublisher

# Import publishers from the publishers package
from .publishers import MarketPricePublisher, BinanceUserDataPublisher

__all__ = [
    'ConfigManager',
    'Account',
    'ConfigError',
    'NatsManager',
    'Strategy',
    'Publisher',
    'IntervalPublisher',
    'StreamPublisher',
    'MarketPricePublisher',
    'BinanceUserDataPublisher',
]

__version__ = '0.1.0'