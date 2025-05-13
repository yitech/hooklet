#!/usr/bin/env python3
"""
Market data publishers for EMS.

This module provides publishers for market data like prices,
orderbooks, and other market-related information.
"""

import random
import logging
from typing import Dict, Optional, Any

from ..publisher import IntervalPublisher
from ..nats_manager import NatsManager

logger = logging.getLogger(__name__)


class MarketPricePublisher(IntervalPublisher):
    """
    Publisher for simulated market price data.
    
    Publishes simulated price data for a given symbol at regular intervals.
    """
    
    def __init__(self, symbol: str, initial_price: float = 30000.0, 
                 volatility: float = 50.0, interval_seconds: float = 1.0,
                 nats_manager: Optional[NatsManager] = None):
        """
        Initialize the market price publisher.
        
        Args:
            symbol: The trading symbol (e.g., "BTC/USDT")
            initial_price: Starting price for the simulation
            volatility: Maximum price change per interval (both up and down)
            interval_seconds: Time between publications in seconds
            nats_manager: Optional instance of NatsManager
        """
        subject = f"market.price.{symbol}"
        super().__init__(subject, interval_seconds, nats_manager)
        self.symbol = symbol
        self.price = initial_price
        self.volatility = volatility
    
    async def get_data(self) -> Dict[str, Any]:
        """
        Generate simulated price data.
        
        Returns:
            Dictionary with symbol and price information
        """
        # Simulate small random price fluctuations
        self.price += random.uniform(-self.volatility, self.volatility)
        return {
            "symbol": self.symbol,
            "price": round(self.price, 2)
        }


class OrderbookPublisher(IntervalPublisher):
    """
    Publisher for simulated orderbook data.
    
    Publishes simulated orderbook data for a given symbol at regular intervals.
    """
    
    def __init__(self, symbol: str, depth: int = 5, 
                 base_price: float = 30000.0, spread: float = 2.0,
                 interval_seconds: float = 0.5,
                 nats_manager: Optional[NatsManager] = None):
        """
        Initialize the orderbook publisher.
        
        Args:
            symbol: Trading symbol (e.g., "BTC/USDT")
            depth: Number of price levels to simulate
            base_price: Center price for the orderbook
            spread: Base spread between bids and asks
            interval_seconds: Time between publications in seconds
            nats_manager: Optional NatsManager instance
        """
        subject = f"market.orderbook.{symbol}"
        super().__init__(subject, interval_seconds, nats_manager)
        self.symbol = symbol
        self.depth = depth
        self.base_price = base_price
        self.spread = spread
    
    async def get_data(self) -> Dict[str, Any]:
        """
        Generate simulated orderbook data.
        
        Returns:
            Dictionary with bids and asks
        """
        # Update base price with some random movement
        self.base_price += random.uniform(-5, 5)
        
        # Generate bids (buy orders below the price)
        bids = []
        for i in range(self.depth):
            price = round(self.base_price - self.spread/2 - i, 2)
            size = round(random.uniform(0.1, 2.0), 3)
            bids.append([price, size])
        
        # Generate asks (sell orders above the price)
        asks = []
        for i in range(self.depth):
            price = round(self.base_price + self.spread/2 + i, 2)
            size = round(random.uniform(0.1, 2.0), 3)
            asks.append([price, size])
        
        return {
            "symbol": self.symbol,
            "bids": bids,
            "asks": asks,
            "timestamp": int(1000 * random.random()) # Just a mock timestamp
        }


class TradePublisher(IntervalPublisher):
    """
    Publisher for simulated trade data.
    
    Publishes simulated trade data for a given symbol at random intervals.
    """
    
    def __init__(self, symbol: str, base_price: float = 30000.0, 
                 volatility: float = 10.0, max_interval: float = 3.0,
                 nats_manager: Optional[NatsManager] = None):
        """
        Initialize the trade publisher.
        
        Args:
            symbol: Trading symbol (e.g., "BTC/USDT")
            base_price: Base price for trades
            volatility: Maximum price variation from base price
            max_interval: Maximum time between trades in seconds
            nats_manager: Optional NatsManager instance
        """
        subject = f"market.trades.{symbol}"
        # Use random intervals between trades
        super().__init__(subject, interval_seconds=random.uniform(0.1, max_interval), nats_manager=nats_manager)
        self.symbol = symbol
        self.base_price = base_price
        self.volatility = volatility
        self.max_interval = max_interval
        self.trade_id = 0
    
    async def get_data(self) -> Dict[str, Any]:
        """
        Generate simulated trade data.
        
        Returns:
            Dictionary with trade information
        """
        # Generate random trade
        side = "buy" if random.random() > 0.5 else "sell"
        price = round(self.base_price + random.uniform(-self.volatility, self.volatility), 2)
        amount = round(random.uniform(0.001, 0.1), 3)
        self.trade_id += 1
        
        # Randomly change the interval for the next publication
        self.interval_seconds = random.uniform(0.1, self.max_interval)
        
        return {
            "symbol": self.symbol,
            "side": side,
            "price": price,
            "amount": amount,
            "trade_id": self.trade_id,
            "timestamp": int(1000 * random.random()) # Just a mock timestamp
        }
