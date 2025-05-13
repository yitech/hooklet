#!/usr/bin/env python3
"""
Example of creating custom publishers for EMS.

This script demonstrates how to create custom publishers by extending
the base Publisher classes provided by EMS.
"""

import asyncio
import logging
import random
import time
from typing import Dict, Any, Optional
from datetime import datetime

from ems import IntervalPublisher, StreamPublisher, NatsManager

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class OrderbookPublisher(IntervalPublisher):
    """
    Custom publisher for simulated orderbook data.
    
    Demonstrates how to create a custom interval-based publisher.
    """
    
    def __init__(self, symbol: str, depth: int = 5, 
                 base_price: float = 30000.0, spread: float = 2.0,
                 nats_manager: Optional[NatsManager] = None):
        """
        Initialize the orderbook publisher.
        
        Args:
            symbol: Trading symbol (e.g., "BTC/USDT")
            depth: Number of price levels to simulate
            base_price: Center price for the orderbook
            spread: Base spread between bids and asks
            nats_manager: Optional NatsManager instance
        """
        subject = f"market.orderbook.{symbol}"
        super().__init__(subject, interval_seconds=0.5, nats_manager=nats_manager)
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
            "timestamp": datetime.now().timestamp()
        }


class ExternalAPIPublisher(StreamPublisher):
    """
    Custom publisher that connects to an external API.
    
    Demonstrates how to create a custom stream-based publisher.
    """
    
    def __init__(self, api_url: str, subject: str, 
                 auth_token: Optional[str] = None,
                 nats_manager: Optional[NatsManager] = None):
        """
        Initialize the external API publisher.
        
        Args:
            api_url: URL of the external API stream
            subject: NATS subject to publish to
            auth_token: Optional authentication token
            nats_manager: Optional NatsManager instance
        """
        super().__init__(subject, nats_manager)
        self.api_url = api_url
        self.auth_token = auth_token
        self.client = None
    
    async def connect_to_stream(self) -> Any:
        """
        Connect to the external API stream.
        
        Returns:
            Client object for the API connection
        """
        # This is a mock implementation - in a real scenario, you would:
        # 1. Import the appropriate client library
        # 2. Connect to the API using authentication
        # 3. Return the client or connection object
        
        logger.info(f"Connecting to external API: {self.api_url}")
        # Mock client creation
        self.client = {"connected": True, "url": self.api_url}
        return self.client
    
    async def get_stream_data(self, stream: Any) -> Any:
        """
        Get data from the external API stream.
        
        Args:
            stream: The client object returned by connect_to_stream
            
        Returns:
            Data received from the external API
        """
        # This is a mock implementation - in a real scenario, you would:
        # 1. Wait for data from the stream
        # 2. Parse and return the data
        
        # Simulate network delay
        await asyncio.sleep(random.uniform(0.2, 1.0))
        
        # Generate mock data
        return {
            "type": "market_update",
            "source": stream["url"],
            "data": {
                "price": round(random.uniform(29000, 31000), 2),
                "volume": round(random.uniform(1, 100), 2),
                "timestamp": time.time()
            }
        }
    
    async def disconnect_from_stream(self, stream: Any) -> None:
        """
        Disconnect from the external API stream.
        
        Args:
            stream: The client object returned by connect_to_stream
        """
        # This is a mock implementation - in a real scenario, you would:
        # 1. Close the connection properly
        
        logger.info(f"Disconnecting from external API: {self.api_url}")
        self.client = None


async def main():
    # Create a shared NatsManager
    nats_manager = NatsManager()
    await nats_manager.connect()
    
    try:
        # Create the custom publishers
        orderbook_publisher = OrderbookPublisher(
            symbol="ETH/USDT",
            depth=10,
            base_price=2200.0,
            nats_manager=nats_manager
        )
        
        api_publisher = ExternalAPIPublisher(
            api_url="https://api.example.com/stream",
            subject="external.market.data",
            nats_manager=nats_manager
        )
        
        # Start the publishers
        logger.info("Starting custom publishers...")
        orderbook_task = asyncio.create_task(orderbook_publisher.start())
        api_task = asyncio.create_task(api_publisher.start())
        
        # Wait for interrupt
        await asyncio.gather(orderbook_task, api_task)
        
    except asyncio.CancelledError:
        logger.info("Publishers cancelled")
    finally:
        # Stop the publishers
        await orderbook_publisher.stop()
        await api_publisher.stop()
        await nats_manager.close()
        logger.info("All publishers stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
