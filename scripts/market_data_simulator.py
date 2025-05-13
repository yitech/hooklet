#!/usr/bin/env python3
"""
Mock price data publisher for SimpleMAStrategy.
Publishes fake BTC/USDT prices to NATS.
"""

import asyncio
import logging
from ems import NatsManager
from ems.publishers import MarketPricePublisher, OrderbookPublisher, TradePublisher

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    # Create a NatsManager instance
    nats_manager = NatsManager()
    await nats_manager.connect()
    
    try:
        # Create publishers for different types of market data
        price_publisher = MarketPricePublisher(
            symbol="BTC/USDT",
            initial_price=30000.0,
            volatility=50.0,
            interval_seconds=1.0,
            nats_manager=nats_manager
        )
        
        orderbook_publisher = OrderbookPublisher(
            symbol="BTC/USDT",
            depth=10,
            base_price=30000.0,
            spread=5.0,
            interval_seconds=0.5,
            nats_manager=nats_manager
        )
        
        trade_publisher = TradePublisher(
            symbol="BTC/USDT",
            base_price=30000.0,
            volatility=25.0,
            max_interval=2.0,
            nats_manager=nats_manager
        )
        
        # Start all publishers as asyncio tasks
        logger.info("Starting market data simulators")
        price_task = asyncio.create_task(price_publisher.start())
        orderbook_task = asyncio.create_task(orderbook_publisher.start())
        trade_task = asyncio.create_task(trade_publisher.start())
        
        # Wait for all publishers to complete (they won't unless stopped)
        await asyncio.gather(price_task, orderbook_task, trade_task)
        
    except asyncio.CancelledError:
        logger.info("Publisher tasks cancelled")
    finally:
        # Make sure we close the publishers if anything goes wrong
        await price_publisher.stop()
        await orderbook_publisher.stop()
        await trade_publisher.stop()
        await nats_manager.close()
        logger.info("All publishers stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
