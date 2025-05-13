#!/usr/bin/env python3
"""
Example demonstrating advanced publisher capabilities.

This script shows how to use advanced publishers like filtering,
transforming, merging, and broadcasting data.
"""

import asyncio
import logging
import random
from datetime import datetime

from ems import NatsManager
from ems.publishers import (
    MarketPricePublisher, 
    OrderbookPublisher,
    FilterPublisher,
    TransformPublisher,
    MergePublisher,
    BroadcastPublisher
)

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def main():
    # Create a NatsManager instance
    nats_manager = NatsManager()
    await nats_manager.connect()
    
    try:
        # Create base publishers
        btc_price_publisher = MarketPricePublisher(
            symbol="BTC/USDT",
            initial_price=30000.0,
            volatility=50.0,
            interval_seconds=1.0,
            nats_manager=nats_manager
        )
        
        eth_price_publisher = MarketPricePublisher(
            symbol="ETH/USDT",
            initial_price=2000.0,
            volatility=20.0,
            interval_seconds=1.0,
            nats_manager=nats_manager
        )
        
        btc_orderbook_publisher = OrderbookPublisher(
            symbol="BTC/USDT",
            depth=5,
            base_price=30000.0,
            nats_manager=nats_manager
        )
        
        # 1. Filter publisher - only show significant price changes
        btc_significant_price_publisher = FilterPublisher(
            source_publisher=btc_price_publisher,
            filter_func=lambda data: abs(data["price"] - 30000) > 100,
            subject="market.price.significant.btc-usdt",
            nats_manager=nats_manager
        )
        
        # 2. Transform publisher - add timestamp and formatted price
        def transform_price(data):
            return {
                "symbol": data["symbol"],
                "price": data["price"],
                "price_formatted": f"${data['price']:,.2f}",
                "timestamp": datetime.now().isoformat(),
                "above_threshold": data["price"] > 30000
            }
        
        btc_enhanced_publisher = TransformPublisher(
            source_publisher=btc_price_publisher,
            transform_func=transform_price,
            subject="market.price.enhanced.btc-usdt",
            nats_manager=nats_manager
        )
        
        # 3. Merge publisher - combine BTC and ETH data
        def merge_crypto_data(data_dict):
            result = {
                "timestamp": datetime.now().isoformat(),
                "prices": {}
            }
            
            for pub_id, data in data_dict.items():
                if data and "symbol" in data:
                    symbol = data["symbol"].split("/")[0].lower()
                    result["prices"][symbol] = data["price"]
            
            return result
        
        crypto_prices_publisher = MergePublisher(
            publishers=[btc_price_publisher, eth_price_publisher],
            subject="market.prices.crypto",
            interval_seconds=2.0,
            merge_func=merge_crypto_data,
            nats_manager=nats_manager
        )
        
        # 4. Broadcast publisher - send BTC orderbook to multiple channels
        btc_orderbook_broadcaster = BroadcastPublisher(
            source_publisher=btc_orderbook_publisher,
            subjects=[
                "market.orderbook.btc-usdt",
                "analytics.market.orderbook.btc-usdt",
                "ui.market.orderbook.btc"
            ],
            nats_manager=nats_manager
        )
        
        # Start all the advanced publishers
        logger.info("Starting publishers")
        publishers = [
            btc_significant_price_publisher,
            btc_enhanced_publisher,
            crypto_prices_publisher,
            btc_orderbook_broadcaster
        ]
        
        tasks = [asyncio.create_task(pub.start()) for pub in publishers]
        await asyncio.gather(*tasks)
        
    except asyncio.CancelledError:
        logger.info("Publishers cancelled")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        # Stop all publishers
        for publisher in publishers:
            await publisher.stop()
        await nats_manager.close()
        logger.info("All publishers stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
