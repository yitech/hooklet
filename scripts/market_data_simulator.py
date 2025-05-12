#!/usr/bin/env python3
"""
Mock price data publisher for SimpleMAStrategy.
Publishes fake BTC/USDT prices to NATS.
"""

import asyncio
import random
import logging
from ems import NatsManager

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SYMBOL = "BTC/USDT"
SUBJECT = f"market.price.{SYMBOL}"

async def publish_mock_prices():
    nats_manager = NatsManager()
    await nats_manager.connect()

    try:
        price = 30000.0  # starting price
        while True:
            # Simulate small random price fluctuations
            price += random.uniform(-50, 50)
            price_data = {"symbol": SYMBOL, "price": round(price, 2)}

            await nats_manager.publish(SUBJECT, price_data)
            logger.info(f"Published: {price_data} to {SUBJECT}")

            await asyncio.sleep(1)  # 1-second interval
    except asyncio.CancelledError:
        pass
    finally:
        await nats_manager.close()
        logger.info("Publisher stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(publish_mock_prices())
    except KeyboardInterrupt:
        logger.info("Interrupted.")
