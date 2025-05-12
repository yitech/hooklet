#!/usr/bin/env python3
"""
Example script demonstrating NATS integration with EMS.

This example shows how to:
1. Connect to NATS
2. Publish messages
3. Subscribe to subjects
"""

import asyncio
import logging
from nats.aio.client import Client as NATS
import os
import json

from ems.config import ConfigManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    # Load EMS configuration
    config = ConfigManager().load()
    
    # Connect to NATS
    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")
    logger.info(f"Connecting to NATS at {nats_url}")
    
    nc = NATS()
    await nc.connect(nats_url)
    
    # Define callback for order updates
    async def order_update_handler(msg):
        subject = msg.subject
        data = json.loads(msg.data.decode())
        logger.info(f"Received message on {subject}: {data}")
    
    # Subscribe to order updates
    await nc.subscribe("ems.orders.>", cb=order_update_handler)
    
    # Example: Publish a test message
    test_order = {
        "account": "binance-main",
        "symbol": "BTC/USDT",
        "type": "limit",
        "side": "buy",
        "amount": 0.001,
        "price": 40000
    }
    
    await nc.publish("ems.orders.new", json.dumps(test_order).encode())
    logger.info(f"Published test order: {test_order}")
    
    # Keep running to receive messages
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
