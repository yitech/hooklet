#!/usr/bin/env python3
"""
Example script demonstrating the usage of NatsManager with EMS.

This example shows how to:
1. Create a NatsManager instance
2. Connect to NATS
3. Register handler functions
4. Unregister handler functions
5. Publish messages
"""

import asyncio
import logging
import json
import signal
import sys

from ems import NatsManager

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define async handler functions for different NATS subjects
async def order_created_handler(data):
    """Handle new order creation events"""
    logger.info(f"New order created: {data}")
    
async def order_filled_handler(data):
    """Handle order fill events"""
    logger.info(f"Order filled: {data}")
    logger.info(f"Fill price: {data.get('price')}, amount: {data.get('filled_amount')}")
    
async def market_data_handler(data):
    """Handle market data updates"""
    logger.info(f"Market data update for {data.get('symbol')}: Bid={data.get('bid')}, Ask={data.get('ask')}")

async def main():
    # Create a NatsManager instance
    nats_manager = NatsManager()
    
    # Connect to NATS server
    await nats_manager.connect()
    
    # Register handler functions
    await nats_manager.register_handler("ems.orders.new", order_created_handler)
    await nats_manager.register_handler("ems.orders.filled", order_filled_handler)
    
    # Register with a custom handler_id
    await nats_manager.register_handler("ems.market.>", market_data_handler, handler_id="market_data_tracker")
    
    # Print registered handlers
    handlers = nats_manager.get_registered_handlers()
    logger.info(f"Registered handlers: {json.dumps(handlers, indent=2)}")
    
    # Publish example messages
    await nats_manager.publish("ems.orders.new", {
        "account": "binance-main",
        "symbol": "BTC/USDT",
        "type": "limit",
        "side": "buy",
        "amount": 0.001,
        "price": 40000
    })
    
    await nats_manager.publish("ems.orders.filled", {
        "account": "binance-main",
        "symbol": "BTC/USDT",
        "order_id": "12345",
        "filled_amount": 0.001,
        "price": 39950
    })
    
    await nats_manager.publish("ems.market.price", {
        "symbol": "BTC/USDT",
        "bid": 39950,
        "ask": 39955,
        "timestamp": 1683912345
    })
    
    # Unregister a handler
    logger.info("Unregistering the market data handler...")
    success = await nats_manager.unregister_handler("ems.market.>", "market_data_tracker")
    if success:
        logger.info("Market data handler unregistered successfully")
    
    # This message will not be processed since we unregistered the handler
    await nats_manager.publish("ems.market.price", {
        "symbol": "ETH/USDT",
        "bid": 2800,
        "ask": 2805,
        "timestamp": 1683912400
    })
    
    # Print updated list of handlers
    handlers = nats_manager.get_registered_handlers()
    logger.info(f"Updated registered handlers: {json.dumps(handlers, indent=2)}")
    
    # Sleep to allow processing of messages
    logger.info("Waiting for 3 seconds to process messages...")
    await asyncio.sleep(3)
    
    # Close the NATS connection when done
    await nats_manager.close()


async def shutdown(loop):
    """Shutdown the application gracefully"""
    logger.info("Shutting down...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

if __name__ == "__main__":
    try:
        # Create an event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Set up signal handlers
        for signal_name in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(
                getattr(signal, signal_name),
                lambda: asyncio.create_task(shutdown(loop))
            )
            
        # Run the main function
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)
