#!/usr/bin/env python3
"""
Binance user data stream client for EMS.
Connects to Binance Futures user data stream and publishes events to NATS.
"""

import asyncio
from binance import AsyncClient
import logging
from ems import NatsManager
from ems.publishers import BinanceUserDataPublisher
from ems.config import ConfigManager

logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SUBJECT = "ems.orders"

async def main():
    # Load configuration from config.yml
    config = ConfigManager().load()
    account = config.get_account("SCYLLA")  # Using the SCYLLA account from config.yml
    
    # Create Binance client
    client = await AsyncClient.create(account.api_key, account.api_secret, testnet=False)
    
    # Create NATS manager
    nats_manager = NatsManager()
    await nats_manager.connect()
    
    try:
        # Create the user data publisher
        publisher = BinanceUserDataPublisher(
            client=client,
            subject=SUBJECT,
            nats_manager=nats_manager
        )
        
        # Start publishing
        logger.info(f"Starting Binance user data stream publisher to {SUBJECT}")
        await publisher.start()
        
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        # Ensure everything is properly closed
        await publisher.stop()
        await nats_manager.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
