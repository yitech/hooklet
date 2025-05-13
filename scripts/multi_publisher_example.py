#!/usr/bin/env python3
"""
Example of running multiple publishers simultaneously.

This script demonstrates how to run multiple publishers in the same application
to provide various types of data to the EMS system.
"""

import asyncio
import logging
from ems import NatsManager, MarketPricePublisher, BinanceUserDataPublisher
from ems.config import ConfigManager
from binance import AsyncClient

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MultiPublisherApp:
    """
    Application that manages multiple data publishers.
    
    Demonstrates how to run and manage multiple publishers in a single application.
    """
    
    def __init__(self):
        """Initialize the application."""
        self.nats_manager = NatsManager()
        self.publishers = []
        self.running = False
    
    async def setup(self):
        """Set up the application and create publishers."""
        # Connect to NATS
        await self.nats_manager.connect()
        
        # Set up market price simulator publishers
        self.publishers.append(
            MarketPricePublisher(
                symbol="BTC/USDT",
                initial_price=30000.0,
                volatility=50.0,
                nats_manager=self.nats_manager
            )
        )
        
        self.publishers.append(
            MarketPricePublisher(
                symbol="ETH/USDT",
                initial_price=2000.0,
                volatility=20.0,
                nats_manager=self.nats_manager
            )
        )
        
        # Set up Binance user data publisher if credentials are available
        try:
            config = ConfigManager().load()
            account = config.get_account("SCYLLA")
            
            client = await AsyncClient.create(account.api_key, account.api_secret, testnet=False)
            
            self.publishers.append(
                BinanceUserDataPublisher(
                    client=client,
                    subject="ems.orders",
                    nats_manager=self.nats_manager
                )
            )
            logger.info("Added Binance user data publisher")
        except Exception as e:
            logger.warning(f"Could not set up Binance user data publisher: {e}")
    
    async def start(self):
        """Start all publishers."""
        if self.running:
            return
            
        self.running = True
        
        # Start all publishers
        publisher_tasks = []
        for i, publisher in enumerate(self.publishers):
            logger.info(f"Starting publisher {i+1}/{len(self.publishers)}: {publisher.__class__.__name__}")
            task = asyncio.create_task(publisher.start())
            publisher_tasks.append(task)
        
        # Wait for all publishers to complete
        try:
            await asyncio.gather(*publisher_tasks)
        except asyncio.CancelledError:
            pass
    
    async def stop(self):
        """Stop all publishers."""
        if not self.running:
            return
            
        self.running = False
        
        # Stop all publishers
        for i, publisher in enumerate(self.publishers):
            logger.info(f"Stopping publisher {i+1}/{len(self.publishers)}: {publisher.__class__.__name__}")
            await publisher.stop()
        
        # Close NATS connection
        await self.nats_manager.close()
        logger.info("All publishers stopped")


async def main():
    app = MultiPublisherApp()
    
    try:
        # Set up publishers
        await app.setup()
        
        # Start all publishers
        await app.start()
        
    except asyncio.CancelledError:
        logger.info("Application cancelled")
    except Exception as e:
        logger.error(f"Error in application: {e}")
    finally:
        # Stop all publishers
        await app.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
