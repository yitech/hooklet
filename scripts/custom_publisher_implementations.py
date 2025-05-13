#!/usr/bin/env python3
"""
Example showing how to create custom publisher implementations.

This script demonstrates how developers can create their own
publisher implementations by inheriting from the base classes.
"""

import asyncio
import logging
import random
import time
from datetime import datetime
from typing import Any, Dict, Optional

from ems import NatsManager
from ems.publisher import IntervalPublisher, StreamPublisher

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


###############################################################################
# Example 1: Custom Interval Publisher
###############################################################################

class NewsPublisher(IntervalPublisher):
    """
    Custom publisher for simulated news events.
    
    Publishes random news events at varying intervals.
    """
    
    # Define some sample news templates
    NEWS_TEMPLATES = [
        "{exchange} announces {event_type} for {symbol}",
        "Market report: {symbol} {direction} by {percent}% on {exchange}",
        "Regulatory update: {country} {action} cryptocurrency {regulation_type}",
        "Whale alert: {amount} {symbol} moved to {destination}",
        "{company} plans to {action} {amount} {symbol} in Q{quarter}"
    ]
    
    EXCHANGES = ["Binance", "Coinbase", "Kraken", "FTX", "Huobi"]
    SYMBOLS = ["BTC", "ETH", "SOL", "ADA", "DOT", "DOGE"]
    EVENT_TYPES = ["listing", "delisting", "trading competition", "airdrop", "staking program"]
    DIRECTIONS = ["up", "down", "surge", "plunge", "rally"]
    COUNTRIES = ["US", "EU", "China", "Japan", "UK", "India"]
    ACTIONS = ["approves", "bans", "restricts", "investigates", "proposes new rules for"]
    REGULATION_TYPES = ["trading", "mining", "staking", "exchanges", "taxation"]
    COMPANIES = ["MicroStrategy", "Tesla", "Square", "Grayscale", "Galaxy Digital"]
    COMPANY_ACTIONS = ["buy", "sell", "hold", "acquire", "invest in"]
    DESTINATIONS = ["exchange", "unknown wallet", "cold storage", "staking contract"]
    
    def __init__(self, subject: str = "market.news", 
                 min_interval: float = 5.0, max_interval: float = 30.0,
                 nats_manager: Optional[NatsManager] = None):
        """
        Initialize the news publisher.
        
        Args:
            subject: NATS subject to publish to
            min_interval: Minimum time between news events in seconds
            max_interval: Maximum time between news events in seconds
            nats_manager: Optional instance of NatsManager
        """
        super().__init__(subject, interval_seconds=min_interval, nats_manager=nats_manager)
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.news_id = 0
    
    async def get_data(self) -> Dict[str, Any]:
        """
        Generate a random news event.
        
        Returns:
            Dictionary with news event information
        """
        # Select a random news template
        template = random.choice(self.NEWS_TEMPLATES)
        
        # Create the news content by filling in the template
        content = template.format(
            exchange=random.choice(self.EXCHANGES),
            symbol=random.choice(self.SYMBOLS),
            event_type=random.choice(self.EVENT_TYPES),
            direction=random.choice(self.DIRECTIONS),
            percent=round(random.uniform(1, 15), 1),
            country=random.choice(self.COUNTRIES),
            action=random.choice(self.ACTIONS),
            regulation_type=random.choice(self.REGULATION_TYPES),
            company=random.choice(self.COMPANIES),
            amount=f"{round(random.uniform(100, 10000)):,}",
            destination=random.choice(self.DESTINATIONS),
            quarter=random.randint(1, 4)
        )
        
        # Increment the news ID
        self.news_id += 1
        
        # Vary the interval for the next news event
        self.interval_seconds = random.uniform(self.min_interval, self.max_interval)
        
        # Calculate a random impact score
        impact_score = random.randint(1, 10)
        
        return {
            "id": self.news_id,
            "timestamp": time.time(),
            "datetime": datetime.now().isoformat(),
            "content": content,
            "impact_score": impact_score,
            "sentiment": "positive" if random.random() > 0.5 else "negative",
            "tags": random.sample(self.SYMBOLS, k=random.randint(1, 3))
        }


###############################################################################
# Example 2: Custom Stream Publisher
###############################################################################

class FileWatcherPublisher(StreamPublisher):
    """
    Custom publisher that watches a file for changes.
    
    Monitors a file for changes and publishes new content.
    """
    
    def __init__(self, filepath: str, subject: str,
                 nats_manager: Optional[NatsManager] = None):
        """
        Initialize the file watcher publisher.
        
        Args:
            filepath: Path to the file to watch
            subject: NATS subject to publish to
            nats_manager: Optional instance of NatsManager
        """
        super().__init__(subject, nats_manager)
        self.filepath = filepath
        self.last_position = 0
        self.file = None
    
    async def connect_to_stream(self) -> Any:
        """
        Open the file for reading.
        
        Returns:
            File object
        """
        logger.info(f"Watching file: {self.filepath}")
        self.file = open(self.filepath, 'r')
        
        # Move to the end of the file
        self.file.seek(0, 2)  # 0 offset from the end (2)
        self.last_position = self.file.tell()
        
        return self.file
    
    async def get_stream_data(self, stream: Any) -> Optional[Dict[str, Any]]:
        """
        Check for new content in the file.
        
        Args:
            stream: The file object
            
        Returns:
            Dictionary with new file content or None if no changes
        """
        # Wait a bit before checking for changes
        await asyncio.sleep(1)
        
        # Get current file size
        current_position = stream.tell()
        stream.seek(0, 2)  # Move to the end
        end_position = stream.tell()
        
        # If the file has grown
        if end_position > self.last_position:
            # Move to where we last read
            stream.seek(self.last_position)
            
            # Read the new content
            new_content = stream.read()
            
            # Update the last position
            self.last_position = end_position
            
            # Return the new content
            return {
                "filepath": self.filepath,
                "timestamp": time.time(),
                "content": new_content,
                "bytes_read": len(new_content)
            }
        else:
            # Move back to the current position
            stream.seek(current_position)
            return None
    
    async def disconnect_from_stream(self, stream: Any) -> None:
        """
        Close the file.
        
        Args:
            stream: The file object
        """
        if stream and not stream.closed:
            stream.close()
            logger.info(f"Closed file: {self.filepath}")


###############################################################################
# Main function to demonstrate the custom publishers
###############################################################################

async def main():
    # Create a NatsManager instance
    nats_manager = NatsManager()
    await nats_manager.connect()
    
    try:
        # Create custom publishers
        news_publisher = NewsPublisher(
            subject="market.news",
            min_interval=3.0,
            max_interval=10.0,
            nats_manager=nats_manager
        )
        
        # For demonstration, create a temporary file to watch
        import tempfile
        temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False)
        temp_file.write("Initial content\n")
        temp_file.flush()
        temp_filepath = temp_file.name
        temp_file.close()
        
        file_watcher = FileWatcherPublisher(
            filepath=temp_filepath,
            subject="file.changes",
            nats_manager=nats_manager
        )
        
        # Start publishers
        logger.info("Starting custom publishers")
        news_task = asyncio.create_task(news_publisher.start())
        file_task = asyncio.create_task(file_watcher.start())
        
        # In a separate task, periodically append to the file
        async def append_to_file():
            for i in range(10):
                with open(temp_filepath, 'a') as f:
                    f.write(f"New content {i} at {datetime.now().isoformat()}\n")
                    f.flush()
                await asyncio.sleep(2)
        
        append_task = asyncio.create_task(append_to_file())
        
        # Wait for some time to demonstrate the publishers
        await asyncio.sleep(30)
        
        # Cancel the tasks
        news_task.cancel()
        file_task.cancel()
        append_task.cancel()
        
        # Clean up the temporary file
        import os
        os.unlink(temp_filepath)
        
    except asyncio.CancelledError:
        logger.info("Publishers cancelled")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        # Stop the publishers
        await news_publisher.stop()
        await file_watcher.stop()
        await nats_manager.close()
        logger.info("All publishers stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
