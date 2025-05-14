#!/usr/bin/env python3
"""
Example script that streams BTC/USDT klines from Binance and publishes them to NATS.

This script uses the ccxt.pro library to stream market data from Binance and
publishes it to a NATS subject using the EMS StreamPublisher class.
"""

import asyncio
import sys
import os
import logging
from typing import Any, Dict, List, Optional

# Add the parent directory to the path to import from ems
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ccxt.pro
from adhoc.publisher import StreamPublisher
from ems.nats_manager import NatsManager

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BinanceKlinePublisher(StreamPublisher):
    """
    Publisher that streams kline (OHLCV) data from Binance and
    publishes it to a NATS subject.
    """
    
    def __init__(self, 
                 nats_manager: NatsManager, 
                 subject: str,
                 symbol: str = 'BTC/USDT', 
                 timeframe: str = '1m',
                 is_future: bool = True):
        """
        Initialize the Binance kline publisher.
        
        Args:
            nats_manager: Instance of NatsManager
            subject: The NATS subject to publish to
            symbol: Trading pair symbol to watch (e.g., 'BTC/USDT')
            timeframe: Timeframe for klines (e.g., '1m', '5m', '1h')
            is_future: Whether to use futures market (True) or spot market (False)
        """
        super().__init__(nats_manager)
        self.subject = subject
        self.symbol = symbol
        self.timeframe = timeframe
        self.is_future = is_future
        self.exchange = None
    
    async def connect_to_stream(self) -> Any:
        """
        Connect to the Binance exchange API.
        
        Returns:
            Exchange object from ccxt.pro
        """
        logger.info(f"Connecting to Binance for {self.symbol} {self.timeframe} klines")
        exchange = ccxt.pro.binance()
        
        if self.is_future:
            exchange.options['defaultType'] = 'future'
        
        return exchange
    
    async def get_stream_data(self, exchange: Any) -> Any:
        """
        Get kline data from Binance.
        
        Args:
            exchange: The exchange object returned by connect_to_stream
            
        Returns:
            Formatted kline data ready for publishing
        """
        try:
            # Watch OHLCV (Open, High, Low, Close, Volume) data
            ohlcv = await exchange.watch_ohlcv_for_symbols([["BTC/USDT", "1m"], ["ETH/USDT", "1m"]])
            logger.info(f"Received kline data: {ohlcv}")
            # Format the data for easier consumption
            timestamp, open_price, high, low, close, volume = ohlcv
            for k, v in ohlcv.items():
                logger.info(f"Received kline data for {k}: {v}")
                timestamp, open_price, high, low, close, volume = v
                formatted_data = {
                    "exchange": "binance",
                    "symbol": self.symbol,
                    "timeframe": self.timeframe,
                    "timestamp": timestamp,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume
                }
                return formatted_data
        except Exception as e:
            logger.error(f"Error watching OHLCV data: {type(e).__name__} - {str(e)}")
            # Re-raise to allow the StreamPublisher to handle it
            raise
    
    async def disconnect_from_stream(self, exchange: Any) -> None:
        """
        Disconnect from the Binance exchange API.
        
        Args:
            exchange: The exchange object returned by connect_to_stream
        """
        if exchange:
            logger.info(f"Closing connection to Binance")
            await exchange.close()
    
    def on_publish(self, data: Any) -> None:
        """
        Called after data is published.
        
        Args:
            data: The data that was published
        """
        logger.info(f"Published {self.symbol} {self.timeframe} kline: "
                   f"time={data['timestamp']}, close={data['close']}")


async def main():
    """Main function to run the publisher."""
    # Create a NatsManager instance
    nats_manager = NatsManager()
    
    # Create a kline publisher
    publisher = BinanceKlinePublisher(
        nats_manager=nats_manager,
        subject="market.binance.kline.btcusdt.1m",
        symbol="BTC/USDT",
        timeframe="1m",
        is_future=True
    )
    
    try:
        # Start the publisher (this will connect to NATS and start streaming)
        await publisher.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, stopping...")
    except Exception as e:
        logger.error(f"Error running publisher: {type(e).__name__} - {str(e)}")
    finally:
        # Ensure we stop and close the publisher properly
        await publisher.stop()
        await publisher.close()


if __name__ == "__main__":
    asyncio.run(main())