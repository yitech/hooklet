#!/usr/bin/env python3
"""
Example script demonstrating orderbook streaming from Binance using CCXT Pro.

This example shows how to:
1. Connect to an exchange
2. Stream real-time orderbook data
3. Handle the data with proper error recovery
4. Gracefully shutdown on interrupt
"""

import asyncio
import signal
import logging
import argparse
import time
from typing import Dict, Any, Optional
import ccxt.pro
from ccxt.base.errors import NetworkError, ExchangeNotAvailable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for controlled shutdown
shutdown_requested = False


async def watch_orderbook(
    exchange: ccxt.pro.Exchange,
    symbol: str,
    depth: int = 5,
    delay: float = 0.5
) -> None:
    """
    Stream and display orderbook data for a specified symbol.
    
    Args:
        exchange: The CCXT Pro exchange instance
        symbol: Trading pair symbol (e.g., 'BTC/USDT')
        depth: Number of price levels to display
        delay: Delay between reconnect attempts on failure
    """
    last_timestamp = 0
    reconnect_delay = delay
    
    while not shutdown_requested:
        try:
            orderbook = await exchange.watch_order_book(symbol)
            
            # Reset reconnect delay on successful fetch
            reconnect_delay = delay
            
            # Calculate update time and latency
            now = time.time() * 1000
            timestamp = orderbook['timestamp'] if orderbook['timestamp'] else now
            latency = now - timestamp
            
            # Format orderbook data for display
            update_time = time.strftime('%H:%M:%S', time.localtime(timestamp / 1000))
            
            # Display information
            logger.info(f"Symbol: {symbol} | Time: {update_time} | Latency: {latency:.2f}ms")
            
            # Display top N levels
            for i in range(min(depth, len(orderbook['bids']))):
                bid = orderbook['bids'][i]
                ask = orderbook['asks'][i] if i < len(orderbook['asks']) else [0, 0]
                logger.info(f"#{i+1:<2} Bid: {bid[0]:<10} ({bid[1]:<10}) | Ask: {ask[0]:<10} ({ask[1]:<10})")
            
            # Display spread
            if orderbook['bids'] and orderbook['asks']:
                best_bid = orderbook['bids'][0][0]
                best_ask = orderbook['asks'][0][0]
                spread = best_ask - best_bid
                spread_percent = (spread / best_bid) * 100
                logger.info(f"Spread: {spread:.2f} ({spread_percent:.4f}%)")
            
            logger.info("-" * 80)
            
            # Limit update rate to avoid console flooding
            await asyncio.sleep(0.1)
            
        except (NetworkError, ExchangeNotAvailable) as e:
            logger.warning(f"Connection issue: {type(e).__name__}, {str(e)}")
            logger.info(f"Reconnecting in {reconnect_delay:.1f} seconds...")
            await asyncio.sleep(reconnect_delay)
            # Implement exponential backoff (max 30 seconds)
            reconnect_delay = min(reconnect_delay * 1.5, 30.0)
            
        except Exception as e:
            logger.error(f"Unexpected error: {type(e).__name__}, {str(e)}")
            if not shutdown_requested:
                logger.info(f"Reconnecting in {reconnect_delay:.1f} seconds...")
                await asyncio.sleep(reconnect_delay)


async def main() -> None:
    """Main function that sets up and runs the orderbook streaming example."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Stream cryptocurrency orderbook data')
    parser.add_argument('--symbol', type=str, default='BTC/USDT', help='Trading symbol (e.g., BTC/USDT)')
    parser.add_argument('--exchange', type=str, default='binance', help='Exchange name (e.g., binance, ftx, kraken)')
    parser.add_argument('--market-type', type=str, default='future', choices=['spot', 'future', 'margin'], 
                       help='Market type (spot, future, margin)')
    parser.add_argument('--depth', type=int, default=5, help='Orderbook depth to display')
    args = parser.parse_args()

    # Setup exchange connection
    exchange_class = getattr(ccxt.pro, args.exchange, None)
    if not exchange_class:
        logger.error(f"Exchange {args.exchange} not found or not supported")
        return
    
    exchange_config = {
        'options': {
            'defaultType': args.market_type,
        },
    }
    
    exchange = exchange_class(exchange_config)
    logger.info(f"Connected to {exchange.id} {args.market_type} market")
    
    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(exchange)))
    
    try:
        logger.info(f"Streaming orderbook for {args.symbol}...")
        await watch_orderbook(exchange, args.symbol, args.depth)
    finally:
        if not shutdown_requested:
            await shutdown(exchange)


async def shutdown(exchange: ccxt.pro.Exchange) -> None:
    """Perform graceful shutdown of the application.
    
    Args:
        exchange: The CCXT Pro exchange instance to close
    """
    global shutdown_requested
    if shutdown_requested:
        return
        
    shutdown_requested = True
    logger.info("Shutdown initiated, closing connections...")
    
    try:
        await exchange.close()
        logger.info("Exchange connection closed")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")


if __name__ == "__main__":
    asyncio.run(main())