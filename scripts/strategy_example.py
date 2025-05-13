#!/usr/bin/env python3
"""
Example implementation of a simple trading strategy using the Strategy class.
"""

import asyncio
import logging
from typing import Dict, Any

from ems import NatsManager
from ems.strategy import Strategy

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleMAStrategy(Strategy):
    """
    A simple Moving Average crossover strategy example.
    """
    
    def __init__(self, nats_manager: NatsManager, symbol: str = "BTC/USDT"):
        """Initialize the strategy."""
        # Use a NATS-safe subject format (replace '/' with '.')
        # safe_symbol = symbol.replace("/", ".")
        safe_symbol = symbol
        super().__init__(nats_manager, strategy_id=f"MA_{safe_symbol}")
        self.symbol = symbol
        self.safe_symbol = safe_symbol
        self.prices: list[float] = []
        self.ma_period = 20
        logger.info(f"Initializing strategy for {symbol} (subject: market.price.{safe_symbol})")
    
    def get_handlers(self) -> Dict[str, Any]:
        """Define the message handlers for this strategy."""
        # Use the safe symbol format for the subject
        return {
            # f"market.price.{self.safe_symbol}": self.handle_price_update,
            f"market.price.{self.symbol}": self.handle_price_update,  # Try both formats
            "ems.orders.filled": self.handle_order_filled,
        }
    
    async def handle_price_update(self, data: Dict[str, Any]) -> None:
        """Handle incoming price updates."""
        logger.info(f"Price update received for {self.symbol}: {data}")
        if "price" in data:
            price = float(data["price"])
            self.prices.append(price)
            
            # Keep only the last ma_period prices
            if len(self.prices) > self.ma_period:
                self.prices.pop(0)
            
            # Calculate MA when we have enough data
            if len(self.prices) == self.ma_period:
                ma = sum(self.prices) / self.ma_period
                logger.info(f"Moving Average for {self.symbol}: {ma:.2f}")
                
                # Example trading logic
                if price > ma:
                    await self.consider_buy_signal(price)
                elif price < ma:
                    await self.consider_sell_signal(price)
    
    async def handle_order_filled(self, data: Dict[str, Any]) -> None:
        """
        Handle order fill notifications.
        
        Args:
            data: Order fill data
        """
        if data.get("symbol") == self.symbol:
            logger.info(f"Order filled: {data}")
    
    async def consider_buy_signal(self, price: float) -> None:
        """
        Process a potential buy signal.
        
        Args:
            price: Current price that triggered the signal
        """
        order_data = {
            "symbol": self.symbol,
            "side": "buy",
            "type": "limit",
            "price": price,
            "amount": 0.01  # Example fixed amount
        }
        await self.publish("ems.orders.new", order_data)
        logger.info(f"Buy signal generated at {price}")
    
    async def consider_sell_signal(self, price: float) -> None:
        """
        Process a potential sell signal.
        
        Args:
            price: Current price that triggered the signal
        """
        order_data = {
            "symbol": self.symbol,
            "side": "sell",
            "type": "limit",
            "price": price,
            "amount": 0.01  # Example fixed amount
        }
        await self.publish("ems.orders.new", order_data)
        logger.info(f"Sell signal generated at {price}")

    async def consider_orderbook(self, data: Dict[str, Any]) -> None:
        """
        Process order book data.
        
        Args:
            data: Order book data
        """
        logger.info(f"Order book update for {self.symbol}: {data}")
    
    async def on_start(self) -> None:
        """
        Initialization when strategy starts.
        """
        logger.info(f"Starting MA Strategy for {self.symbol}")
        self.prices.clear()
    
    async def on_stop(self) -> None:
        """
        Cleanup when strategy stops.
        """
        logger.info(f"Stopping MA Strategy for {self.symbol}")
        self.prices.clear()


async def main():
    """
    Example of how to use the strategy.
    """
    # Create NatsManager instance
    nats_manager = NatsManager()
    await nats_manager.connect()
    
    try:
        # Create and start the strategy
        strategy = SimpleMAStrategy(nats_manager)
        await strategy.start()
        
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
    
    except KeyboardInterrupt:
        # Stop the strategy and cleanup
        await strategy.stop()
        await nats_manager.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
