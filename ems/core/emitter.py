import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import ccxt.pro
from ccxt.base.errors import ExchangeNotAvailable, NetworkError

from ems.nats_manager import NatsManager

from .executor import EventExecutor

logger = logging.getLogger(__name__)


class EventEmitter(EventExecutor, ABC):
    """
    Base class for event emitters.
    This abstract class provides the structure for emitting events.
    """

    def __init__(self, nats_manager: NatsManager, executor_id: None | str = None):
        super().__init__(nats_manager, executor_id)

    async def on_start(self) -> None:
        """
        Called when the emitter starts. Override to add specific initialization.
        """
        logger.info(f"Starting emitter {self.executor_id}")

    async def on_finish(self) -> None:
        """
        Called when the emitter finishes. Override to add specific cleanup.
        """
        logger.info(f"Stopping emitter {self.executor_id}")

    async def emit_event(self, subject: str, data: Any) -> None:
        """
        Emit an event by publishing to the NATS subject.

        Args:
            subject: The NATS subject to publish to
            data: The data to publish (will be JSON encoded)
        """
        try:
            await self.publish(subject, data)
            logger.debug(f"Emitted event to {subject}: {data}")
        except Exception as e:
            logger.error(f"Failed to emit event to {subject}: {str(e)}")
            raise


class OrderbookEmitter(EventEmitter):
    """
    Emitter that streams orderbook data from an exchange and emits it via NATS.

    This emitter connects to a cryptocurrency exchange using CCXT Pro,
    receives real-time orderbook data, and emits it through the NATS
    messaging system for other components to consume.
    """

    def __init__(
        self,
        nats_manager: NatsManager,
        exchange_id: str,
        symbol: str,
        market_type: str = "future",
        depth: int = 5,
        reconnect_delay: float = 0.5,
        executor_id: Optional[str] = None,
    ):
        """
        Initialize the orderbook emitter.

        Args:
            nats_manager: The NATS manager instance to use for messaging
            exchange_id: The ID of the exchange to connect to (e.g., 'binance')
            symbol: The trading symbol to stream (e.g., 'BTC/USDT')
            market_type: The market type (e.g., 'spot', 'future', 'margin')
            depth: Number of price levels to include in the emitted data
            reconnect_delay: Initial delay between reconnection attempts
            executor_id: Optional unique ID for this emitter
        """
        self.exchange_id = exchange_id
        self.symbol = symbol
        self.market_type = market_type
        self.depth = depth
        self.reconnect_delay = reconnect_delay

        # Generate a default executor_id if none provided
        if executor_id is None:
            symbol_safe = symbol.replace("/", ".")
            executor_id = f"orderbook_{exchange_id}_{symbol_safe}_{market_type}"

        super().__init__(nats_manager, executor_id)

        self.exchange = None
        self._shutdown_requested = False

        # Prepare NATS subject for orderbook data
        # Format: orderbook.<exchange>.<symbol>.<market_type>.<depth>
        symbol_safe = symbol.replace("/", ".")
        self.orderbook_subject = f"orderbook.{exchange_id}.{symbol_safe}.{market_type}.{depth}"

        logger.info(
            f"OrderbookEmitter initialized for {symbol} on {exchange_id} {market_type} market"
        )

    async def on_start(self) -> None:
        """
        Initialize the exchange connection when the emitter starts.
        """
        await super().on_start()

        # Create exchange instance
        exchange_class = getattr(ccxt.pro, self.exchange_id, None)
        if not exchange_class:
            raise ValueError(f"Exchange {self.exchange_id} not found or not supported")

        exchange_config = {
            "options": {
                "defaultType": self.market_type,
            },
        }

        self.exchange = exchange_class(exchange_config)
        logger.info(f"Connected to {self.exchange.id} {self.market_type} market")
        self._shutdown_requested = False

    async def on_execute(self) -> None:
        """
        Main execution loop that streams and emits orderbook data.
        """
        if not self.exchange:
            raise RuntimeError("Exchange connection not initialized")

        await self._stream_orderbook()

    async def on_finish(self) -> None:
        """
        Close the exchange connection when the emitter stops.
        """
        self._shutdown_requested = True

        if self.exchange:
            try:
                await self.exchange.close()
                logger.info(f"Closed connection to {self.exchange_id}")
            except Exception as e:
                logger.error(f"Error closing exchange connection: {str(e)}")

        await super().on_finish()

    def _format_orderbook_data(
        self, orderbook: Dict[str, Any], timestamp: float, latency: float
    ) -> Dict[str, Any]:
        """
        Format the raw orderbook data for emission.

        Args:
            orderbook: Raw orderbook data from the exchange
            timestamp: Timestamp of the orderbook update
            latency: Latency in milliseconds

        Returns:
            Formatted orderbook data ready for emission
        """
        # Extract the specified depth from the orderbook
        bids = orderbook["bids"][: self.depth] if "bids" in orderbook else []
        asks = orderbook["asks"][: self.depth] if "asks" in orderbook else []

        # Calculate spread if possible
        spread = None
        spread_percent = None
        if bids and asks:
            best_bid = bids[0][0]
            best_ask = asks[0][0]
            spread = best_ask - best_bid
            spread_percent = (spread / best_bid) * 100

        # Format and return the data
        return {
            "symbol": self.symbol,
            "exchange": self.exchange_id,
            "market_type": self.market_type,
            "timestamp": timestamp,
            "latency_ms": latency,
            "bids": bids,
            "asks": asks,
            "spread": spread,
            "spread_percent": spread_percent,
            "depth": self.depth,
        }

    async def _stream_orderbook(self) -> None:
        """
        Stream orderbook data from the exchange and emit events.
        """
        reconnect_delay = self.reconnect_delay

        while not self._shutdown_requested:
            try:
                orderbook = await self.exchange.watch_order_book(self.symbol)

                # Reset reconnect delay on successful fetch
                reconnect_delay = self.reconnect_delay

                # Calculate update time and latency
                now = time.time() * 1000
                timestamp = orderbook.get("timestamp", now)
                latency = now - timestamp if timestamp else 0

                # Format the orderbook data
                formatted_data = self._format_orderbook_data(orderbook, timestamp, latency)

                # Emit the orderbook event
                await self.emit_event(self.orderbook_subject, formatted_data)

                # Include some debug info in logs
                log_data = {
                    "symbol": self.symbol,
                    "best_bid": formatted_data["bids"][0] if formatted_data["bids"] else None,
                    "best_ask": formatted_data["asks"][0] if formatted_data["asks"] else None,
                    "latency_ms": formatted_data["latency_ms"],
                }
                logger.debug(f"Emitted orderbook update: {log_data}")

                # Small delay to prevent flooding
                await asyncio.sleep(0.1)

            except (NetworkError, ExchangeNotAvailable) as e:
                logger.warning(
                    f"Connection issue with {self.exchange_id}: {type(e).__name__}, {str(e)}"
                )
                logger.info(f"Reconnecting in {reconnect_delay:.1f} seconds...")
                await asyncio.sleep(reconnect_delay)
                # Implement exponential backoff (max 30 seconds)
                reconnect_delay = min(reconnect_delay * 1.5, 30.0)

            except Exception as e:
                logger.error(f"Unexpected error in OrderbookEmitter: {type(e).__name__}, {str(e)}")
                if not self._shutdown_requested:
                    logger.info(f"Reconnecting in {reconnect_delay:.1f} seconds...")
                    await asyncio.sleep(reconnect_delay)
