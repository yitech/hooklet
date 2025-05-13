#!/usr/bin/env python3
"""
Binance data publishers for EMS.

This module provides publishers for Binance API streams including
user data, market data, and other Binance-specific information.
"""

import logging
from typing import Any, Optional

from ..publisher import StreamPublisher
from ..nats_manager import NatsManager

logger = logging.getLogger(__name__)


class BinanceUserDataPublisher(StreamPublisher):
    """
    Publisher for Binance user data stream events.
    
    Connects to Binance user data stream and publishes received events.
    """
    
    def __init__(self, client, subject: str, 
                 nats_manager: Optional[NatsManager] = None):
        """
        Initialize the Binance user data publisher.
        
        Args:
            client: Binance AsyncClient instance
            subject: NATS subject to publish to
            nats_manager: Optional instance of NatsManager
        """
        super().__init__(subject, nats_manager)
        self.client = client
        self.bsm = None
        
    async def connect_to_stream(self) -> Any:
        """
        Connect to Binance user data stream.
        
        Returns:
            Stream object for receiving data
        """
        from binance import BinanceSocketManager
        
        # Get the Futures listenKey
        listen_key = await self.client.futures_stream_get_listen_key()
        logger.info(f"Obtained futures listen key: {listen_key[:5]}...")

        # Create socket manager for Futures
        self.bsm = BinanceSocketManager(self.client)
        
        # Get the user data socket for Futures
        socket = self.bsm.futures_user_socket()
        
        # Create the stream
        stream = await socket.__aenter__()
        logger.info("Connected to Binance Futures user data stream")
        return stream
        
    async def get_stream_data(self, stream: Any) -> Any:
        """
        Get data from the Binance stream.
        
        Args:
            stream: The stream object returned by connect_to_stream
            
        Returns:
            Event data from Binance
        """
        return await stream.recv()
        
    async def disconnect_from_stream(self, stream: Any) -> None:
        """
        Disconnect from the Binance stream.
        
        Args:
            stream: The stream object returned by connect_to_stream
        """
        if self.bsm:
            # Close the socket context manager
            await stream.socket.__aexit__(None, None, None)
            
        if self.client:
            await self.client.close_connection()
            
        logger.info("Disconnected from Binance Futures user data stream")


class BinanceMarketStreamPublisher(StreamPublisher):
    """
    Publisher for Binance market data streams.
    
    Connects to Binance market data stream and publishes received events.
    """
    
    def __init__(self, client, symbol: str, stream_type: str, 
                 nats_manager: Optional[NatsManager] = None):
        """
        Initialize the Binance market stream publisher.
        
        Args:
            client: Binance AsyncClient instance
            symbol: Trading symbol (e.g., "BTCUSDT")
            stream_type: Type of market data stream ("trade", "kline", "depth", etc.)
            nats_manager: Optional instance of NatsManager
        """
        subject = f"market.binance.{stream_type}.{symbol.lower()}"
        super().__init__(subject, nats_manager)
        self.client = client
        self.symbol = symbol
        self.stream_type = stream_type
        self.bsm = None
        
    async def connect_to_stream(self) -> Any:
        """
        Connect to Binance market data stream.
        
        Returns:
            Stream object for receiving data
        """
        from binance import BinanceSocketManager
        
        # Create socket manager
        self.bsm = BinanceSocketManager(self.client)
        
        # Get the appropriate socket based on stream type
        if self.stream_type == "trade":
            socket = self.bsm.trade_socket(self.symbol)
        elif self.stream_type == "kline":
            socket = self.bsm.kline_socket(self.symbol)
        elif self.stream_type == "depth":
            socket = self.bsm.depth_socket(self.symbol)
        elif self.stream_type == "ticker":
            socket = self.bsm.symbol_ticker_socket(self.symbol)
        elif self.stream_type == "miniticker":
            socket = self.bsm.symbol_miniticker_socket(self.symbol)
        else:
            raise ValueError(f"Unsupported stream type: {self.stream_type}")
        
        # Create the stream
        stream = await socket.__aenter__()
        logger.info(f"Connected to Binance {self.stream_type} stream for {self.symbol}")
        return stream
        
    async def get_stream_data(self, stream: Any) -> Any:
        """
        Get data from the Binance stream.
        
        Args:
            stream: The stream object returned by connect_to_stream
            
        Returns:
            Event data from Binance
        """
        return await stream.recv()
        
    async def disconnect_from_stream(self, stream: Any) -> None:
        """
        Disconnect from the Binance stream.
        
        Args:
            stream: The stream object returned by connect_to_stream
        """
        if self.bsm:
            # Close the socket context manager
            await stream.socket.__aexit__(None, None, None)
            
        logger.info(f"Disconnected from Binance {self.stream_type} stream for {self.symbol}")
