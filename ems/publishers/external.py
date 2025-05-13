#!/usr/bin/env python3
"""
External API publishers for EMS.

This module provides publishers for connecting to external APIs
and publishing data from them.
"""

import asyncio
import logging
import random
from typing import Any, Dict, Optional

from ..publisher import StreamPublisher, IntervalPublisher
from ..nats_manager import NatsManager

logger = logging.getLogger(__name__)


class RestApiPublisher(IntervalPublisher):
    """
    Publisher for data from RESTful APIs.
    
    Fetches data from a REST API at regular intervals and publishes it.
    """
    
    def __init__(self, endpoint: str, subject: str, interval_seconds: float = 5.0,
                 auth_headers: Optional[Dict[str, str]] = None,
                 nats_manager: Optional[NatsManager] = None):
        """
        Initialize the REST API publisher.
        
        Args:
            endpoint: URL of the REST API endpoint
            subject: NATS subject to publish to
            interval_seconds: Time between API calls in seconds
            auth_headers: Optional authentication headers
            nats_manager: Optional instance of NatsManager
        """
        super().__init__(subject, interval_seconds, nats_manager)
        self.endpoint = endpoint
        self.auth_headers = auth_headers or {}
        self.client = None
        
    async def setup(self) -> None:
        """Set up the HTTP client for API calls."""
        import aiohttp
        
        if self.client is None:
            self.client = aiohttp.ClientSession(headers=self.auth_headers)
            logger.info(f"Created HTTP client for {self.endpoint}")
    
    async def get_data(self) -> Any:
        """
        Fetch data from the REST API.
        
        Returns:
            Data received from the API
        """
        import aiohttp
        
        # Ensure client is set up
        if self.client is None:
            await self.setup()
        
        try:
            async with self.client.get(self.endpoint) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    logger.error(f"API call failed with status {response.status}: {await response.text()}")
                    return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Error calling API {self.endpoint}: {str(e)}")
            return None
    
    def on_publish(self, data: Any) -> None:
        """Log when data is published."""
        if data is not None:
            logger.info(f"Published data from {self.endpoint} to {self.subject}")
    
    async def stop(self) -> None:
        """Stop the publisher and clean up resources."""
        await super().stop()
        
        if self.client is not None:
            await self.client.close()
            self.client = None
            logger.info(f"Closed HTTP client for {self.endpoint}")


class WebsocketPublisher(StreamPublisher):
    """
    Publisher for data from websocket APIs.
    
    Connects to a websocket endpoint and publishes received events.
    """
    
    def __init__(self, endpoint: str, subject: str,
                 auth_headers: Optional[Dict[str, str]] = None,
                 subscription_message: Optional[Dict[str, Any]] = None,
                 nats_manager: Optional[NatsManager] = None):
        """
        Initialize the websocket publisher.
        
        Args:
            endpoint: URL of the websocket endpoint
            subject: NATS subject to publish to
            auth_headers: Optional authentication headers
            subscription_message: Optional message to send after connection to subscribe
            nats_manager: Optional instance of NatsManager
        """
        super().__init__(subject, nats_manager)
        self.endpoint = endpoint
        self.auth_headers = auth_headers or {}
        self.subscription_message = subscription_message
        self.ws = None
        
    async def connect_to_stream(self) -> Any:
        """
        Connect to the websocket stream.
        
        Returns:
            Websocket connection object
        """
        import aiohttp
        
        # Create HTTP session with headers
        session = aiohttp.ClientSession(headers=self.auth_headers)
        
        # Connect to the websocket
        self.ws = await session.ws_connect(self.endpoint)
        logger.info(f"Connected to websocket at {self.endpoint}")
        
        # Send subscription message if provided
        if self.subscription_message:
            await self.ws.send_json(self.subscription_message)
            logger.info(f"Sent subscription message to {self.endpoint}")
        
        return self.ws
        
    async def get_stream_data(self, stream: Any) -> Any:
        """
        Get data from the websocket stream.
        
        Args:
            stream: The websocket connection
            
        Returns:
            Data received from the websocket
        """
        import aiohttp
        import json
        
        # Wait for the next message
        msg = await stream.receive()
        
        # Handle different message types
        if msg.type == aiohttp.WSMsgType.TEXT:
            try:
                return json.loads(msg.data)
            except json.JSONDecodeError:
                return {"text": msg.data}
        elif msg.type == aiohttp.WSMsgType.BINARY:
            return {"binary": True, "data": msg.data}
        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
            logger.warning(f"Websocket connection closed or error: {msg}")
            return None
        else:
            return {"type": str(msg.type), "data": msg.data}
        
    async def disconnect_from_stream(self, stream: Any) -> None:
        """
        Disconnect from the websocket stream.
        
        Args:
            stream: The websocket connection
        """
        if stream and not stream.closed:
            await stream.close()
            
        # Close the session
        if hasattr(stream, "session") and not stream.session.closed:
            await stream.session.close()
            
        logger.info(f"Disconnected from websocket at {self.endpoint}")
