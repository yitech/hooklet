#!/usr/bin/env python3
"""
NATS Manager module for the hooklet

This module provides a class to manage connections to NATS,
register and unregister handler functions for NATS subjects.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

from nats.aio.client import Client as NATS
from nats.aio.subscription import Subscription

from hooklet.base import BasePilot
from hooklet.types import MessageHandlerCallback

logger = logging.getLogger(__name__)


class NatsPilot(BasePilot):
    """
    Class for managing NATS connections and message handlers.

    This class provides functionality to:
    1. Connect to NATS server
    2. Register async handler functions for specific subjects
    3. Unregister handler functions
    4. Publish messages to subjects
    """

    def __init__(self, nats_url: Optional[str] = None):
        """
        Initialize the NATS manager.

        Args:
            nats_url: URL of the NATS server. If None, uses environment variable or default.
        """
        self.nats_url = nats_url or os.environ.get("NATS_URL", "nats://localhost:4222")
        self.nc = NATS()
        self._connected = False
        self._subscriptions: Dict[str, Dict[str, Subscription]] = (
            {}
        )  # {subject: {handler_id: subscription}}
        self._handlers: Dict[str, Dict[str, MessageHandlerCallback]] = (
            {}
        )  # {subject: {handler_id: handler_func}}

    async def connect(self) -> None:
        """
        Connect to the NATS server.

        Raises:
            Exception: If connection to NATS server fails.
        """
        if self._connected:
            logger.debug("Already connected to NATS")
            return

        logger.info(f"Connecting to NATS at {self.nats_url}")
        try:
            await self.nc.connect(self.nats_url)
            self._connected = True
            logger.info("Connected to NATS server successfully")
        except Exception as e:
            logger.error(f"Failed to connect to NATS: {str(e)}")
            raise

    def is_connected(self) -> bool:
        """
        Check if the NATS connection is established.

        Returns:
            True if connected, False otherwise.
        """
        return self._connected

    async def close(self) -> None:
        """
        Close the connection to the NATS server.
        """
        if self._connected:
            logger.info("Closing NATS connection")
            await self.nc.close()
            self._connected = False
            self._subscriptions = {}
            self._handlers = {}

    async def register_handler(
        self, subject: str, handler: MessageHandlerCallback, handler_id: Optional[str] = None
    ) -> str:
        """
        Register a handler function for a specific subject.

        Args:
            subject: NATS subject to subscribe to (can include wildcards).
            handler: Async function to call when a message is received.
            handler_id: Optional unique identifier for this handler. If not provided, uses
                function name.

        Returns:
            Handler ID string.

        Raises:
            ValueError: If handler is not callable or if handler_id already exists for this subject.
            RuntimeError: If not connected to NATS.
        """
        if not self._connected:
            await self.connect()

        if not callable(handler):
            raise ValueError("Handler must be a callable function")

        # Generate a handler ID if not provided
        if handler_id is None:
            handler_id = getattr(handler, "__name__", str(id(handler)))

        # Initialize subject dictionaries if they don't exist
        if subject not in self._subscriptions:
            self._subscriptions[subject] = {}
        if subject not in self._handlers:
            self._handlers[subject] = {}

        # Check if handler_id already exists for this subject
        if handler_id in self._handlers[subject]:
            raise ValueError(
                f"Handler with ID '{handler_id}' already registered for subject '{subject}'"
            )

        # Define wrapper function to handle message decoding and pass to handler
        async def message_wrapper(msg):
            try:
                # Decode message data
                data = json.loads(msg.data.decode()) if msg.data else {}
                # Call user-provided handler with the data
                await handler(data)
            except Exception as e:
                logger.error(f"Error in handler '{handler_id}' for subject '{subject}': {str(e)}")

        # Subscribe to the subject
        sub = await self.nc.subscribe(subject, cb=message_wrapper)

        # Store subscription and handler
        self._subscriptions[subject][handler_id] = sub
        self._handlers[subject][handler_id] = handler

        logger.info(f"Registered handler '{handler_id}' for subject '{subject}'")
        return handler_id

    async def unregister_handler(self, handler_id: str) -> bool:
        """
        Unregister a handler function by its ID.

        Args:
            handler_id: Unique identifier of the handler to unregister.

        Returns:
            None
        """
        if not self._connected:
            logger.warning("Not connected to NATS. Cannot unregister handler.")
            return

        # Search for the handler_id across all subjects
        found = False
        for subject in list(self._handlers.keys()):
            if handler_id in self._handlers[subject]:
                # Found the handler, now unsubscribe from NATS
                sub = self._subscriptions[subject][handler_id]
                await sub.unsubscribe()

                # Remove subscription and handler
                del self._subscriptions[subject][handler_id]
                del self._handlers[subject][handler_id]
                found = True

                # Clean up empty dictionaries
                if not self._subscriptions[subject]:
                    del self._subscriptions[subject]
                if not self._handlers[subject]:
                    del self._handlers[subject]

                logger.info(f"Unregistered handler '{handler_id}' from subject '{subject}'")
                break

        if not found:
            logger.warning(f"Handler '{handler_id}' not found")
        return found

    async def publish(self, subject: str, data: Any) -> None:
        """
        Publish a message to a subject.

        Args:
            subject: NATS subject to publish to.
            data: Data to publish (will be JSON encoded).

        Raises:
            RuntimeError: If not connected to NATS.
        """
        if not self._connected:
            await self.connect()

        # Convert data to JSON and encode as bytes
        encoded_data = json.dumps(data).encode()

        # Publish to NATS
        await self.nc.publish(subject, encoded_data)
        logger.debug(f"Published message to '{subject}'")

    def get_registered_handlers(self) -> Dict[str, List[str]]:
        """
        Get a dictionary of all registered handlers.

        Returns:
            Dictionary mapping subjects to lists of handler IDs.
        """
        result = {}
        for subject, handlers in self._handlers.items():
            result[subject] = list(handlers.keys())
        return result
