#!/usr/bin/env python3
"""
Strategy module for the EMS (Exchange Management System).

This module provides a base Strategy class that can be extended to implement
specific trading strategies using NATS message handlers.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List

from .nats_manager import NatsManager

logger = logging.getLogger(__name__)

class Strategy(ABC):
    """
    Base class for implementing trading strategies.
    
    This class provides the basic structure for creating trading strategies
    that can subscribe to and handle NATS messages for market data, orders,
    and other events.
    """
    
    def __init__(self, nats_manager: NatsManager, strategy_id: Optional[str] = None):
        """
        Initialize the strategy.
        
        Args:
            nats_manager: Instance of NatsManager for handling NATS communications
            strategy_id: Optional unique identifier for this strategy instance
        """
        self._nats_manager = nats_manager
        self.strategy_id = strategy_id or self.__class__.__name__
        self._registered_handlers: Dict[str, List[str]] = {}
        self._running = False
    
    async def start(self) -> None:
        """
        Start the strategy by registering all handlers.
        
        This method should be called to activate the strategy.
        It ensures the strategy is connected to NATS and registers all handlers.
        """
        logger.info(f"Starting strategy: {self.strategy_id}")
        
        if not self._nats_manager._connected:
            await self._nats_manager.connect()
            
        self._running = True
        await self._register_handlers()
        await self.on_start()
    
    async def stop(self) -> None:
        """
        Stop the strategy by unregistering all handlers.
        
        This method should be called to deactivate the strategy.
        It unregisters all handlers and performs cleanup.
        """
        logger.info(f"Stopping strategy: {self.strategy_id}")
        self._running = False
        await self._unregister_handlers()
        await self.on_stop()

    
    async def _register_handlers(self) -> None:
        """
        Register all handlers defined in get_handlers().
        """
        try:
            handlers = self.get_handlers()
            for subject, handler in handlers.items():
                handler_id = f"{self.strategy_id}_{subject}"
                try:
                    await self._nats_manager.register_handler(subject, handler, handler_id)
                    if subject not in self._registered_handlers:
                        self._registered_handlers[subject] = []
                    self._registered_handlers[subject].append(handler_id)
                    logger.debug(f"Registered handler for {subject} with ID {handler_id}")
                except Exception as e:
                    logger.error(f"Failed to register handler for {subject}: {str(e)}")
        except Exception as e:
            logger.error(f"Error in strategy {self.strategy_id}: {str(e)}")
            raise
    
    async def _unregister_handlers(self) -> None:
        """
        Unregister all handlers that were registered by this strategy.
        """
        try:
            for subject, handler_ids in self._registered_handlers.items():
                for handler_id in handler_ids:
                    try:
                        success = await self._nats_manager.unregister_handler(subject, handler_id)
                        if success:
                            logger.debug(f"Unregistered handler {handler_id} from {subject}")
                        else:
                            logger.warning(f"Failed to unregister handler {handler_id} from {subject}")
                    except Exception as e:
                        logger.error(f"Error unregistering handler {handler_id}: {str(e)}")
            self._registered_handlers.clear()
        except Exception as e:
            logger.error(f"Error in strategy {self.strategy_id}: {str(e)}")
            raise
    
    async def publish(self, subject: str, data: Any) -> None:
        """
        Publish a message to a NATS subject.
        
        Args:
            subject: The NATS subject to publish to
            data: The data to publish (will be JSON encoded)
        """
        await self._nats_manager.publish(subject, data)
        logger.debug(f"Published data to {subject}")
    
    @abstractmethod
    def get_handlers(self) -> Dict[str, Any]:
        """
        Get the mapping of subjects to handler functions.
        
        This method must be implemented by subclasses to define which
        NATS subjects they want to subscribe to and what handler functions
        should be called when messages are received.
        
        Returns:
            Dictionary mapping subject strings to handler functions
        """
        raise NotImplementedError("Strategies must implement get_handlers()")
    
    async def on_start(self) -> None:
        """
        Called after all handlers are registered.
        
        Override this method to perform any initialization needed
        when the strategy starts.
        """
        pass
    
    async def on_stop(self) -> None:
        """
        Called after all handlers are unregistered.
        
        Override this method to perform any cleanup needed
        when the strategy stops.
        """
        pass
