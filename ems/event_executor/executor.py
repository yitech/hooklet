#!/usr/bin/env python3
"""
Publisher module for the EMS (Exchange Management System).
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List
import uuid

from ems.nats_manager import NatsManager
import time

logger = logging.getLogger(__name__)



class EventExecutor(ABC):
    """
    Base class for event executor.
    
    This abstract class provides the structure for event driven class
    """

    def __init__(self, nats_manager: NatsManager, executor_id: Optional[str] = None):
        """
        Initialize the publisher.
        
        Args:
            subject: The NATS subject to publish to
            nats_manager: instance of NatsManager
        """
        self._nats_manager = nats_manager
        self._executor_id = executor_id or uuid.uuid4().hex
        self._running: bool = False
        self._created_at: int = time.time()

    async def start(self) -> None:
        """
        Start the publisher.
        
        Connects to NATS if not already connected and begins publishing data.
        """
        logger.info(f"Starting publisher for {self.subject}")

        if not self._nats_manager._connected:
            await self._nats_manager.connect()

        await self.on_start()

        self._running = True
        await self._run_publisher()

    async def on_start(self) -> None:
        """
        Called when the executor starts.
        
        This method can be overridden to perform any setup tasks needed
        when the executor starts.
        """
        pass

    async def stop(self) -> None:
        """
        Stop the publisher.
        
        Sets running flag to False and allows the publisher to stop gracefully.
        """
        self._running = False
        await self.on_finish()

    async def on_finish(self) -> None:
        """
        Called when the executor stops.
        
        This method can be overridden to perform any cleanup tasks needed
        when the executor stops.
        """
        pass

    async def status(self) -> Dict[str, Any]:
        """
        Get the status of the executor.
        
        Returns:
            A dictionary containing the status of the executor.
        """
        return {
            "executor_id": self._executor_id,
            "running": self._running,
            "created_at": self._created_at,
        }
    
    async def on_status(self) -> None:
        """
        Called when the executor status is requested.
        
        This method can be overridden to perform any tasks needed
        when the executor status is requested.
        """
        pass

