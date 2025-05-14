#!/usr/bin/env python3
"""
Publisher module for the EMS (Exchange Management System).
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List

from .nats_manager import NatsManager

logger = logging.getLogger(__name__)


class Publisher(ABC):
    """
    Base class for data publishers.
    
    This abstract class provides the structure for creating data publishers
    that can publish data to NATS subjects.
    """
    
    def __init__(self, nats_manager: NatsManager):
        """
        Initialize the publisher.
        
        Args:
            subject: The NATS subject to publish to
            nats_manager: Optional instance of NatsManager. If None, a new one will be created.
        """
        self._nats_manager = nats_manager
        self._running: bool = False
        
    async def start(self) -> None:
        """
        Start the publisher.
        
        Connects to NATS if not already connected and begins publishing data.
        """
        logger.info(f"Starting publisher for {self.subject}")
        
        if not self._nats_manager._connected:
            await self._nats_manager.connect()
            
        self._running = True
        await self._run_publisher()
    
    async def stop(self) -> None:
        """
        Stop the publisher.
        
        Sets running flag to False and allows the publisher to stop gracefully.
        """
        self._running = False
        logger.info(f"Publisher for {self.subject} stopping...")
    
    async def close(self) -> None:
        """
        Close the NATS connection.
        
        Should be called when done with the publisher.
        """
        await self._nats_manager.close()
        logger.info(f"Publisher for {self.subject} closed.")
    
    async def publish(self, subject: str, data: Any) -> None:
        """
        Publish data to the configured subject.
        
        Args:
            data: The data to publish (will be JSON encoded)
        """
        await self._nats_manager.publish(subject, data)
        logger.debug(f"Published data to {subject}")
    
    async def _run_publisher(self) -> None:
        """
        Main publisher loop.
        
        This method must be implemented by subclasses to define the
        specific publishing behavior.
        
        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("Publishers must implement _run_publisher()")


class IntervalPublisher(Publisher):
    """
    Publisher that publishes data at regular intervals.
    
    This class is useful for simulated data or periodic updates.
    """
    
    def __init__(self, subject: str, nats_manager: NatsManager, interval_seconds: float):
        """
        Initialize the interval publisher.
        
        Args:
            subject: The NATS subject to publish to
            interval_seconds: Time between publications in seconds
            nats_manager: Optional instance of NatsManager
        """
        super().__init__(subject, nats_manager)
        self.interval_seconds = interval_seconds
    
    async def _run_publisher(self) -> None:
        """
        Run the publisher loop, publishing at regular intervals.
        """
        try:
            while self._running:
                data = await self.get_data()
                if data is not None:
                    await self.publish(data)
                    self.on_publish(data)
                await asyncio.sleep(self.interval_seconds)
        except asyncio.CancelledError:
            logger.info(f"IntervalPublisher for {self.subject} cancelled")
        except Exception as e:
            logger.error(f"Error in IntervalPublisher for {self.subject}: {str(e)}")
            raise
    
    async def get_data(self) -> Any:
        """
        Get the data to publish.
        
        This method must be implemented by subclasses to provide
        the data that should be published in each interval.
        
        Returns:
            The data to publish or None if no data should be published this time
            
        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("IntervalPublishers must implement get_data()")
    
    def on_publish(self, data: Any) -> None:
        """
        Called after data is published.
        
        Can be overridden by subclasses to perform actions after publishing.
        
        Args:
            data: The data that was published
        """
        logger.debug(f"Published: {data} to {self.subject}")


class StreamPublisher(Publisher):
    """
    Publisher that streams data from an external source.
    
    This class is useful for external data sources like websockets, API streams, etc.
    """
    
    async def _run_publisher(self) -> None:
        """
        Run the publisher loop, streaming from an external source.
        """
        try:
            stream = await self.connect_to_stream()
            while self._running:
                data = await self.get_stream_data(stream)
                if data is not None:
                    await self.publish(data)
                    self.on_publish(data)
        except asyncio.CancelledError:
            logger.info(f"StreamPublisher for {self.subject} cancelled")
        except Exception as e:
            logger.error(f"Error in StreamPublisher for {self.subject}: {str(e)}")
            raise
        finally:
            await self.disconnect_from_stream(stream)
    
    async def connect_to_stream(self) -> Any:
        """
        Connect to the data stream.
        
        This method must be implemented by subclasses to establish
        a connection to the external data source.
        
        Returns:
            Stream object or connection that will be passed to get_stream_data
            
        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("StreamPublishers must implement connect_to_stream()")
    
    async def get_stream_data(self, stream: Any) -> Any:
        """
        Get data from the stream.
        
        This method must be implemented by subclasses to receive
        data from the external stream.
        
        Args:
            stream: The stream object returned by connect_to_stream
            
        Returns:
            The data to publish or None if no data should be published
            
        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("StreamPublishers must implement get_stream_data()")
    
    async def disconnect_from_stream(self, stream: Any) -> None:
        """
        Disconnect from the data stream.
        
        Can be overridden by subclasses to properly close the stream connection.
        
        Args:
            stream: The stream object returned by connect_to_stream
        """
        pass
    
    def on_publish(self, data: Any) -> None:
        """
        Called after data is published.
        
        Can be overridden by subclasses to perform actions after publishing.
        
        Args:
            data: The data that was published
        """
        logger.info(f"Published: {data} to {self.subject}")
