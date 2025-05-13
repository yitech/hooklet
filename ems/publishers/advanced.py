#!/usr/bin/env python3
"""
Advanced publishers for EMS.

This module provides more sophisticated publishers with advanced features
like combining multiple data sources, filtering, transforming, etc.
"""

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Union

from ..publisher import Publisher, IntervalPublisher
from ..nats_manager import NatsManager

logger = logging.getLogger(__name__)


class FilterPublisher(Publisher):
    """
    Publisher that filters data from a source publisher.
    
    Takes data from a source publisher, applies a filter function,
    and only publishes data that passes the filter.
    """
    
    def __init__(self, source_publisher: Publisher, filter_func: Callable[[Any], bool],
                 subject: Optional[str] = None, nats_manager: Optional[NatsManager] = None):
        """
        Initialize the filter publisher.
        
        Args:
            source_publisher: Source publisher to get data from
            filter_func: Function that returns True for data to publish
            subject: Optional override for the subject to publish to
            nats_manager: Optional instance of NatsManager
        """
        subject = subject or source_publisher.subject
        super().__init__(subject, nats_manager or source_publisher._nats_manager)
        self.source_publisher = source_publisher
        self.filter_func = filter_func
        self._original_publish = source_publisher.publish
        
    async def _run_publisher(self) -> None:
        """Run the filter publisher."""
        # Replace the source publisher's publish method with our filtered version
        self.source_publisher.publish = self._filtered_publish
        
        # Start the source publisher
        await self.source_publisher.start()
    
    async def _filtered_publish(self, data: Any) -> None:
        """Filter the data and publish if it passes."""
        if self.filter_func(data):
            await super().publish(data)
        
    async def stop(self) -> None:
        """Stop the publisher and restore the source publisher."""
        await super().stop()
        
        # Restore the original publish method
        if hasattr(self, '_original_publish'):
            self.source_publisher.publish = self._original_publish
        
        # Stop the source publisher
        await self.source_publisher.stop()


class TransformPublisher(Publisher):
    """
    Publisher that transforms data from a source publisher.
    
    Takes data from a source publisher, applies a transform function,
    and publishes the transformed data.
    """
    
    def __init__(self, source_publisher: Publisher, transform_func: Callable[[Any], Any],
                 subject: Optional[str] = None, nats_manager: Optional[NatsManager] = None):
        """
        Initialize the transform publisher.
        
        Args:
            source_publisher: Source publisher to get data from
            transform_func: Function that transforms the data
            subject: Optional override for the subject to publish to
            nats_manager: Optional instance of NatsManager
        """
        subject = subject or source_publisher.subject
        super().__init__(subject, nats_manager or source_publisher._nats_manager)
        self.source_publisher = source_publisher
        self.transform_func = transform_func
        self._original_publish = source_publisher.publish
        
    async def _run_publisher(self) -> None:
        """Run the transform publisher."""
        # Replace the source publisher's publish method with our transformed version
        self.source_publisher.publish = self._transformed_publish
        
        # Start the source publisher
        await self.source_publisher.start()
    
    async def _transformed_publish(self, data: Any) -> None:
        """Transform the data and publish it."""
        transformed_data = self.transform_func(data)
        await super().publish(transformed_data)
        
    async def stop(self) -> None:
        """Stop the publisher and restore the source publisher."""
        await super().stop()
        
        # Restore the original publish method
        if hasattr(self, '_original_publish'):
            self.source_publisher.publish = self._original_publish
        
        # Stop the source publisher
        await self.source_publisher.stop()


class MergePublisher(IntervalPublisher):
    """
    Publisher that merges data from multiple sources.
    
    Takes the latest data from multiple source publishers and
    merges them into a single message.
    """
    
    def __init__(self, publishers: List[Publisher], subject: str,
                 interval_seconds: float = 1.0, merge_func: Optional[Callable] = None,
                 nats_manager: Optional[NatsManager] = None):
        """
        Initialize the merge publisher.
        
        Args:
            publishers: List of publishers to get data from
            subject: Subject to publish the merged data to
            interval_seconds: Time between publications in seconds
            merge_func: Optional function to customize the merging logic
            nats_manager: Optional instance of NatsManager
        """
        super().__init__(subject, interval_seconds, nats_manager)
        self.publishers = publishers
        self.merge_func = merge_func
        self.latest_data = {id(pub): None for pub in publishers}
        
    async def setup(self) -> None:
        """Set up the publisher with hooks into source publishers."""
        # Create hooks for each source publisher
        for publisher in self.publishers:
            original_publish = publisher.publish
            publisher_id = id(publisher)
            
            # Create a closure to capture the publisher_id
            async def capture_latest(data: Any, pub_id=publisher_id, orig_pub=original_publish) -> None:
                # Store the latest data
                self.latest_data[pub_id] = data
                # Call the original publish method
                await orig_pub(data)
            
            # Replace the publish method with our capturing version
            publisher.publish = capture_latest
    
    async def get_data(self) -> Dict[str, Any]:
        """
        Merge the latest data from all sources.
        
        Returns:
            Merged data from all sources
        """
        # Basic merge: combine all data into a dictionary
        if self.merge_func is None:
            merged = {}
            for publisher_id, data in self.latest_data.items():
                if data is not None:
                    pub_key = f"source_{publisher_id}"
                    merged[pub_key] = data
            return merged
        else:
            # Use the custom merge function
            return self.merge_func(self.latest_data)
    
    async def _run_publisher(self) -> None:
        """Run the merge publisher and all source publishers."""
        # Set up the captures
        await self.setup()
        
        # Start all source publishers
        for publisher in self.publishers:
            asyncio.create_task(publisher.start())
        
        # Run the normal interval publisher logic
        await super()._run_publisher()
    
    async def stop(self) -> None:
        """Stop the publisher and all source publishers."""
        await super().stop()
        
        # Stop all source publishers
        for publisher in self.publishers:
            await publisher.stop()


class BroadcastPublisher(Publisher):
    """
    Publisher that broadcasts the same data to multiple subjects.
    
    Takes data from a source publisher and broadcasts it to multiple subjects.
    """
    
    def __init__(self, source_publisher: Publisher, subjects: List[str],
                 nats_manager: Optional[NatsManager] = None):
        """
        Initialize the broadcast publisher.
        
        Args:
            source_publisher: Source publisher to get data from
            subjects: List of subjects to broadcast to
            nats_manager: Optional instance of NatsManager
        """
        # Use the first subject as the "primary" subject
        super().__init__(subjects[0], nats_manager or source_publisher._nats_manager)
        self.source_publisher = source_publisher
        self.subjects = subjects
        self._original_publish = source_publisher.publish
        
    async def _run_publisher(self) -> None:
        """Run the broadcast publisher."""
        # Replace the source publisher's publish method with our broadcasting version
        self.source_publisher.publish = self._broadcast_publish
        
        # Start the source publisher
        await self.source_publisher.start()
    
    async def _broadcast_publish(self, data: Any) -> None:
        """Broadcast the data to all subjects."""
        for subject in self.subjects:
            await self._nats_manager.publish(subject, data)
        
    async def stop(self) -> None:
        """Stop the publisher and restore the source publisher."""
        await super().stop()
        
        # Restore the original publish method
        if hasattr(self, '_original_publish'):
            self.source_publisher.publish = self._original_publish
        
        # Stop the source publisher
        await self.source_publisher.stop()
