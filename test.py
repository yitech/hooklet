#!/usr/bin/env python3
"""
NATS JetStream Demo - Publish and Subscribe Pull Methods

This demo showcases how to use NATS JetStream for reliable message processing
using both publish and subscribe_pull patterns. It demonstrates:

1. Publishing messages to JetStream streams
2. Pull-based subscription for reliable message consumption
3. Message acknowledgment and error handling
4. Stream and consumer management
5. Real-world scenarios like order processing

Requirements:
- NATS server with JetStream enabled
- nats-py library installed
"""

import asyncio
import json
import time
import uuid
from typing import Any, Dict, List
from dataclasses import dataclass, asdict
from datetime import datetime

# NATS imports
from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.aio.msg import Msg as NatsMsg


@dataclass
class Order:
    """Sample order data structure"""
    order_id: str
    customer_id: str
    items: List[Dict[str, Any]]
    total_amount: float
    status: str
    created_at: str


class NatsJetStreamDemo:
    """Demo class for NATS JetStream publish and subscribe_pull operations"""
    
    def __init__(self, nats_url: str = "nats://localhost:4222"):
        self.nats_url = nats_url
        self.nc: NATS = None
        self.js: JetStreamContext = None
        self.connected = False
        
        # Demo configuration - use unique names to avoid conflicts
        import uuid
        unique_id = str(uuid.uuid4())[:8]
        self.stream_name = f"ORDERS_DEMO_{unique_id}"
        self.subject = f"orders.demo.{unique_id}.*"
        self.consumer_name = f"order-processor-{unique_id}"
        
        # Statistics
        self.published_count = 0
        self.processed_count = 0
        self.error_count = 0
    
    async def connect(self):
        """Connect to NATS server and initialize JetStream"""
        try:
            print(f"🔌 Connecting to NATS at {self.nats_url}...")
            self.nc = NATS()
            await self.nc.connect(self.nats_url)
            self.js = self.nc.jetstream()
            self.connected = True
            print("✅ Connected to NATS successfully!")
            
            # Create stream for orders (don't fail if this doesn't work)
            try:
                await self._create_stream()
            except Exception as e:
                print(f"⚠️ Stream creation failed: {e}")
                print("⚠️ Continuing with demo...")
            
        except Exception as e:
            print(f"❌ Failed to connect to NATS: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from NATS"""
        if self.nc and self.connected:
            await self.nc.close()
            self.connected = False
            print("🔌 Disconnected from NATS")
    
    async def _create_stream(self):
        """Create JetStream stream for orders"""
        try:
            # Check if stream exists
            try:
                await self.js.streams_info(self.stream_name)
                print(f"📦 Stream '{self.stream_name}' already exists")
                return
            except Exception:
                # Stream doesn't exist, continue to create it
                pass
            
            # Create new stream
            await self.js.add_stream(
                name=self.stream_name,
                subjects=[self.subject],
                retention="workqueue",  # Work queue retention for order processing
                max_msgs_per_subject=1000,
                max_msgs=10000,
                max_bytes=1024 * 1024 * 10,  # 10MB
                max_age=3600 * 24,  # 24 hours
                max_msg_size=1024 * 1024,  # 1MB
                storage="memory",  # Use memory for demo (faster, no disk I/O)
                num_replicas=1,
                duplicate_window=120,  # 2 minutes
            )
            print(f"📦 Created JetStream stream: {self.stream_name}")
            
        except Exception as e:
            print(f"❌ Failed to create stream: {e}")
            # Don't raise the exception, just log it and continue
            print(f"⚠️ Continuing without stream creation...")
    
    async def _create_consumer(self):
        """Create pull consumer for order processing"""
        try:
            # Check if consumer exists
            try:
                await self.js.consumer_info(self.stream_name, self.consumer_name)
                print(f"👤 Consumer '{self.consumer_name}' already exists")
                return
            except Exception:
                # Consumer doesn't exist, continue to create it
                pass
            
            # Create pull consumer
            await self.js.add_consumer(
                stream=self.stream_name,
                durable_name=self.consumer_name,
                ack_policy="explicit",
                deliver_policy="all",
                filter_subject=self.subject,
                max_deliver=3,  # Retry up to 3 times
                ack_wait=30,  # 30 seconds ack wait
                max_waiting=1,  # Only one pull request at a time
            )
            print(f"👤 Created pull consumer: {self.consumer_name}")
            
        except Exception as e:
            print(f"❌ Failed to create consumer: {e}")
            print(f"⚠️ Continuing without consumer creation...")
    
    def _generate_sample_order(self) -> Order:
        """Generate a sample order for demo purposes"""
        order_id = str(uuid.uuid4())
        customer_id = f"customer_{uuid.uuid4().hex[:8]}"
        
        # Generate random items
        items = []
        num_items = 1 + (hash(order_id) % 5)  # 1-5 items
        for i in range(num_items):
            item = {
                "product_id": f"prod_{uuid.uuid4().hex[:8]}",
                "name": f"Product {i+1}",
                "quantity": 1 + (hash(f"{order_id}_{i}") % 10),
                "price": round(10.0 + (hash(f"{order_id}_{i}") % 100), 2)
            }
            items.append(item)
        
        total_amount = sum(item["quantity"] * item["price"] for item in items)
        
        return Order(
            order_id=order_id,
            customer_id=customer_id,
            items=items,
            total_amount=round(total_amount, 2),
            status="pending",
            created_at=datetime.now().isoformat()
        )
    
    async def publish_order(self, order: Order) -> bool:
        """Publish an order to JetStream"""
        try:
            if not self.connected:
                raise RuntimeError("Not connected to NATS")
            
            # Convert order to dict and add metadata
            order_data = asdict(order)
            order_data.update({
                "published_at": datetime.now().isoformat(),
                "message_id": str(uuid.uuid4())
            })
            
            # Publish to JetStream
            subject = f"orders.demo.{self.stream_name.split('_')[-1]}.{order.customer_id}"
            payload = json.dumps(order_data).encode()
            
            ack = await self.js.publish(subject, payload)
            
            self.published_count += 1
            print(f"📤 Published order {order.order_id} (seq: {ack.seq})")
            return True
            
        except Exception as e:
            self.error_count += 1
            print(f"❌ Failed to publish order {order.order_id}: {e}")
            return False
    
    async def publish_batch_orders(self, count: int = 10):
        """Publish a batch of sample orders"""
        print(f"\n📦 Publishing {count} sample orders...")
        
        tasks = []
        for i in range(count):
            order = self._generate_sample_order()
            task = asyncio.create_task(self.publish_order(order))
            tasks.append(task)
            
            # Small delay between publishes
            await asyncio.sleep(0.1)
        
        # Wait for all publishes to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = sum(1 for r in results if r is True)
        print(f"✅ Published {successful}/{count} orders successfully")
    
    async def process_order(self, msg: NatsMsg) -> bool:
        """Process a single order message"""
        try:
            # Parse message data
            order_data = json.loads(msg.data.decode())
            order_id = order_data["order_id"]
            
            print(f"🔄 Processing order {order_id}...")
            
            # Simulate order processing
            await asyncio.sleep(0.5)  # Simulate processing time
            
            # Simulate some processing logic
            if hash(order_id) % 10 == 0:  # 10% chance of processing error
                raise Exception("Simulated processing error")
            
            # Update order status
            order_data["status"] = "processed"
            order_data["processed_at"] = datetime.now().isoformat()
            
            # Acknowledge the message
            await msg.ack()
            
            self.processed_count += 1
            print(f"✅ Processed order {order_id}")
            return True
            
        except Exception as e:
            self.error_count += 1
            print(f"❌ Failed to process order: {e}")
            
            # Negative acknowledge for retry
            await msg.nak()
            return False
    
    async def subscribe_pull_orders(self, batch_size: int = 5, timeout: int = 30):
        """Subscribe to orders using pull subscription"""
        try:
            # Ensure consumer exists
            await self._create_consumer()
            
            print(f"\n👂 Starting pull subscription (batch_size={batch_size}, timeout={timeout}s)...")
            
            # Create pull subscription
            subscription = await self.js.pull_subscribe(
                subject=self.subject,
                durable=self.consumer_name,
                stream=self.stream_name
            )
            
            print("✅ Pull subscription created, waiting for messages...")
            
            while True:
                try:
                    # Pull messages
                    messages = await subscription.fetch(batch=batch_size, timeout=timeout)
                    
                    if not messages:
                        print("⏰ No messages received, continuing...")
                        continue
                    
                    print(f"📥 Received {len(messages)} messages")
                    
                    # Process messages concurrently
                    tasks = []
                    for msg in messages:
                        task = asyncio.create_task(self.process_order(msg))
                        tasks.append(task)
                    
                    # Wait for all processing to complete
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    successful = sum(1 for r in results if r is True)
                    print(f"✅ Processed {successful}/{len(messages)} messages successfully")
                    
                except Exception as e:
                    print(f"❌ Error in pull subscription: {e}")
                    await asyncio.sleep(1)  # Wait before retrying
                    
        except Exception as e:
            print(f"❌ Failed to create pull subscription: {e}")
            raise
    
    async def get_stream_info(self):
        """Get and display stream information"""
        try:
            info = await self.js.stream_info(self.stream_name)
            print(f"\n📊 Stream Information:")
            print(f"   Name: {info.config.name}")
            print(f"   Subjects: {info.config.subjects}")
            print(f"   Messages: {info.state.messages}")
            print(f"   Bytes: {info.state.bytes}")
            print(f"   Consumers: {info.state.consumer_count}")
            
        except Exception as e:
            print(f"❌ Failed to get stream info: {e}")
    
    async def get_consumer_info(self):
        """Get and display consumer information"""
        try:
            info = await self.js.consumer_info(self.stream_name, self.consumer_name)
            print(f"\n👤 Consumer Information:")
            print(f"   Name: {info.name}")
            print(f"   Durable: {info.config.durable_name}")
            print(f"   Pending: {info.num_pending}")
            print(f"   Delivered: {info.delivered.stream_seq}")
            print(f"   Ack Floor: {info.ack_floor.stream_seq}")
            
        except Exception as e:
            print(f"❌ Failed to get consumer info: {e}")
    
    async def list_all_streams(self):
        """List all available streams"""
        try:
            streams = await self.js.streams_info()
            print(f"\n📋 Available Streams:")
            for stream in streams:
                print(f"   - {stream.config.name} ({stream.state.messages} messages)")
        except Exception as e:
            print(f"❌ Failed to list streams: {e}")
    
    async def delete_stream(self, stream_name: str = None):
        """Delete a stream (use with caution!)"""
        if stream_name is None:
            stream_name = self.stream_name
        
        try:
            await self.js.delete_stream(stream_name)
            print(f"🗑️ Deleted stream: {stream_name}")
        except Exception as e:
            print(f"❌ Failed to delete stream {stream_name}: {e}")
    
    async def purge_stream(self, stream_name: str = None):
        """Purge all messages from a stream"""
        if stream_name is None:
            stream_name = self.stream_name
        
        try:
            await self.js.purge_stream(stream_name)
            print(f"🧹 Purged all messages from stream: {stream_name}")
        except Exception as e:
            print(f"❌ Failed to purge stream {stream_name}: {e}")
    
    def print_statistics(self):
        """Print demo statistics"""
        print(f"\n📈 Demo Statistics:")
        print(f"   Published: {self.published_count}")
        print(f"   Processed: {self.processed_count}")
        print(f"   Errors: {self.error_count}")
        print(f"   Success Rate: {(self.processed_count / max(self.published_count, 1)) * 100:.1f}%")


async def simple_demo():
    """Simple demo showing basic publish and subscribe_pull operations"""
    print("🚀 Simple NATS JetStream Demo")
    print("=" * 40)
    
    demo = NatsJetStreamDemo()
    
    try:
        # Connect to NATS
        await demo.connect()
        
        # Publish a few orders
        await demo.publish_batch_orders(5)
        
        # Show stream info
        await demo.get_stream_info()
        
        # Process orders with pull subscription
        print("\n🔄 Processing orders...")
        processing_task = asyncio.create_task(
            demo.subscribe_pull_orders(batch_size=2, timeout=5)
        )
        
        # Let it run for a few seconds
        await asyncio.sleep(3)
        
        # Cancel processing
        processing_task.cancel()
        try:
            await processing_task
        except asyncio.CancelledError:
            pass
        
        # Show results
        demo.print_statistics()
        
    except Exception as e:
        print(f"❌ Simple demo failed: {e}")
    finally:
        await demo.disconnect()
    
    print("✅ Simple demo completed!")


async def run_demo():
    """Run the complete NATS JetStream demo"""
    print("🚀 Starting NATS JetStream Demo")
    print("=" * 60)
    
    demo = NatsJetStreamDemo()
    
    try:
        # Connect to NATS
        await demo.connect()
        
        # Show initial stream info
        await demo.get_stream_info()
        
        # Publish sample orders
        await demo.publish_batch_orders(15)
        
        # Show updated stream info
        await demo.get_stream_info()
        
        # Start pull subscription in background
        print("\n🔄 Starting order processing...")
        processing_task = asyncio.create_task(
            demo.subscribe_pull_orders(batch_size=3, timeout=10)
        )
        
        # Publish more orders while processing
        await asyncio.sleep(2)
        await demo.publish_batch_orders(5)
        
        # Let processing continue for a bit
        await asyncio.sleep(5)
        
        # Cancel processing task
        processing_task.cancel()
        try:
            await processing_task
        except asyncio.CancelledError:
            pass
        
        # Show final statistics
        demo.print_statistics()
        await demo.get_consumer_info()
        
    except KeyboardInterrupt:
        print("\n⏹️ Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
    finally:
        await demo.disconnect()
    
    print("\n" + "=" * 60)
    print("✅ NATS JetStream Demo completed!")


if __name__ == "__main__":
    import sys
    
    # Check command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "simple":
        # Run simple demo
        asyncio.run(simple_demo())
    else:
        # Run full demo
        asyncio.run(run_demo())
