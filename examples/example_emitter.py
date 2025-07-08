#!/usr/bin/env python3
"""
Random Number Emitter Example

This example demonstrates how to create an emitter that continuously
emits random positive numbers and uniform(0,1) values every second.
"""

import asyncio
import random
import time
from typing import Any, Dict, AsyncGenerator

from hooklet.node.emitter import Emitter
from hooklet.pilot.inproc_pilot import InprocPilot
from hooklet.pilot.nats_pilot import NatsPilot
from hooklet.base.types import Msg
from hooklet.utils.id_generator import generate_id


class RandomNumberEmitter(Emitter):
    """Emitter that generates random positive numbers and uniform(0,1) values"""
    
    def __init__(self, name: str, pubsub, router):
        super().__init__(name, pubsub, router)
        self.emit_count = 0
    
    async def emit(self) -> AsyncGenerator[Msg, None]:
        """Emit random numbers every second"""
        while not self.shutdown_event.is_set():
            # Generate random positive integer
            random_int = random.randint(1, 1000)
            
            # Generate uniform(0,1) value
            uniform_value = random.uniform(0, 1)
            
            # Create message with both values
            msg: Msg = {
                "_id": generate_id(),
                "type": "random_numbers",
                "data": {
                    "positive_integer": random_int,
                    "uniform_value": uniform_value,
                    "timestamp": time.time(),
                    "emit_count": self.emit_count
                },
                "error": None
            }
            
            self.emit_count += 1
            yield msg
            
            # Wait for 1 second before next emission
            await asyncio.sleep(1)


class NumberSubscriber:
    """Simple subscriber to demonstrate receiving emitted messages"""
    
    def __init__(self, name: str, pubsub):
        self.name = name
        self.pubsub = pubsub
        self.received_count = 0
    
    async def start(self):
        """Start listening for messages"""
        print(f"📡 {self.name} started listening for random numbers...")
        
        async def message_handler(msg: Msg):
            self.received_count += 1
            data = msg.get('data', {})
            
            print(f"📨 {self.name} received message #{self.received_count}:")
            print(f"   ID: {msg.get('_id')}")
            print(f"   Positive Integer: {data.get('positive_integer')}")
            print(f"   Uniform Value: {data.get('uniform_value'):.4f}")
            print(f"   Timestamp: {data.get('timestamp'):.2f}")
            print(f"   Emit Count: {data.get('emit_count')}")
            print()
        
        self.subscription_id = await self.pubsub.subscribe("random_numbers", message_handler)
    
    async def stop(self):
        """Stop listening for messages"""
        if hasattr(self, 'subscription_id'):
            await self.pubsub.unsubscribe("random_numbers", self.subscription_id)
        print(f"🛑 {self.name} stopped listening")


def router(msg: Msg) -> str:
    """Simple router that routes all messages to 'random_numbers' subject"""
    return "random_numbers"


async def run_demo():
    """Run the random number emitter demo"""
    print("🚀 Starting Random Number Emitter Demo")
    print("=" * 60)
    
    # Create pilot and connect
    pilot = InprocPilot()
    # pilot = NatsPilot(
    #     nats_url="nats://localhost:4222"
    # )
    await pilot.connect()
    
    # Create emitter and subscriber
    emitter = RandomNumberEmitter("random-emitter", pilot.pubsub(), router)
    subscriber = NumberSubscriber("number-subscriber", pilot.pubsub())
    
    # Start subscriber first
    await subscriber.start()
    
    # Start emitter
    await emitter.start()
    print("✅ Random number emitter started")
    print("📊 Emitting random positive integers and uniform(0,1) values every second...")
    print("=" * 60)
    
    # Let it run for 10 seconds
    try:
        await asyncio.sleep(10)
    except KeyboardInterrupt:
        print("\n⏹️  Demo interrupted by user")
    
    # Stop everything
    await emitter.close()
    await subscriber.stop()
    
    print("=" * 60)
    print(f"✅ Demo completed! Received {subscriber.received_count} messages")


if __name__ == "__main__":
    try:
        asyncio.run(run_demo())
    except KeyboardInterrupt:
        print("\n👋 Demo stopped by user")
