#!/usr/bin/env python3
"""
Example demonstrating the usage of NatsPilot for pub/sub and request-reply patterns.
This example requires a running NATS server at nats://localhost:4222
"""

import asyncio
import json
from hooklet.pilot import NatsPilot
from hooklet.base.types import Msg

async def pubsub_example():
    """Demonstrate pub/sub functionality"""
    print("=== Pub/Sub Example ===")
    
    # Create two pilots for publisher and subscriber
    publisher = NatsPilot(nats_url="nats://localhost:4222")
    subscriber = NatsPilot(nats_url="nats://localhost:4222")
    
    try:
        # Connect both pilots
        await publisher.connect()
        await subscriber.connect()
        
        # Set up subscriber
        received_messages = []
        
        async def message_handler(msg: Msg):
            received_messages.append(msg)
            print(f"Received message: {msg}")
        
        # Subscribe to a topic
        pubsub = subscriber.pubsub()
        subscription_id = await pubsub.subscribe("news.tech", message_handler)
        print(f"Subscribed to 'news.tech' with ID: {subscription_id}")
        
        # Publish some messages
        publisher_pubsub = publisher.pubsub()
        
        messages = [
            {"title": "AI Breakthrough", "content": "New AI model achieves 99% accuracy"},
            {"title": "Quantum Computing", "content": "Quantum computer solves complex problem"},
            {"title": "Blockchain News", "content": "New blockchain platform launched"}
        ]
        
        for msg in messages:
            await publisher_pubsub.publish("news.tech", msg)
            print(f"Published: {msg['title']}")
            await asyncio.sleep(0.5)  # Small delay between messages
        
        # Wait for messages to be processed
        await asyncio.sleep(1)
        
        print(f"Total messages received: {len(received_messages)}")
        
        # Unsubscribe
        await pubsub.unsubscribe("news.tech", subscription_id)
        print("Unsubscribed from 'news.tech'")
        
    except Exception as e:
        print(f"Error in pub/sub example: {e}")
    finally:
        await publisher.disconnect()
        await subscriber.disconnect()

async def reqreply_example():
    """Demonstrate request-reply functionality"""
    print("\n=== Request-Reply Example ===")
    
    # Create two pilots for client and server
    client = NatsPilot(nats_url="nats://localhost:4222")
    server = NatsPilot(nats_url="nats://localhost:4222")
    
    try:
        # Connect both pilots
        await client.connect()
        await server.connect()
        
        # Set up server (service provider)
        async def calculator_service(msg: Msg):
            operation = msg.get("operation")
            a = msg.get("a", 0)
            b = msg.get("b", 0)
            
            if operation == "add":
                result = a + b
            elif operation == "multiply":
                result = a * b
            elif operation == "divide":
                result = a / b if b != 0 else "Error: Division by zero"
            else:
                result = "Error: Unknown operation"
            
            return {"result": result, "operation": operation, "a": a, "b": b}
        
        # Register the service
        reqreply = server.reqreply()
        service_name = await reqreply.register_callback("calculator", calculator_service)
        print(f"Registered calculator service: {service_name}")
        
        # Client makes requests
        client_reqreply = client.reqreply()
        
        requests = [
            {"operation": "add", "a": 10, "b": 5},
            {"operation": "multiply", "a": 7, "b": 8},
            {"operation": "divide", "a": 20, "b": 4},
        ]
        
        for req in requests:
            response = await client_reqreply.request("calculator", req)
            print(f"Request: {req} -> Response: {response}")
            await asyncio.sleep(0.2)
        
        # Unregister service
        await reqreply.unregister_callback("calculator")
        print("Unregistered calculator service")
        
    except Exception as e:
        print(f"Error in request-reply example: {e}")
    finally:
        await client.disconnect()
        await server.disconnect()

async def context_manager_example():
    """Demonstrate context manager usage"""
    print("\n=== Context Manager Example ===")
    
    try:
        async with NatsPilot(nats_url="nats://localhost:4222") as pilot:
            print("Connected using context manager")
            
            # Use the pilot
            pubsub = pilot.pubsub()
            
            async def simple_handler(msg: Msg):
                print(f"Context manager received: {msg}")
            
            subscription_id = await pubsub.subscribe("test", simple_handler)
            await pubsub.publish("test", {"message": "Hello from context manager"})
            
            await asyncio.sleep(0.5)
            await pubsub.unsubscribe("test", subscription_id)
            
        print("Disconnected automatically")
        
    except Exception as e:
        print(f"Error in context manager example: {e}")

async def main():
    """Run all examples"""
    print("NatsPilot Examples")
    print("Make sure you have a NATS server running at nats://localhost:4222")
    print("You can start one with: docker run -p 4222:4222 nats")
    print()
    
    try:
        await pubsub_example()
        await reqreply_example()
        await context_manager_example()
    except KeyboardInterrupt:
        print("\nExamples interrupted by user")
    except Exception as e:
        print(f"Error running examples: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 