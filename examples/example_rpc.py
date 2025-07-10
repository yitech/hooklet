#!/usr/bin/env python3
"""
RPC Demo Example - Arithmetic Operations

This example demonstrates how to create an RPC server that handles
basic arithmetic operations (+, -, *, /) and a client that calls them.
"""

import asyncio
import time
import uuid
from typing import Any, Dict

from hooklet.node.rpc import RPCServer, RPCClient
from hooklet.pilot.inproc_pilot import InprocPilot
from hooklet.pilot.nats_pilot import NatsPilot
from hooklet.base.types import Req, Reply


class ArithmeticServer(RPCServer):
    """RPC Server that handles arithmetic operations"""
    
    async def on_request(self, req: Req) -> Reply:
        """Handle arithmetic operation requests"""
        start_ms = int(time.time() * 1000)
        
        try:
            # Extract operation and operands from request
            operation = req.type
            params = req.params
            a = params.get('a')
            b = params.get('b')
            
            # Validate input
            if a is None or b is None:
                raise ValueError("Both operands 'a' and 'b' are required")
            
            if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
                raise ValueError("Operands must be numbers")
            
            # Perform arithmetic operation
            result = None
            if operation == 'add':
                result = a + b
            elif operation == 'subtract':
                result = a - b
            elif operation == 'multiply':
                result = a * b
            elif operation == 'divide':
                if b == 0:
                    raise ValueError("Division by zero is not allowed")
                result = a / b
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            end_ms = int(time.time() * 1000)
            
            return Reply(
                type=operation,
                result=result,
                error=None,
                start_ms=start_ms,
                end_ms=end_ms
            )
            
        except Exception as e:
            end_ms = int(time.time() * 1000)
            return Reply(
                type=req.type,
                result=None,
                error=str(e),
                start_ms=start_ms,
                end_ms=end_ms
            )


class ArithmeticClient(RPCClient):
    """RPC Client for arithmetic operations"""
    
    async def add(self, a: float, b: float) -> float:
        """Add two numbers"""
        req = Req(
            type='add',
            params={'a': a, 'b': b},
            error=None
        )
        reply = await self.request('arithmetic-server', req)
        
        if reply.error:
            raise ValueError(f"Add operation failed: {reply.error}")
        
        return reply.result
    
    async def subtract(self, a: float, b: float) -> float:
        """Subtract two numbers"""
        req = Req(
            type='subtract',
            params={'a': a, 'b': b},
            error=None
        )
        reply = await self.request('arithmetic-server', req)
        
        if reply.error:
            raise ValueError(f"Subtract operation failed: {reply.error}")
        
        return reply.result
    
    async def multiply(self, a: float, b: float) -> float:
        """Multiply two numbers"""
        req = Req(
            type='multiply',
            params={'a': a, 'b': b},
            error=None
        )
        reply = await self.request('arithmetic-server', req)
        
        if reply.error:
            raise ValueError(f"Multiply operation failed: {reply.error}")
        
        return reply.result
    
    async def divide(self, a: float, b: float) -> float:
        """Divide two numbers"""
        req = Req(
            type='divide',
            params={'a': a, 'b': b},
            error=None
        )
        reply = await self.request('arithmetic-server', req)
        
        if reply.error:
            raise ValueError(f"Divide operation failed: {reply.error}")
        
        return reply.result


async def run_demo():
    """Run the RPC arithmetic demo"""
    print("🚀 Starting RPC Arithmetic Demo")
    print("=" * 50)
    
    # Create pilot and connect
    pilot = InprocPilot()
    # pilot = NatsPilot(
    #     nats_url="nats://localhost:4222"
    # )
    await pilot.connect()
    
    # Create server and client
    server = ArithmeticServer("arithmetic-server", pilot.reqreply())
    client = ArithmeticClient(pilot.reqreply())
    
    # Start server
    await server.start()
    print("✅ Arithmetic server started")
    
    # Test operations
    test_cases = [
        ("Addition", client.add, 10, 5),
        ("Subtraction", client.subtract, 10, 5),
        ("Multiplication", client.multiply, 10, 5),
        ("Division", client.divide, 10, 5),
        ("Division by zero", client.divide, 10, 0),
    ]
    
    for test_name, operation, a, b in test_cases:
        print(f"\n🧮 Testing {test_name}: {a} and {b}")
        try:
            if test_name == "Division by zero":
                result = await operation(a, b)
                print(f"   ❌ Expected error but got: {result}")
            else:
                result = await operation(a, b)
                print(f"   ✅ Result: {result}")
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # Test with floating point numbers
    print(f"\n🧮 Testing floating point operations:")
    try:
        result = await client.add(3.14, 2.86)
        print(f"   ✅ 3.14 + 2.86 = {result}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    try:
        result = await client.multiply(2.5, 4.0)
        print(f"   ✅ 2.5 * 4.0 = {result}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print("\n" + "=" * 50)
    print("✅ RPC Arithmetic Demo completed!")


if __name__ == "__main__":
    asyncio.run(run_demo())
