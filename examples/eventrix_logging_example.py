"""
Example demonstrating the usage of Eventrix logger.

This example shows how each Eventrix instance gets its own logger
with executor_id context automatically included in all log messages.
"""

import asyncio
import time
from typing import Any

from hooklet.base import BaseEventrix, BasePilot
from hooklet.logger import setup_default_logging, LogLevel, LogFormat


class MockPilot(BasePilot):
    """Mock pilot for demonstration purposes."""
    
    def __init__(self):
        self._connected = False
    
    async def connect(self) -> None:
        self._connected = True
    
    def is_connected(self) -> bool:
        return self._connected
    
    async def close(self) -> None:
        self._connected = False
    
    async def register_handler(self, subject: str, handler, handler_id: str = None) -> str:
        return "mock_handler_id"
    
    async def unregister_handler(self, handler_id: str) -> bool:
        return True
    
    async def publish(self, subject: str, data: Any) -> None:
        pass


class ExampleEventrix(BaseEventrix):
    """Example Eventrix implementation that demonstrates logger usage."""
    
    def __init__(self, pilot: BasePilot, executor_id: str = None, duration: int = 3):
        super().__init__(pilot, executor_id)
        self.duration = duration
    
    async def on_start(self) -> None:
        """Called when the executor starts."""
        self.logger.info("Starting executor", extra={"duration": self.duration})
    
    async def on_execute(self) -> None:
        """Main execution logic."""
        for i in range(self.duration):
            self.logger.info(f"Processing step {i+1}/{self.duration}")
            
            # Simulate some work and publish a message
            await self.publish("test.subject", {"step": i+1, "timestamp": time.time()})
            
            await asyncio.sleep(1)
    
    async def on_finish(self) -> None:
        """Called when the executor finishes."""
        self.logger.info("Executor cleanup completed")
    
    async def on_stop(self) -> None:
        """Called when the executor is stopped."""
        self.logger.info("Executor stopped")
    
    async def on_error(self, exception: Exception) -> None:
        """Called when an error occurs."""
        self.logger.error("Error in executor", exc_info=exception)


async def demo_single_executor():
    """Demonstrate a single executor with logging."""
    print("\n=== Single Executor Demo ===")
    
    # Set up basic logging
    setup_default_logging(level=LogLevel.DEBUG, format_type=LogFormat.DETAILED)
    
    pilot = MockPilot()
    executor = ExampleEventrix(pilot, executor_id="demo-executor-1", duration=3)
    
    print(f"Created executor with ID: {executor.executor_id}")
    
    try:
        await executor.start()
        # Let it run for a bit
        await asyncio.sleep(5)
    finally:
        await executor.stop()
        await pilot.close()


async def demo_multiple_executors():
    """Demonstrate multiple executors each with their own logger."""
    print("\n=== Multiple Executors Demo ===")
    
    # Set up logging with JSON format for better structured output
    setup_default_logging(level=LogLevel.INFO, format_type=LogFormat.JSON)
    
    pilot = MockPilot()
    
    # Create multiple executors
    executors = [
        ExampleEventrix(pilot, executor_id=f"executor-{i}", duration=2)
        for i in range(3)
    ]
    
    print(f"Created {len(executors)} executors:")
    for executor in executors:
        print(f"  - {executor.executor_id}")
    
    try:
        # Start all executors
        tasks = [executor.start() for executor in executors]
        await asyncio.gather(*tasks)
        
        # Let them run
        await asyncio.sleep(4)
        
    finally:
        # Stop all executors
        stop_tasks = [executor.stop() for executor in executors]
        await asyncio.gather(*stop_tasks)
        await pilot.close()


async def main():
    """Run all demos."""
    await demo_single_executor()
    await demo_multiple_executors()


if __name__ == "__main__":
    asyncio.run(main()) 