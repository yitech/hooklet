# hooklet

An asynchronous, event-driven Python framework for developing applications with NATS messaging system(expected to support more in the future). This framework leverages Python's async/await features and NATS to create a flexible, extensible event driven class.

## Installation

### From PyPI

```bash
pip install hooklet
```

### From Source

```bash
git clone https://github.com/yitech/hooklet.git
cd hooklet
pip install -e .
```

## Quick Start

```python
import asyncio
import logging
import signal
from hooklet.collection.emitter.example import ExampleEmitter
from hooklet.collection.handler.example import ExampleHandler
from hooklet.nats_manager import NatsManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def handle_shutdown(shutdown_event):
    """Handle graceful shutdown when SIGINT is received."""
    logger.info("Shutting down...")
    shutdown_event.set()

async def main():
    nats_manager = NatsManager()
    await nats_manager.connect()

    emitter = ExampleEmitter(nats_manager=nats_manager)
    handler = ExampleHandler(nats_manager=nats_manager)

    # Run both emitter and handler concurrently
    emitter_task = asyncio.create_task(emitter.start())
    handler_task = asyncio.create_task(handler.start())

    try:
        logger.info("ExampleEmitter and ExampleHandler are running. Press Ctrl+C to stop.")
        # Create an event for clean shutdown
        shutdown_event = asyncio.Event()
        
        # Set up a signal handler for keyboard interrupt
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(
            signal.SIGINT,
            lambda: asyncio.create_task(handle_shutdown(shutdown_event))
        )
        
        # Wait until shutdown is triggered
        await shutdown_event.wait()
        
    except asyncio.CancelledError:
        logger.info("Task was cancelled, shutting down...")
    finally:
        # First, stop both components
        await emitter.stop()
        await handler.stop()

        # Wait for their tasks to complete
        await asyncio.gather(emitter_task, handler_task, return_exceptions=True)

        # Finally close the NATS connection
        await nats_manager.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # This captures the KeyboardInterrupt at the top level
        # if it escapes from the main coroutine
        logger.info("Shutdown complete.")
```

## Examples

See the `examples` directory for more usage examples.

## Requirements

- Python 3.12+
- nats-py 2.3.1+

## License

MIT
