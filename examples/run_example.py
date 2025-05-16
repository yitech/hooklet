#!/usr/bin/env python3
"""
Run both ExampleEmitter and ExampleHandler in a single script.
"""

import asyncio
import logging
from natsio.collection.emitter.example import ExampleEmitter
from natsio.collection.handler.example import ExampleHandler
from natsio.nats_manager import NatsManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # First, stop both components
        await emitter.stop()
        await handler.stop()

        # Wait for their tasks to complete
        await asyncio.gather(emitter_task, handler_task, return_exceptions=True)

        # Finally close the NATS connection
        await nats_manager.close()

if __name__ == "__main__":
    asyncio.run(main())
