#!/usr/bin/env python3
"""
Run both ExampleEmitter and ExampleHandler in a single script.
"""

import asyncio
import logging
import signal
from hooklet.eventrix.collection import ExampleEmitter, ExampleHandler
from hooklet.pilot import ZmqPilot, ZmqBroker

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def handle_shutdown(shutdown_event):
    """Handle graceful shutdown when SIGINT is received."""
    logger.info("Shutting down...")
    shutdown_event.set()

async def main():
    publisher_url = "inproc://pub"
    subscriber_url = "inproc://sub"
    zmq_broker = ZmqBroker(publisher_url, subscriber_url) # Run the broker in which the emitter and handler will connect
    await zmq_broker.connect()
    zmq_pilot = ZmqPilot(publisher_url, subscriber_url) # Create a ZMQ pilot to manage the connections
    await zmq_pilot.connect()

    emitter = ExampleEmitter(pilot=zmq_pilot)
    handler = ExampleHandler(pilot=zmq_pilot)

    # Run both emitter and handler concurrently
    await emitter.start()
    await handler.start()

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

        # Finally close the NATS connection
        await zmq_pilot.close()
        await zmq_broker.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # This captures the KeyboardInterrupt at the top level
        # if it escapes from the main coroutine
        logger.info("Shutdown complete.")
