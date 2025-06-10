# Hooklet v2

Hooklet is an event-driven framework that enables building distributed systems using a source-sink architecture. The v2 version introduces a more robust and flexible implementation with improved message routing and handling capabilities.

## Architecture

### Core Components

1. **Node**
   - Base class for all nodes in the system
   - Handles message routing, generation, and processing
   - Manages lifecycle events (start, execute, stop, finish)
   - Supports both source and sink functionality

2. **Source**
   - Generates events and publishes them to the message bus
   - Implements `generator_func()` to produce messages
   - Uses a router function to determine message destinations

3. **Sinker**
   - Consumes events from specified sources
   - Implements `handler_func()` to process incoming messages
   - Can generate new messages in response to received events

4. **Pilot**
   - Manages the underlying message transport (e.g., NATS)
   - Handles message publishing and subscription
   - Provides connection management

### Message Flow

1. Sources generate messages using their `generator_func()`
2. Messages are routed using the provided router function
3. Sinkers receive messages from their subscribed sources
4. Sinkers process messages using their `handler_func()`
5. Processed messages can be routed to other destinations

## Usage

### Basic Example

```python
import asyncio
from hooklet.eventrix.collection.v2.source import ExampleSource
from hooklet.eventrix.collection.v2.sinker import ExampleSinker
from hooklet.pilot import NatsPilot

async def main():
    # Initialize the message transport
    nats_pilot = NatsPilot()
    await nats_pilot.connect()

    # Create a source that generates events
    source = ExampleSource(
        pilot=nats_pilot,
        router=lambda e: "example"  # Route all messages to "example" subject
    )

    # Create a sinker that processes events
    sinker = ExampleSinker(
        pilot=nats_pilot,
        sources=["example"],  # Subscribe to "example" subject
        router=lambda e: None  # No further routing needed
    )

    # Start both components
    await source.start()
    await sinker.start()

    try:
        # Keep the application running
        await asyncio.Event().wait()
    finally:
        # Clean shutdown
        await source.stop()
        await sinker.stop()
        await nats_pilot.close()

if __name__ == "__main__":
    asyncio.run(main())
```

### Creating Custom Sources

```python
from hooklet.eventrix.v2.node import Node
from hooklet.types import HookletMessage

class CustomSource(Node):
    def __init__(self, pilot, router):
        super().__init__(pilot, [], router)

    async def generator_func(self):
        while self.is_running:
            message = HookletMessage(
                type="custom",
                payload={"data": "example"}
            )
            yield message
            await asyncio.sleep(1)
```

### Creating Custom Sinkers

```python
from hooklet.eventrix.v2.node import Node
from hooklet.types import HookletMessage

class CustomSinker(Node):
    def __init__(self, pilot, sources, router):
        super().__init__(pilot, sources, router)

    async def handler_func(self, message: HookletMessage):
        # Process the incoming message
        processed_message = HookletMessage(
            type="processed",
            payload={"result": f"Processed {message.payload['data']}"}
        )
        yield processed_message
```

## Message Structure

Messages in Hooklet v2 follow a structured format:

```python
class HookletMessage:
    id: UUID
    correlation_id: UUID | None
    type: str
    payload: dict
    node_id: str | None
    start_at: int | None
    finish_at: int | None
```

## Best Practices

1. **Error Handling**
   - Implement proper error handling in both source and sinker nodes
   - Use the `on_error` method to handle exceptions
   - Ensure graceful shutdown in case of errors

2. **Message Routing**
   - Design clear routing patterns for your messages
   - Use meaningful subject names
   - Consider message flow and dependencies

3. **Resource Management**
   - Always properly close connections and resources
   - Implement graceful shutdown procedures
   - Handle cleanup in finally blocks

4. **Monitoring**
   - Use the built-in logging capabilities
   - Monitor message flow and processing times
   - Track node lifecycle events

## Requirements

- Python 3.8+
- NATS server (for NATS transport)
- Required Python packages (see requirements.txt)

## Installation

```bash
pip install -r requirements.txt
```

## License

[Your License Here]
