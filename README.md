# hooklet

An asynchronous, event-driven Python framework for developing applications with NATS messaging system(expected to support more in the future). This framework leverages Python's async/await features and NATS to create a flexible, extensible event driven class.

## Installation

### From PyPI

```bash
pip install hooklet
```

### From Source

```bash
git clone https://github.com/yourusername/hooklet.git
cd hooklet
pip install -e .
```

## Quick Start

```python
import asyncio
from hooklet.nats_manager import NatsManager

async def main():
    # Connect to NATS
    nats_manager = NatsManager()
    await nats_manager.connect()

    # Publish a message
    await nats_manager.publish("example.topic", "Hello World!")

    # Subscribe to a topic
    async def message_handler(msg):
        print(f"Received: {msg.data.decode()}")
    
    await nats_manager.subscribe("example.topic", message_handler)
    
    # Wait for a while
    await asyncio.sleep(5)
    
    # Clean up
    await nats_manager.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## Examples

See the `examples` directory for more usage examples.

## Requirements

- Python 3.12+
- nats-py 2.3.1+

## License

MIT
