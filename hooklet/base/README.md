# Hooklet Base Module

The `hooklet.base` module defines the abstract layer of encapsulation for the Hooklet project. This module serves as the foundation for understanding the architecture and provides the necessary abstractions for implementing different message broker backends.

## Overview

The base module establishes the core abstractions that enable Hooklet to support multiple message broker implementations (NATS, ZeroMQ, in-process) while maintaining a consistent interface. It defines the contract that all concrete implementations must follow, ensuring interoperability and allowing developers to easily add new transport layers.

## Architecture

### Core Abstractions

#### 1. Pilot Interface (`Pilot`)

The `Pilot` class is the main abstraction that represents a message broker connection. It provides:

- **Connection Management**: `connect()`, `disconnect()`, `is_connected()`
- **Message Publishing**: `publish(subject, data)`
- **Interface Access**: `pubsub()` and `reqreply()` methods

```python
class Pilot(ABC):
    @abstractmethod
    def is_connected(self) -> bool: ...
    
    @abstractmethod
    async def connect(self) -> None: ...
    
    @abstractmethod
    async def disconnect(self) -> None: ...
    
    @abstractmethod
    async def publish(self, subject: str, data: Any) -> None: ...
    
    @abstractmethod
    def pubsub(self) -> PubSub: ...
    
    @abstractmethod
    def reqreply(self) -> ReqReply: ...
```

#### 2. PubSub Interface (`PubSub`)

Handles publish/subscribe messaging patterns:

```python
class PubSub(ABC):
    @abstractmethod
    async def publish(self, subject: str, data: Any) -> None: ...
    
    @abstractmethod
    def subscribe(self, subject: str, callback: Callback|AsyncCallback) -> str: ...
    
    @abstractmethod
    def unsubscribe(self, subject: str) -> bool: ...
```

#### 3. ReqReply Interface (`ReqReply`)

Handles request/reply messaging patterns:

```python
class ReqReply(ABC):
    @abstractmethod
    async def request(self, subject: str, data: Any, timeout: float = 5.0) -> Any: ...
    
    @abstractmethod
    def register_callback(self, subject: str, callback: Callback|AsyncCallback) -> str: ...
```

#### 4. Type Definitions (`types.py`)

Defines common callback types used throughout the system:

```python
Callback = Callable[[Any], Any]
AsyncCallback = Callable[[Any], Awaitable[Any]]
```

## Design Principles

### 1. Separation of Concerns

The base module separates different messaging patterns:
- **PubSub**: For broadcast-style messaging
- **ReqReply**: For request-response patterns
- **Pilot**: Coordinates both patterns and manages connections

### 2. Interface Segregation

Each interface has a single responsibility:
- `PubSub` handles only publish/subscribe operations
- `ReqReply` handles only request/reply operations
- `Pilot` manages the overall connection and provides access to both interfaces

### 3. Extensibility

The abstract design allows for easy addition of new transport layers:
- Implement the `Pilot` interface for new message brokers
- Override specific interfaces (`PubSub`, `ReqReply`) as needed
- Maintain consistent behavior across different implementations

## Implementation Guidelines

### Adding a New Transport Layer

To add support for a new message broker:

1. **Create a new pilot class** that inherits from `Pilot`
2. **Implement all abstract methods** defined in the base interfaces
3. **Handle connection lifecycle** properly (connect/disconnect)
4. **Provide proper error handling** and logging
5. **Ensure thread safety** for concurrent operations

### Example Implementation Structure

```python
from hooklet.base import Pilot, PubSub, ReqReply

class MyCustomPilot(Pilot):
    def __init__(self):
        self._pubsub = MyCustomPubSub()
        self._reqreply = MyCustomReqReply()
    
    async def connect(self) -> None:
        # Implementation specific connection logic
        pass
    
    def pubsub(self) -> PubSub:
        return self._pubsub
    
    def reqreply(self) -> ReqReply:
        return self._reqreply

class MyCustomPubSub(PubSub):
    # Implement pub/sub methods
    pass

class MyCustomReqReply(ReqReply):
    # Implement req/reply methods
    pass
```
