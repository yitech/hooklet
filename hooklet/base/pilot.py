from abc import ABC, abstractmethod
from typing import Any

from hooklet.base.types import Callback, AsyncCallback

class PubSub(ABC):
    """
    Base class for pub/sub.
    """

    @abstractmethod
    async def publish(self, subject: str, data: Any) -> None:
        """
        Publish data to a specific subject.
        """
        raise NotImplementedError("Subclasses must implement publish()")
    
    @abstractmethod
    def subscribe(self, subject: str, callback: Callback|AsyncCallback) -> str:
        """
        Subscribe to a specific subject.
        """
        raise NotImplementedError("Subclasses must implement subscribe()")
    
    @abstractmethod
    def unsubscribe(self, subject: str) -> bool:
        """
        Unsubscribe from a specific subject.
        """
        raise NotImplementedError("Subclasses must implement unsubscribe()")


class ReqReply(ABC):

    @abstractmethod
    async def request(self, subject: str, data: Any, timeout: float = 5.0) -> Any:
        """
        Request data from a specific subject.
        """
        raise NotImplementedError("Subclasses must implement request()")

    @abstractmethod
    def register_callback(self, subject: str, callback: Callback|AsyncCallback) -> str:
        """
        Register a callback for a specific subject.
        """
        raise NotImplementedError("Subclasses must implement register_callback()")


class Pilot(ABC):
    """
    Base class for all pilots.
    """

    async def __aenter__(self) -> "Pilot":
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.disconnect()

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if the message broker is connected.
        """
        raise NotImplementedError("Subclasses must implement is_connected()")
    
    @abstractmethod
    async def connect(self) -> None:
        """
        Connect to the message broker.
        """
        raise NotImplementedError("Subclasses must implement connect()")
    
    @abstractmethod
    async def disconnect(self) -> None:
        """
        Disconnect from the message broker.
        """
        raise NotImplementedError("Subclasses must implement disconnect()")
    
    @abstractmethod
    def pubsub(self) -> PubSub:
        """
        Get the pub/sub interface.
        """
        raise NotImplementedError("Subclasses must implement pubsub()")
    
    @abstractmethod
    def reqreply(self) -> ReqReply:
        """
        Get the req/reply interface.
        """
        raise NotImplementedError("Subclasses must implement reqreply()")
