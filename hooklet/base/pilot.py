from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable

from hooklet.base.types import Job, Msg, Reply, Req


class PubSub(ABC):
    """
    Base class for pub/sub.
    """

    @abstractmethod
    async def publish(self, subject: str, msg: Msg) -> None:
        """
        Publish data to a specific subject.
        """
        raise NotImplementedError("Subclasses must implement publish()")

    @abstractmethod
    async def subscribe(self, subject: str, callback: Callable[[Msg], Awaitable[Any]]) -> int:
        """
        Subscribe to a specific subject.
        """
        raise NotImplementedError("Subclasses must implement subscribe()")

    @abstractmethod
    async def unsubscribe(self, subject: str, subscription_id: int) -> bool:
        """
        Unsubscribe from a specific subject.
        """
        raise NotImplementedError("Subclasses must implement unsubscribe()")


class ReqReply(ABC):

    @abstractmethod
    async def request(self, subject: str, req: Req, timeout: float = 10.0) -> Reply:
        """
        Request data from a specific subject.
        """
        raise NotImplementedError("Subclasses must implement request()")

    @abstractmethod
    async def register_callback(
        self, subject: str, callback: Callable[[Req], Awaitable[Reply]]
    ) -> None:
        """
        Register a callback for a specific subject.
        """
        raise NotImplementedError("Subclasses must implement register_callback()")

    @abstractmethod
    async def unregister_callback(self, subject: str) -> bool:
        """
        Unregister a callback for a specific subject.
        """
        raise NotImplementedError("Subclasses must implement unregister_callback()")


class PushPull(ABC):

    @abstractmethod
    async def push(self, subject: str, job: Job) -> bool:
        """
        Push data to a specific subject.
        """
        raise NotImplementedError("Subclasses must implement push()")

    @abstractmethod
    async def register_worker(
        self, subject: str, callback: Callable[[Job], Awaitable[Any]], n_workers: int = 1
    ) -> None:
        """
        Register a worker for a specific subject.
        """
        raise NotImplementedError("Subclasses must implement register_worker()")

    @abstractmethod
    async def unregister_worker(self, subject: str) -> None:
        """
        Unregister a worker for a specific subject.
        """
        raise NotImplementedError("Subclasses must implement unregister_worker()")

    @abstractmethod
    async def subscribe(self, subject: str, callback: Callable[[Job], Awaitable[Any]]) -> int:
        """
        Subscribe to a specific subject.
        """
        raise NotImplementedError("Subclasses must implement subscribe()")

    @abstractmethod
    async def unsubscribe(self, subject: str, subscription_id: int) -> bool:
        """
        Unsubscribe from a specific subject.
        """
        raise NotImplementedError("Subclasses must implement unsubscribe()")


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

    @abstractmethod
    def pushpull(self) -> PushPull:
        """
        Get the push/pull interface.
        """
        raise NotImplementedError("Subclasses must implement pushpull()")
