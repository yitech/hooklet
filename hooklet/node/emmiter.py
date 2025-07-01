from hooklet.base.node import Node
from abc import abstractmethod, ABC
from typing import Callable, AsyncGenerator
from hooklet.base.pilot import Msg, PubSub
import asyncio

class Emitter(Node, ABC):
    def __init__(self, name: str, pubsub: PubSub, router: Callable[[Msg], str]):
        """
        Emitter node that emits messages to the router.
        """
        super().__init__(name)
        self.pubsub = pubsub
        self.router = router
    

    @abstractmethod
    async def emit(self) -> AsyncGenerator[Msg, None]:
        """
        Use is_running to stop the emit.
        """
        raise NotImplementedError("Subclasses must implement emit()")

    async def run(self):
        emit_gen = None
        try:
            emit_gen = self.emit()
            async for msg in self.emit():
                if self.shutdown_event.is_set():
                    break
                subject = self.router(msg)
                await self.pubsub.publish(subject, msg)
        except Exception as e:
            await self.on_error(e)
        finally:
            # Ensure the generator is properly closed
            if emit_gen is not None:
                try:
                    await emit_gen.aclose()
                except:
                    pass