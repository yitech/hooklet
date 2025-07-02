from abc import ABC, abstractmethod

from hooklet.base.node import Node
from hooklet.base.pilot import ReqReply
from hooklet.base.types import Reply, Req


class RPCServer(Node, ABC):
    def __init__(self, name: str, reqreply: ReqReply):
        super().__init__(name)
        self.reqreply = reqreply

    async def start(self):
        await super().start()
        await self.reqreply.register_callback(self.name, self.callback)

    @abstractmethod
    async def callback(self, req: Req) -> Reply:
        """
        The ReqReply require developer to implement the callback, including the error
        handling and return the Reply with error.
        """
        raise NotImplementedError("Subclasses must implement callback()")

    async def run(self):
        await self.shutdown_event.wait()


class RPCClient(Node):
    def __init__(self, name: str, reqreply: ReqReply):
        super().__init__(name)
        self.reqreply = reqreply

    async def request(self, subject: str, req: Req) -> Reply:
        try:
            return await self.reqreply.request(subject, req)
        except Exception as e:
            await self.on_error(e)
            raise  # Re-raise the exception

    async def run(self):
        await self.shutdown_event.wait()
