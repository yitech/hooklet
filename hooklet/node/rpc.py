from abc import ABC, abstractmethod

from hooklet.base.node import Node
from hooklet.base.pilot import ReqReply
from hooklet.base.types import Reply, Req


class RPCServer(Node, ABC):
    def __init__(self, name: str, reqreply: ReqReply):
        super().__init__(name)
        self.reqreply = reqreply

    async def on_start(self):
        await self.reqreply.register_callback(self.name, self.on_request)

    @abstractmethod
    async def on_request(self, req: Req) -> Reply:
        """
        The ReqReply require developer to implement the callback, including the error
        handling and return the Reply with error.
        """
        raise NotImplementedError("Subclasses must implement callback()")

    async def on_close(self):
        await self.reqreply.unregister_callback(self.name)


class RPCClient:
    def __init__(self, reqreply: ReqReply):
        self.reqreply = reqreply

    async def request(self, subject: str, req: Req, timeout: float = 10.0) -> Reply:
        return await self.reqreply.request(subject, req, timeout)
