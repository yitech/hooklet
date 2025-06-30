from abc import abstractmethod, ABC
from hooklet.base.node import Node
from hooklet.base.pilot import ReqReply, Msg
import asyncio

class RPCServer(Node, ABC):
    def __init__(self, name: str, reqreply: ReqReply):
        super().__init__(name)
        self.reqreply = reqreply
        self.queue: asyncio.Queue[Msg] = asyncio.Queue()

    async def start(self):
        await super().start()
        self.reqreply.register_callback(self.name, self.callback)

    @abstractmethod
    async def callback(self, msg: Msg) -> Msg:
        raise NotImplementedError("Subclasses must implement callback()")   

    async def run(self):
        await self.shutdown_event.wait()


class RPCClient(Node):
    def __init__(self, name: str, reqreply: ReqReply):
        super().__init__(name)
        self.reqreply = reqreply

    async def request(self, subject: str, msg: Msg) -> Msg:
        return await self.reqreply.request(subject, msg)
    
    async def run(self):
        await self.shutdown_event.wait()

    