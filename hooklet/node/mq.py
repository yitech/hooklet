from abc import ABC

from hooklet.base.node import Node
from hooklet.base.pilot import PushPull


class Worker(Node, ABC):
    def __init__(self, name: str, pushpull: PushPull):
        super().__init__(name)
        self.pushpull = pushpull
