from hooklet.base.node import Node
from abc import abstractmethod, ABC
from typing import Callable, AsyncGenerator
from hooklet.base.pilot import Job, PushPull
from contextlib import aclosing
import asyncio

class Worker(Node, ABC):
    def __init__(self, name: str, pushpull: PushPull):
        super().__init__(name)
        self.pushpull = pushpull


    