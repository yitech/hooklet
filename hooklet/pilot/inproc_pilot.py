import asyncio
import uuid
from typing import Any, Dict
from hooklet.base import Pilot, Callback, AsyncCallback
from hooklet.logger import get_logger

logger = get_logger(__name__)

class InProcPilot(Pilot):
    def __init__(self) -> None:
        super().__init__()
        self._connected = False
        

    def is_connected(self) -> bool:
        return self._connected


    