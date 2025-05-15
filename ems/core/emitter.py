import logging
from abc import ABC, abstractmethod
from typing import Any

from ems.nats_manager import NatsManager

from .executor import EventExecutor

logger = logging.getLogger(__name__)


class EventEmitter(EventExecutor, ABC):
    """
    Base class for event emitters.
    This abstract class provides the structure for emitting events.
    """

    def __init__(self, nats_manager: NatsManager, executor_id: None | str = None):
        super().__init__(nats_manager, executor_id)
    

    