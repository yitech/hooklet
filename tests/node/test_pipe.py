import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch
from typing import AsyncGenerator

from hooklet.node.pipe import Pipe
from hooklet.pilot.inproc_pilot import InprocPilot
from hooklet.base.types import Msg
from hooklet.utils.id_generator import generate_id


