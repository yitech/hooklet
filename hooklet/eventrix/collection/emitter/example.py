import asyncio
import logging
import uuid

from hooklet.eventrix.emitter import Emitter

logger = logging.getLogger(__name__)


class ExampleEmitter(Emitter):
    async def get_generators(self):
        async def example_generator():
            while True:
                event = {"id": str(uuid.uuid4())}
                await asyncio.sleep(1)  # Emit every second
                yield event

        return {"example": example_generator}
