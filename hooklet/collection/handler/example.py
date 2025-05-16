import logging

from hooklet.core.handler import EventHandler

logger = logging.getLogger(__name__)


class ExampleHandler(EventHandler):
    def get_handlers(self):
        async def handle_example_event(data):
            event_id = data.get("id")
            logger.info(f"Received event with id: {event_id}")

        return {"example": handle_example_event}
