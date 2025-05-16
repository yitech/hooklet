import logging

from hooklet.eventrix.handler import Handler

logger = logging.getLogger(__name__)


class ExampleHandler(Handler):
    def get_handlers(self):
        async def handle_example_event(data):
            event_id = data.get("id")
            logger.info(f"Received event with id: {event_id}")

        return {"example": handle_example_event}
