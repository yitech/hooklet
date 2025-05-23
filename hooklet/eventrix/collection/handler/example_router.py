import logging

from hooklet.eventrix.handler import Handler

logger = logging.getLogger(__name__)


class ExampleRouterHandler(Handler):
    def get_handlers(self):
        async def handle_even_example_event(data):
            event_id = data.get("id")
            logger.info(f"Received even event with id: {event_id}")
        async def handle_odd_example_event(data):
            event_id = data.get("id")
            logger.info(f"Received odd event with id: {event_id}")
        
        return {"example.even": handle_even_example_event,
                "example.odd": handle_odd_example_event}
