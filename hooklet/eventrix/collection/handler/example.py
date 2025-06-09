from hooklet.eventrix.v1.handler import Handler


class ExampleHandler(Handler):
    def get_handlers(self):
        async def handle_example_event(data):
            event_id = data.get("id")
            self.logger.info(f"Received event with id: {event_id}")

        return {"example": handle_example_event}
