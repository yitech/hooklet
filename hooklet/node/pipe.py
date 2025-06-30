from hooklet.base import PubSub


class Pipe:
    def __init__(self, name: str, pubsub: PubSub):
        self.name = name
        self.pubsub = pubsub

    