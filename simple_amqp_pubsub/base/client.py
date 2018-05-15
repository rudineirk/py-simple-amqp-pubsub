from typing import Any

from simple_amqp_pubsub.data import Event, Source


class PubSubClient:
    def __init__(self, pubsub: 'BasePubSub', source: Source):
        self.pubsub = pubsub
        self.source = source
        self._source_name = source.name

        self.events_cache = {}

    def push(self, topic: str, payload: Any):
        return self.pubsub.push_event(Event(
            source=self._source_name,
            topic=topic,
            payload=payload,
        ))

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            pass

        try:
            return self.events_cache[name]
        except KeyError:
            pass

        def publisher(payload):
            event = Event(
                source=self._source_name,
                topic=name,
                payload=payload,
            )
            return self.pubsub.push_event(event)

        self.events_cache[name] = publisher
        return publisher
