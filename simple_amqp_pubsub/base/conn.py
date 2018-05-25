from abc import ABCMeta
from typing import Callable, Dict, Set, Tuple

from simple_amqp_pubsub.data import Event, Pipe, Source
from simple_amqp_pubsub.log import logger
from simple_amqp_pubsub.subscriber import Subscriber

from .client import PubSubClient


class BasePubSub(metaclass=ABCMeta):
    CLIENT_CLS = PubSubClient

    def __init__(self):
        self._listen_services = {}
        self._recv_error_handlers = set()

        self._sources: Dict[str, Source] = {}
        self._pipes: Dict[str, Pipe] = {}
        self._handlers: Dict[Set[str], Callable] = {}
        self.log = logger

    def listen(self, source: Source, pipe: Pipe, topic: str):
        self._add_source(source)
        self._add_pipe(pipe)

        def decorator(func):
            self._handlers[(source.name, topic, pipe.name)] = func
            return func

        return decorator

    def add_subscriber(self, sub: Subscriber, servicer) -> 'BasePubSub':
        pipe = sub.pipe
        self._add_pipe(pipe)

        event_handlers = sub.get_event_handlers(servicer)
        for source, topics in event_handlers.items():
            self._add_source(source)
            for topic, handler in topics.items():
                self._handlers[(source.name, topic, pipe.name)] = handler

        return self

    def client(self, service: str) -> PubSubClient:
        raise NotImplementedError

    def push_event(self, event: Event):
        raise NotImplementedError

    def recv_event(self, event: Event):
        raise NotImplementedError

    def log_event_recv(self, event: Event):
        self.log.info('event received [{}:{}]'.format(
            event.source,
            event.topic,
        ))

    def log_event_sent(self, event: Event):
        self.log.info('sending event [{}:{}]'.format(
            event.source,
            event.topic,
        ))

    def add_recv_event_error_handler(self, handler):
        self._recv_error_handlers.add(handler)

    def _get_handler(
            self,
            source: str,
            topic: str,
            pipe: str,
    ) -> Tuple[Callable, str]:
        try:
            handler = self._handlers[(source, topic, pipe)]
        except KeyError:
            msg = 'Handler [{source}:{topic}->{pipe}] not found'.format(
                source=source,
                topic=topic,
                pipe=pipe,
            )
            return None, msg

        return handler, None

    def _add_source(self, source: Source):
        if source.name not in self._sources:
            self._sources[source.name] = source

    def _add_pipe(self, pipe: Pipe):
        if pipe.name not in self._pipes:
            self._pipes[pipe.name] = pipe
