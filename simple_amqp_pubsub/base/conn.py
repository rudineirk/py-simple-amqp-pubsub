from abc import ABCMeta
from typing import Callable, Tuple

from simple_amqp_pubsub.data import Event

from .client import PubSubClient


class BasePubSub(metaclass=ABCMeta):
    CLIENT_CLS = PubSubClient

    def __init__(self):
        self._listen_services = {}
        self._recv_error_handlers = set()

    def listen(self, service: str, name: str=None):
        if service not in self._listen_services:
            self._listen_services[service] = {}

        def decorator(func, name=name):
            if name is None:
                name = func.__name__

            self._listen_routes[service][name] = func
            return func

        return decorator

    def add_subscriber(self, subscriber) -> 'BasePubSub':
        service = subscriber.sub.service
        event_handlers = subscriber.sub.get_event_handlers(subscriber)

        if service not in self._listen_services:
            self._listen_services[service] = {}

        for event, handler in event_handlers.items():
            self._listen_services[service][event] = handler

        return self

    def client(self, service: str) -> PubSubClient:
        raise NotImplementedError

    def push_event(self, event: Event):
        raise NotImplementedError

    def recv_event(self, event: Event):
        raise NotImplementedError

    def add_recv_event_error_handler(self, handler):
        self._recv_error_handlers.add(handler)

    def _get_handler(
            self,
            service: str,
            event: str,
    ) -> Tuple[Callable, str]:
        try:
            event_handlers = self._listen_services[service]
        except KeyError:
            msg = 'Service [{}] not found'.format(service)
            return None, msg

        try:
            handler = event_handlers[event]
        except KeyError:
            msg = 'Event handler [{}->{}] not found'.format(
                service,
                event,
            )
            return None, msg

        return handler, None
