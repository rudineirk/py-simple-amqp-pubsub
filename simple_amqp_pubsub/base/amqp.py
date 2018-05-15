from abc import ABCMeta
from typing import List

from simple_amqp import AmqpMsg, AmqpParameters
from simple_amqp_pubsub.consts import (
    PUBSUB_EXCHANGE,
    PUBSUB_QUEUE,
    RETRY_DLX_EXCHANGE_NAME,
    RETRY_EXCHANGE_NAME,
    RETRY_QUEUE_NAME
)
from simple_amqp_pubsub.data import Event
from simple_amqp_pubsub.encoding import decode_event, encode_event

from .client import PubSubClient
from .conn import BasePubSub


def transform_retry_time(time):
    if 'ms' in time:
        time = int(time.replace('ms', ''))
    elif 's' in time:
        time = int(time.replace('s', '')) * 1000
    elif 'm' in time:
        time = int(time.replace('m', '')) * 1000 * 60
    elif 'h' in time:
        time = int(time.replace('h', '')) * 1000 * 60 * 60

    return time


class BaseAmqpPubSub(BasePubSub, metaclass=ABCMeta):
    CLIENT_CLS = PubSubClient

    def __init__(
            self,
            params: AmqpParameters,
            service: str='service.name',
            retries: List[str]=None,
            enable_retries: bool=True,
    ):
        super().__init__()
        self.service = service
        self.conn = self._create_conn(params)
        self.retries = retries \
            if retries is not None \
            else ['1m', '15m', '1h']
        self._enable_retries = enable_retries

        self._publish_services = set()
        self._listen_channel = None
        self._publish_channel = None
        self._service_queue = None

    def _create_conn(self, params: AmqpParameters):
        raise NotImplementedError

    def configure(self):
        self._create_publish()
        self._create_listen()
        self._create_retries()

    def start(self, auto_reconnect: bool=True, wait: bool=True):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def client(self, service: str) -> PubSubClient:
        self._publish_services.add(service)
        return self.CLIENT_CLS(self, service)

    def push_event(self, event: Event):
        msg = self._encode_event(event)
        msg = msg.replace(
            exchange=PUBSUB_EXCHANGE.format(service=event.service),
            topic=event.topic,
        )
        return self._send_event_msg(msg)

    def _send_event_msg(self, msg: AmqpMsg):
        raise NotImplementedError

    def _on_event_message(self, msg: AmqpMsg):
        raise NotImplementedError

    def _retry_event(self, event: Event) -> AmqpMsg:
        try:
            retry_time = self.retries[event.retry_count]
        except IndexError:
            return

        event = event.replace(retry_count=event.retry_count+1)
        msg = self._encode_event(event)

        retry_exchange = RETRY_EXCHANGE_NAME.format(service=self.service)
        retry_queue = RETRY_QUEUE_NAME.format(
            time=retry_time,
            service=self.service,
        )
        msg = msg.replace(
            exchange=retry_exchange,
            topic=retry_queue,
        )
        return msg

    def _decode_event(self, msg: AmqpMsg) -> Event:
        return decode_event(msg)

    def _encode_event(self, event: Event) -> AmqpMsg:
        return encode_event(event)

    def _create_publish(self):
        channel = self.conn.channel()
        for service in self._publish_services:
            exchange = PUBSUB_EXCHANGE.format(service=service)
            channel.exchange(exchange, 'topic', durable=True)

        self._publish_channel = channel

    def _create_listen(self):
        channel = self.conn.channel()

        queue_name = PUBSUB_QUEUE.format(service=self.service)
        queue = channel.queue(queue_name, durable=True)
        for service, events in self._listen_services.items():
            exchange_name = PUBSUB_EXCHANGE.format(service=service)
            exchange = channel \
                .exchange(exchange_name, 'topic', durable=True)

            for event in events:
                queue.bind(exchange, event)

        queue.consume(self._on_event_message)
        self._service_queue = queue
        self._listen_channel = channel

    def _create_retries(self):
        if not self._enable_retries:
            return

        channel = self._publish_channel

        retry_dlx_exchange_name = RETRY_DLX_EXCHANGE_NAME \
            .format(service=self.service)
        retry_dlx_exchange = channel.exchange(
            retry_dlx_exchange_name,
            'topic',
            durable=True,
        )

        retry_exchange_name = RETRY_EXCHANGE_NAME.format(service=self.service)
        retry_exchange = channel.exchange(
            retry_exchange_name,
            'topic',
            durable=True,
        )

        service_exchange_name = PUBSUB_EXCHANGE.format(service=self.service)
        service_queue_name = PUBSUB_QUEUE.format(service=self.service)
        self._service_queue.bind(retry_dlx_exchange, service_queue_name)

        for retry_time in self.retries:
            retry_queue_name = RETRY_QUEUE_NAME.format(
                time=retry_time,
                service=self.service,
            )

            retry_queue = channel.queue(
                retry_queue_name,
                durable=True,
                props={
                    'x-message-ttl': transform_retry_time(retry_time),
                    'x-dead-letter-exchange': retry_dlx_exchange_name,
                    'x-dead-letter-routing-key': service_exchange_name,
                },
            )
            retry_queue.bind(retry_exchange, retry_queue_name)
