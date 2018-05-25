from abc import ABCMeta
from typing import Dict

from simple_amqp import (
    AmqpChannel,
    AmqpConnection,
    AmqpExchange,
    AmqpMsg,
    AmqpParameters,
    AmqpQueue
)

from simple_amqp_pubsub.consts import (
    PUBSUB_EXCHANGE,
    PUBSUB_QUEUE,
    RETRY_DLX_EXCHANGE_NAME,
    RETRY_EXCHANGE_NAME,
    RETRY_QUEUE_NAME
)
from simple_amqp_pubsub.data import Event, Pipe, Source
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
            conn: AmqpConnection = None,
            params: AmqpParameters = None,
    ):
        super().__init__()
        if conn is not None:
            self.conn = conn
        else:
            self.conn = self._create_conn(params)

        self._publish_channel: AmqpChannel = None
        self._listen_channel: AmqpChannel = None

        self._exchanges: Dict[str, AmqpExchange] = {}
        self._queues: Dict[str, AmqpQueue] = {}

    def _create_conn(self, params: AmqpParameters):
        raise NotImplementedError

    def configure(self):
        self._setup_channels()
        self._create_sources()
        self._create_pipes()
        self._bind_handlers()

    def start(self, auto_reconnect: bool=True, wait: bool=True):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def client(self, source: Source) -> PubSubClient:
        self._add_source(source)
        return self.CLIENT_CLS(self, source)

    def push_event(self, event: Event):
        self.log_event_sent(event)
        msg = self._encode_event(event)
        msg = msg.replace(
            exchange=PUBSUB_EXCHANGE.format(name=event.source),
            topic=event.topic,
        )
        return self._send_event_msg(msg)

    def _send_event_msg(self, msg: AmqpMsg):
        raise NotImplementedError

    def _on_event_message(self, msg: AmqpMsg):
        raise NotImplementedError

    def _retry_event(self, event: Event, pipe: Pipe) -> AmqpMsg:
        try:
            retry_time = pipe.retries[event.retry_count]
        except IndexError:
            return

        event = event.replace(retry_count=event.retry_count+1)
        msg = self._encode_event(event)

        retry_exchange = RETRY_EXCHANGE_NAME.format(name=event.pipe)
        retry_queue = RETRY_QUEUE_NAME.format(
            time=retry_time,
            name=event.pipe,
        )
        msg = msg.replace(
            exchange=retry_exchange,
            topic=retry_queue,
        )
        return msg

    def _decode_event(self, msg: AmqpMsg, pipe_name: str) -> Event:
        return decode_event(msg, pipe_name)

    def _encode_event(self, event: Event) -> AmqpMsg:
        return encode_event(event)

    def _setup_channels(self):
        self._publish_channel = self.conn.channel()
        self._listen_channel = self.conn.channel()

    def _create_sources(self):
        channel = self._publish_channel
        for source in self._sources.values():
            exchange_name = PUBSUB_EXCHANGE.format(name=source.name)
            exchange = channel.exchange(exchange_name, 'topic', durable=True)

            self._exchanges[exchange_name] = exchange

    def _create_pipes(self):
        channel = self.conn.channel()

        for pipe in self._pipes.values():
            queue_name = PUBSUB_QUEUE.format(name=pipe.name)
            queue = channel.queue(queue_name, durable=True)

            self._queues[queue_name] = queue

            if pipe.retries_enabled:
                self._create_retries(pipe, queue)

    def _bind_handlers(self):
        for source, topic, pipe in self._handlers:
            exchange_name = PUBSUB_EXCHANGE.format(name=source)
            exchange = self._exchanges[exchange_name]

            queue_name = PUBSUB_QUEUE.format(name=pipe)
            queue = self._queues[queue_name]

            queue.bind(exchange, topic)
            queue.consume(self._on_event_message(pipe_name=pipe))

    def _create_retries(self, pipe: Pipe, queue: AmqpQueue):
        pub_channel = self._publish_channel
        sub_channel = self._listen_channel

        retry_dlx_exchange_name = RETRY_DLX_EXCHANGE_NAME \
            .format(name=pipe.name)
        retry_dlx_exchange = pub_channel.exchange(
            retry_dlx_exchange_name,
            'topic',
            durable=True,
        )

        retry_exchange_name = RETRY_EXCHANGE_NAME.format(name=pipe.name)
        retry_exchange = pub_channel.exchange(
            retry_exchange_name,
            'topic',
            durable=True,
        )

        service_exchange_name = PUBSUB_EXCHANGE.format(name=pipe.name)
        service_queue_name = PUBSUB_QUEUE.format(name=pipe.name)
        queue.bind(retry_dlx_exchange, service_queue_name)

        for retry_time in pipe.retries:
            retry_queue_name = RETRY_QUEUE_NAME.format(
                time=retry_time,
                name=pipe.name,
            )

            retry_queue = sub_channel.queue(
                retry_queue_name,
                durable=True,
                props={
                    'x-message-ttl': transform_retry_time(retry_time),
                    'x-dead-letter-exchange': retry_dlx_exchange_name,
                    'x-dead-letter-routing-key': service_exchange_name,
                },
            )
            retry_queue.bind(retry_exchange, retry_queue_name)
