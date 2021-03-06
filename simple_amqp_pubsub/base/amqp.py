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
            logger=None,
    ):
        super().__init__(logger=logger)
        if conn is not None:
            self.conn = conn
        else:
            self.conn = self._create_conn(params)

        self._encoders = {'json': encode_event}
        self._decoders = {'json': decode_event}

        self.setup_stage_name = '1:pubsub.setup'
        self.setup_stage = None
        self.listen_stage_name = '2:pubsub.listen'
        self.listen_stage = None

        self._publish_channels: Dict[str, AmqpChannel] = {}
        self._listen_channels: Dict[str, AmqpChannel] = {}

        self._exchanges: Dict[str, AmqpExchange] = {}
        self._queues: Dict[str, AmqpQueue] = {}

    def _create_conn(self, params: AmqpParameters):
        raise NotImplementedError

    def configure(self):
        self._configure_stages()
        self._create_sources()
        self._create_pipes()
        self._bind_handlers()

    def start(self, auto_reconnect: bool=True):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def client(self, source: Source) -> PubSubClient:
        self._add_source(source)
        return self.CLIENT_CLS(self, source)

    def push_event(self, event: Event):
        self.log_event_sent(event)
        source = self._sources[event.source]
        encoder = self._encoders[source.encoding]
        msg = encoder(event)
        msg = msg.replace(
            exchange=PUBSUB_EXCHANGE.format(name=event.source),
            topic=event.topic,
        )
        channel = self._publish_channels[event.source]
        return self._send_event_msg(channel, msg)

    def add_encoder(self, name: str, encoder):
        self._encoders[name] = encoder

    def add_decoder(self, name: str, decoder):
        self._decoders[name] = decoder

    def _send_event_msg(self, channel: AmqpChannel, msg: AmqpMsg):
        raise NotImplementedError

    def _on_event_message(self, msg: AmqpMsg):
        raise NotImplementedError

    def _retry_event(self, event: Event, pipe: Pipe) -> AmqpMsg:
        try:
            retry_time = pipe.retries[event.retry_count]
        except IndexError:
            return

        event = event.replace(retry_count=event.retry_count+1)
        source = self._sources[event.source]
        encoder = self._encoders[source.encoding]

        retry_exchange = RETRY_EXCHANGE_NAME.format(name=event.pipe)
        retry_queue = RETRY_QUEUE_NAME.format(
            time=retry_time,
            name=event.pipe,
        )

        msg = encoder(event)
        msg = msg.replace(
            exchange=retry_exchange,
            topic=retry_queue,
        )
        return msg

    def _configure_stages(self):
        self.setup_stage = self.conn.stage(self.setup_stage_name)
        self.listen_stage = self.conn.stage(self.listen_stage_name)

    def _create_sources(self):
        for source in self._sources.values():
            channel = self.conn.channel(stage=self.setup_stage)
            self._publish_channels[source.name] = channel
            exchange_name = PUBSUB_EXCHANGE.format(name=source.name)
            exchange = channel.exchange(
                exchange_name,
                'topic',
                durable=source.durable,
                stage=self.setup_stage,
            )

            self._exchanges[exchange_name] = exchange

    def _create_pipes(self):
        for pipe in self._pipes.values():
            channel = self.conn.channel(stage=self.setup_stage)
            self._listen_channels[pipe.name] = channel
            queue_name = PUBSUB_QUEUE.format(name=pipe.name)
            queue = channel.queue(
                queue_name,
                durable=pipe.durable,
                stage=self.setup_stage,
            )

            self._queues[queue_name] = queue

            if pipe.retries_enabled:
                self._create_retries(pipe, queue)

    def _bind_handlers(self):
        for source_name, topic, pipe_name in self._handlers:
            exchange_name = PUBSUB_EXCHANGE.format(name=source_name)
            exchange = self._exchanges[exchange_name]

            queue_name = PUBSUB_QUEUE.format(name=pipe_name)
            queue = self._queues[queue_name]

            queue.bind(exchange, topic, stage=self.setup_stage)

            source = self._sources[source_name]
            handler = self._on_event_message(
                pipe_name=pipe_name,
                decoder=self._decoders[source.encoding],
            )
            queue.consume(handler, stage=self.listen_stage)

    def _create_retries(self, pipe: Pipe, queue: AmqpQueue):
        channel = self._listen_channels[pipe.name]

        retry_dlx_exchange_name = RETRY_DLX_EXCHANGE_NAME \
            .format(name=pipe.name)
        retry_dlx_exchange = channel.exchange(
            retry_dlx_exchange_name,
            'topic',
            durable=pipe.durable,
            stage=self.setup_stage,
        )

        retry_exchange_name = RETRY_EXCHANGE_NAME.format(name=pipe.name)
        retry_exchange = channel.exchange(
            retry_exchange_name,
            'topic',
            durable=pipe.durable,
            stage=self.setup_stage,
        )

        service_exchange_name = PUBSUB_EXCHANGE.format(name=pipe.name)
        service_queue_name = PUBSUB_QUEUE.format(name=pipe.name)
        queue.bind(
            retry_dlx_exchange,
            service_queue_name,
            stage=self.setup_stage,
        )

        for retry_time in pipe.retries:
            retry_queue_name = RETRY_QUEUE_NAME.format(
                time=retry_time,
                name=pipe.name,
            )

            retry_queue = channel.queue(
                retry_queue_name,
                durable=pipe.durable,
                props={
                    'x-message-ttl': transform_retry_time(retry_time),
                    'x-dead-letter-exchange': retry_dlx_exchange_name,
                    'x-dead-letter-routing-key': service_exchange_name,
                },
                stage=self.setup_stage,
            )
            retry_queue.bind(
                retry_exchange,
                retry_queue_name,
                stage=self.setup_stage,
            )
