import traceback

from simple_amqp import AmqpMsg, AmqpParameters
from simple_amqp.gevent import GeventAmqpConnection

from simple_amqp_pubsub import Event, Pipe
from simple_amqp_pubsub.base import BaseAmqpPubSub


class GeventAmqpPubSub(BaseAmqpPubSub):
    def start(self, auto_reconnect: bool=True, wait: bool=True):
        self.conn.start(auto_reconnect, wait)

    def stop(self):
        self.conn.stop()

    def _create_conn(self, params: AmqpParameters):
        return GeventAmqpConnection(params)

    def recv_event(self, event: Event):
        self.log_event_recv(event)
        handler, error = self._get_handler(
            event.source,
            event.topic,
            event.pipe,
        )
        if error:
            return error

        try:
            handler(event)
            return
        except Exception as e:
            if not self._recv_error_handlers:
                traceback.print_exc()
            else:
                for handler in self._recv_error_handlers:
                    handler(e)

        pipe = self._pipes[event.pipe]
        if not pipe.retries_enabled:
            return 'error processing message'

        self._retry_event(event, pipe)

    def _retry_event(self, event: Event, pipe: Pipe):
        msg = super()._retry_event(event, pipe)
        if not msg:
            return

        self._send_event_msg(msg)

    def _send_event_msg(self, msg: AmqpMsg):
        self._publish_channel.publish(msg)

    def _on_event_message(self, pipe_name: str):
        def msg_handler(msg: AmqpMsg) -> bool:
            event = self._decode_event(msg, pipe_name)
            resp = self.recv_event(event)
            if resp is not None:
                return False
            return True

        return msg_handler
