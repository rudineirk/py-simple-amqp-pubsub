import traceback

from simple_amqp import AmqpMsg, AmqpParameters
from simple_amqp.asyncio import AsyncioAmqpConnection

from simple_amqp_pubsub import Event, Pipe
from simple_amqp_pubsub.base import BaseAmqpPubSub


class AsyncioAmqpPubSub(BaseAmqpPubSub):
    async def start(self, auto_reconnect: bool=True, wait: bool=True):
        await self.conn.start(auto_reconnect, wait)

    async def stop(self):
        await self.conn.stop()

    def _create_conn(self, params: AmqpParameters):
        return AsyncioAmqpConnection(params)

    async def recv_event(self, event: Event):
        self.log_event_recv(event)
        handler, error = self._get_handler(
            event.source,
            event.topic,
            event.pipe,
        )
        if error:
            return error

        try:
            await handler(event)
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

        await self._retry_event(event, pipe)

    async def _retry_event(self, event: Event, pipe: Pipe):
        msg = super()._retry_event(event, pipe)
        if not msg:
            return

        await self._send_event_msg(msg)

    async def _send_event_msg(self, msg: AmqpMsg):
        await self._publish_channel.publish(msg)

    def _on_event_message(self, pipe_name: str):
        async def msg_handler(msg: AmqpMsg) -> bool:
            event = self._decode_event(msg, pipe_name)
            resp = await self.recv_event(event)
            if resp is not None:
                return False
            return True

        return msg_handler
