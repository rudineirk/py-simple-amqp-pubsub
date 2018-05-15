import traceback

from simple_amqp import AmqpMsg, AmqpParameters
from simple_amqp.asyncio import AsyncioAmqpConnection
from simple_amqp_pubsub import Event
from simple_amqp_pubsub.base import BaseAmqpPubSub


class AsyncioAmqpPubSub(BaseAmqpPubSub):
    async def start(self, auto_reconnect: bool=True, wait: bool=True):
        await self.conn.start(auto_reconnect, wait)

    async def stop(self):
        await self.conn.stop()

    def _create_conn(self, params: AmqpParameters):
        return AsyncioAmqpConnection(params)

    async def recv_event(self, event: Event):
        handler, error = self._get_handler(event.service, event.topic)
        if error:
            return error

        try:
            await handler(event.payload)
            return
        except Exception as e:
            if not self._recv_error_handlers:
                traceback.print_exc()
            else:
                for handler in self._recv_error_handlers:
                    handler(e)

        if not self._enable_retries:
            return 'error processing message'

        await self._retry_event(event)

    async def _retry_event(self, event: Event):
        msg = super()._retry_event(event)
        if not msg:
            return

        await self._send_event_msg(msg)

    async def _send_event_msg(self, msg: AmqpMsg):
        await self._publish_channel.publish(msg)

    async def _on_event_message(self, msg: AmqpMsg) -> bool:
        event = self._decode_event(msg)
        resp = await self.recv_event(event)
        if resp is not None:
            return False
        return True
