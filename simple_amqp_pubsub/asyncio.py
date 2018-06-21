import traceback

from simple_amqp import AmqpChannel, AmqpMsg, AmqpParameters
from simple_amqp.asyncio import AsyncioAmqpConnection

from simple_amqp_pubsub import Event, Pipe
from simple_amqp_pubsub.base import BaseAmqpPubSub


class AsyncioAmqpPubSub(BaseAmqpPubSub):
    async def start(self, auto_reconnect: bool=True):
        self.conn.add_stage(self.setup_stage)
        self.conn.add_stage(self.listen_stage)
        await self.conn.start(auto_reconnect)

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

        channel = self._listen_channels[pipe.name]
        await self._send_event_msg(channel, msg)

    async def _send_event_msg(self, channel: AmqpChannel, msg: AmqpMsg):
        await channel.publish(msg)

    def _on_event_message(self, pipe_name: str, decoder):
        async def msg_handler(msg: AmqpMsg) -> bool:
            event = decoder(msg, pipe_name)
            resp = await self.recv_event(event)
            if resp is not None:
                return False
            return True

        return msg_handler
