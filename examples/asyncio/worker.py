import timeit
from asyncio import get_event_loop, set_event_loop_policy, sleep

import uvloop

from simple_amqp import AmqpParameters
from simple_amqp_pubsub import Subscriber
from simple_amqp_pubsub.asyncio import AsyncioAmqpPubSub

set_event_loop_policy(uvloop.EventLoopPolicy())

pubsub_conn = AsyncioAmqpPubSub(
    AmqpParameters(),
    'logs.worker',
    retries=['5s', '10s', '30s'],
)


class LogService:
    sub = Subscriber('logs.worker')

    def __init__(self):
        self._last_dt = timeit.default_timer()

    @sub.listen('logs')
    async def logs(self, log_line: str):
        print('## log line: ', log_line)
        time = timeit.default_timer()
        print('## dt {0:.2f}ms'.format((time - self._last_dt) * 1000))
        self._last_dt = time


logs_service = LogService()
pubsub_conn \
    .add_subscriber(logs_service)

pubsub_conn.configure()

loop = get_event_loop()


async def main():
    await pubsub_conn.start()
    while True:
        await sleep(1)

loop.run_until_complete(main())
