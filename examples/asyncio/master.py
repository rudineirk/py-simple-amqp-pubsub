import timeit
from asyncio import get_event_loop, set_event_loop_policy, sleep

import uvloop

from simple_amqp import AmqpParameters
from simple_amqp_pubsub.asyncio import AsyncioAmqpPubSub

set_event_loop_policy(uvloop.EventLoopPolicy())


pubsub_conn = AsyncioAmqpPubSub(
    AmqpParameters(),
    'logs.master',
    enable_retries=False,
)

logs_client = pubsub_conn.client('logs.worker')
pubsub_conn.configure()

loop = get_event_loop()


async def main():
    await pubsub_conn.start()

    inc = 0
    while True:
        start = timeit.default_timer()
        await sleep(.001)
        await logs_client.push('logs', 'log #{}'.format(inc))
        inc += 1
        end = timeit.default_timer()
        print('# dt {0:.2f}ms'.format((end - start) * 1000))

loop.run_until_complete(main())
