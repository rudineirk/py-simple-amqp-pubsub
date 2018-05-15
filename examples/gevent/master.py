from gevent import monkey  # isort:skip
monkey.patch_all()  # isort:skip

import timeit  # noqa: E402

from gevent import sleep  # noqa: E402

from simple_amqp import AmqpParameters  # noqa: E402
from simple_amqp_pubsub.gevent import GeventAmqpPubSub  # noqa: E402

pubsub_conn = GeventAmqpPubSub(
    AmqpParameters(),
    'logs.master',
    enable_retries=False,
)

logs_client = pubsub_conn.client('logs.worker')

pubsub_conn.configure()
pubsub_conn.start()

inc = 0
while True:
    start = timeit.default_timer()
    sleep(.001)
    logs_client.push('logs', 'log #{}'.format(inc))
    inc += 1
    end = timeit.default_timer()
    print('# dt {0:.2f}ms'.format((end - start) * 1000))
