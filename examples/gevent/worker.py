from gevent import monkey  # isort:skip
monkey.patch_all()  # isort:skip

import timeit  # noqa: E402

from gevent import sleep  # noqa: E402

from simple_amqp import AmqpParameters  # noqa: E402
from simple_amqp_pubsub import Subscriber  # noqa: E402
from simple_amqp_pubsub.gevent import GeventAmqpPubSub  # noqa: E402

pubsub_conn = GeventAmqpPubSub(
    AmqpParameters(),
    'logs.worker',
    retries=['5s', '10s', '30s'],
)


class LogService:
    sub = Subscriber('logs.worker')

    def __init__(self):
        self._last_dt = timeit.default_timer()

    @sub.listen('logs')
    def logs(self, log_line: str):
        print('## log line: ', log_line)
        time = timeit.default_timer()
        print('## dt {0:.2f}ms'.format((time - self._last_dt) * 1000))
        self._last_dt = time


logs_service = LogService()
pubsub_conn \
    .add_subscriber(logs_service)

pubsub_conn.configure()
pubsub_conn.start()

while True:
    sleep(1)
