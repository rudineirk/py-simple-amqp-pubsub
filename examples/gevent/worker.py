from gevent import monkey  # isort:skip
monkey.patch_all()  # isort:skip

import timeit  # noqa: E402

from gevent import sleep  # noqa: E402
from simple_amqp import AmqpParameters  # noqa: E402

from simple_amqp_pubsub import Event, Pipe, Source, Subscriber  # noqa: E402
from simple_amqp_pubsub.gevent import GeventAmqpPubSub  # noqa: E402

pubsub_conn = GeventAmqpPubSub(
    params=AmqpParameters(),
)

LOGS_SOURCE = Source(name='logs')
LOGS_PIPE = Pipe(
    name='logs.worker',
    retries=['5s', '10s', '30s'],
)


class LogService:
    sub = Subscriber(LOGS_PIPE)

    def __init__(self):
        self._last_dt = timeit.default_timer()

    @sub.listen(LOGS_SOURCE, 'logs')
    def logs(self, event: Event):
        log_line = event.payload

        print('## log line: ', log_line)
        time = timeit.default_timer()
        print('## dt {0:.2f}ms'.format((time - self._last_dt) * 1000))
        self._last_dt = time


logs_service = LogService()
pubsub_conn \
    .add_subscriber(logs_service.sub, logs_service)

pubsub_conn.configure()
pubsub_conn.start()

while True:
    sleep(1)
