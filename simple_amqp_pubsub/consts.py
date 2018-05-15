PUBSUB_EXCHANGE = 'events.{service}'
PUBSUB_QUEUE = 'events.{service}'

RETRY_EXCHANGE_NAME = 'events-retry.{service}'
RETRY_QUEUE_NAME = 'events-retry.{time}.{service}'
RETRY_DLX_EXCHANGE_NAME = 'events-retry-dlx.{service}'
