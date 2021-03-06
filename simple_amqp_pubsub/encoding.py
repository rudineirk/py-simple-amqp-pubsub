import json

from simple_amqp import AmqpMsg

from .data import Event

CONTENT_TYPE_MSGPACK = 'application/msgpack'


def encode_event(event: Event) -> AmqpMsg:
    payload = json.dumps({
        'source': event.source,
        'topic': event.topic,
        'payload': event.payload,
    })
    payload = payload.encode('utf8')
    headers = {}
    if event.retry_count > 0:
        headers['retry_count'] = event.retry_count

    return AmqpMsg(
        payload=payload,
        content_type=CONTENT_TYPE_MSGPACK,
        headers=headers,
    )


def decode_event(msg: AmqpMsg, pipe_name: str) -> Event:
    payload = json.loads(msg.payload)
    retry_count = msg.headers.get('retry_count', 0)

    return Event(
        source=payload['source'],
        topic=payload['topic'],
        payload=payload['payload'],
        pipe=pipe_name,
        retry_count=retry_count,
    )
