from rbmq_client.consumer_async import ConsumerAsync
from time import sleep

def on_message(*args, **kwargs):
    print(*args, **kwargs)

consumer = ConsumerAsync(
    {
        "host": "localhost",
        "port": 5672,
        "username": "guest",
        "password": "guest"
    },
    {
        'queue': 'test_queue',
        'prefetch_size': 0,
        'prefetch_count': 1,
        'exchange': 'test_queue_exchange',
        'exchange_type': 'topic',
        'exchange_durable': True,
        'routing_key': 'items.*',
        'durable': True,
    }, 
    on_message
).start()