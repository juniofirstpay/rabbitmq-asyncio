from connection import get_async_connection, logger
from consumer import Consumer


def callback(connection):
    def on_message(*args, **kwargs):
        print(*args, **kwargs)

    Consumer(connection, logger).set_config(
        {
            "queue": "test_queue_1",
            "prefetch_size": 0,
            "prefetch_count": 1,
            "exchange": "test_queue_exchange",
            "exchange_type": "topic",
            "exchange_durable": True,
            "routing_key": "events.test.queue.*",
            "durable": True,
        }
    ).set_callback(on_message).start()


get_async_connection(callback)
