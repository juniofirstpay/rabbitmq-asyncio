-----------------------------
#### Base Config File
````
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
````