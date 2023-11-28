from rbmq_client.publisher_async import PublisherAsync
from time import sleep

publisher = PublisherAsync(
    {
        "host": "localhost",
        "port": 5672,
        "username": "guest",
        "password": "guest"
    },
    {
        "queue": "test_queue_2",
        "prefetch_size": 0,
        "prefetch_count": 1,
        "exchange": "test_queue_exchange",
        "exchange_type": "topic",
        "exchange_durable": True,
        "routing_key": "items.*",
        "durable": True,
    }
).start()
# # publisher.start()
# sleep(3)
# publisher.logger.info("Starting to print messages")

# publisher.push("items.1", "message1")
# publisher.push("items.2", "message2")
# publisher.push("items.3", "message3")
# publisher.push("items.4", "message4")
# publisher.push("items.5", "message5")
# publisher.push("items.6", "message6")
# publisher.push("items.7", "message7")
# publisher.push("items.8", "message8")
# publisher.push("items.9", "message9")

# sleep(30)

# publisher.push("items.1", "message1")
# publisher.push("items.2", "message2")
# publisher.push("items.3", "message3")
# publisher.push("items.4", "message4")
# publisher.push("items.5", "message5")
# publisher.push("items.6", "message6")
# publisher.push("items.7", "message7")
# publisher.push("items.8", "message8")
# publisher.push("items.9", "message9")

# sleep(30)

# publisher.push("items.1", "message1")
# publisher.push("items.2", "message2")
# publisher.push("items.3", "message3")
# publisher.push("items.4", "message4")
# publisher.push("items.5", "message5")
# publisher.push("items.6", "message6")
# publisher.push("items.7", "message7")
# publisher.push("items.8", "message8")
# publisher.push("items.9", "message9")