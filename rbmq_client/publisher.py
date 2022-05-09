class Publisher:
    class Transaction(object):
        def __init__(self, channel, exchange, routing_key_prefix):
            self.channel = channel
            self.exchange = exchange
            self.routing_key_prefix = routing_key_prefix

        def start(self):
            self.channel.tx_select()
            return self

        def add(self, key,  message):
            self.channel.basic_publish(
                self.exchange,
                self.routing_key_prefix,
                message,
            )
            return self

        def confirm(self):
            self.channel.tx_commit()
            return self

        def stop(self):
            self.channel.tx_rollback()
            self.channel.close()

    def __init__(self, connection, logger):
        self.connection = connection
        self.logger = logger

    def set_config(self, queue_config):
        self.queue_config = queue_config
        return self

    def start(self):
        self.channel = self.connection.channel()
        self.__declare_exchange(self.channel)
        return self

    def stop(self):
        self.channel.close()
        self.logger.info("Channel closed")

    def push(self, key, message):
        routing_key = self.queue_config.get("routing_key_prefix") + key
        self.logger.msg(f"Routing Key: {routing_key}")
        self.channel.basic_publish(
            self.queue_config.get("exchange"),
            routing_key,
            message,
        )

    def __declare_exchange(self, channel):
        channel.exchange_declare(
            self.queue_config.get("exchange"),
            exchange_type=self.queue_config.get("exchange_type"),
            passive=self.queue_config.get("exchange_passive", False),
            durable=self.queue_config.get("exchange_durable", False),
            auto_delete=self.queue_config.get("exchange_auto_delete", False),
        )

    def transaction(self):
        channel = self.connection.channel()
        self.__declare_exchange(channel)
        return Publisher.Transaction(
            channel,
            self.queue_config.get("exchange"),
            self.queue_config.get("routing_key_prefix"),
        ).start()
