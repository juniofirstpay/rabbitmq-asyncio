import threading


class Consumer(threading.Thread):

    def __init__(self, connection, logger, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs, daemon=True)
        self.connection = connection
        self.channel = None
        self.logger = logger

    def set_config(self, queue_config):
        self.queue_config=queue_config
        return self

    def set_callback(self, callback):
        self.on_message_callback =callback
        return self

    def start(self) -> None:
        super().start()
        self.connection.channel(on_open_callback=self.on_open)
        return self
    
    def on_open(self, channel):
        channel.add_on_close_callback(self.on_close)
        self.logger.info("Channel Established")
        self.channel = channel
        self.configure()


    def on_close(self, channel, *args, **kwargs):
        print(channel, *args)
        self.logger.critical("Channel Closed")

    def configure(self):
        on_queue_qos = lambda x: self.channel.basic_consume(self.queue_config.get('queue', ''), 
                                                            self.on_message,
                                                            auto_ack=self.queue_config.get('auto_ack', True))
        
        on_queue_bind = lambda x: self.channel.basic_qos(prefetch_size=self.queue_config.get('prefetch_size', 0), 
                                                        prefetch_count=self.queue_config.get('prefetch_count', 0),
                                                        callback=on_queue_qos)
        
        on_queue_declare = lambda x: self.channel.queue_bind(self.queue_config.get('queue', ''), 
                                                            self.queue_config.get('exchange'), 
                                                            routing_key=self.queue_config.get('routing_key'),
                                                            arguments=self.queue_config.get('bind_arguments', {}),
                                                            callback=on_queue_bind)
        
        on_exchange_declare = lambda x: self.channel.queue_declare(self.queue_config.get('queue', ''),
                                                                   passive=self.queue_config.get('passive', False),
                                                                   durable=self.queue_config.get('durable', False),
                                                                   exclusive=self.queue_config.get('exclusive', False),
                                                                   auto_delete=self.queue_config.get('auto_delete', False),
                                                                   arguments=self.queue_config.get('declare_arguments', {}),
                                                                   callback=on_queue_declare)
        self.channel.exchange_declare(self.queue_config.get('exchange'), 
                                      exchange_type=self.queue_config.get('exchange_type'), 
                                      passive=self.queue_config.get('exchange_passive', False),
                                      durable=self.queue_config.get('exchange_durable', False),
                                      auto_delete=self.queue_config.get('exchange_auto_delete', False),
                                      callback=on_exchange_declare)

    def on_message(self, *args, **kwargs):
        self.logger.info("Message Received")
        self.on_message_callback.__call__(*args, **kwargs)
