import threading
import pika
import structlog


class ConsumerAsync:

    def __init__(self, credentials, queue_config, on_message_callback) -> None:
        self.connection = None
        self.channel = None
        self.logger = structlog.get_logger()
        self._stopping = False
        
        self.credentials = credentials
        self.queue_config = queue_config
        self.on_message_callback = on_message_callback
        self.open_retry_interval = 1
        
    def start(self):
        self.logger.info("Starting Thread")
        old_connection_ioloop = None
        if getattr(self, 'thread', None):
            old_connection_ioloop = self.connection.ioloop
            self.open_retry_interval *= 2

        self.thread = threading.Thread(target=self.run, daemon=False)
        self.thread.name = f"Thread #{self.open_retry_interval}"
        self.thread.start()
        
        if old_connection_ioloop:
            old_connection_ioloop.stop()

        return self
    
    def run(self):
        try:
            credentials = pika.PlainCredentials(self.credentials.get('username'), self.credentials.get('password'))
            connection_parameters = pika.ConnectionParameters(
                host=self.credentials.get('host'), 
                port=self.credentials.get('port'), 
                credentials=credentials,
                connection_attempts=3,
                retry_delay=5,
                heartbeat=60)
            self.connection = pika.SelectConnection(connection_parameters)
            try:
                def on_connection_close(*args, **kwargs):
                    self.logger.msg("Connection closed callback")
                    self.connection.ioloop.call_later(self.open_retry_interval, self.start)

                def on_connection_open(connection):
                    self.logger.msg("Connection Open. Callback received")
                    self.connection.channel(on_open_callback=self.on_open)
                
                def on_connection_open_error(*args, **kwargs):
                    self.logger.msg("Connection Open Error")
                    self.connection.ioloop.call_later(self.open_retry_interval, self.start)

                self.connection.add_on_open_callback(on_connection_open)
                self.connection.add_on_close_callback(on_connection_close)
                self.connection.add_on_open_error_callback(on_connection_open_error)
                self.logger.msg("Starting IOLoop")
                self.connection.ioloop.start()
                self.connection.ioloop.call_later(self.open_retry_interval, self.start)
                self.logger.msg(f"Connection Retry Interval {self.open_retry_interval}")
            except Exception as e:
                self.logger.msg(f"Exception: {e}")
                if self.connection.is_open:
                    self.connection.close()
                self.connection.ioloop.call_later(self.open_retry_interval, self.start)
                self.logger.msg(f"Connection Retry Interval {self.open_retry_interval}")
                self.connection.ioloop.start()
            except KeyboardInterrupt as e:
                self.logger.msg(f"Interrupt: {e}")
                self.connection.close()
                self.logger.msg("Connection Closed")
        except Exception as e:
            self.logger.info(e)

    def close(self) -> None:
        self._stopping = True
        if self.channel and self.channel.is_open:
            self.channel.close()
            self.logger.info("Channel closed")

        if not self.channel:
            self.logger.info("No channel exists to close")
        
        if not self.channel.is_open:
            self.logger.info("Channel is already closed")
        
        if self.connection and self.connection.is_open:
            self.connection.close()
        
        if self.thread.is_alive:
            self.thread.stop()
    
    def on_open(self, channel):
        channel.add_on_close_callback(self.on_close)
        self.logger.info("Channel Established")
        self.channel = channel
        self.configure()


    def on_close(self, channel, *args, **kwargs):
        print(channel, *args)
        self.logger.critical("Channel Closed")
        if not self._stopping:
            if self.connection.is_open:
                self.channel.open()

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