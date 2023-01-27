import threading
import queue
import pika
import structlog
import traceback

class PublisherAsync:

    def __init__(self, credentials, queue_config, **kwargs) -> None:
        self.connection = None
        self.channel = None
        self.logger = structlog.get_logger()
        self._stopping = False
        
        self.credentials = credentials
        self.queue_config = queue_config

        queue_max_size = kwargs.get('queue_max_size', 10000000)
        
        self._message_queue = queue.Queue(maxsize=10000000)
        self.open_retry_interval = 1
        self.should_auto_close = False
        
    def start(self):
        self.logger.info("Starting Thread")
        old_connection_ioloop = None
        if getattr(self, 'thread', None):
            old_connection_ioloop = self.connection.ioloop
            self.open_retry_interval *= 2

        self.thread = threading.Thread(target=self.run, daemon=False)
        self.thread.name = f"Thread #{self.open_retry_interval}"
        self.thread.setDaemon(True)
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
                    if not self._stopping:
                        self.connection.ioloop.call_later(self.open_retry_interval, self.start)
                        self.logger.msg("Connection closed callback")

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
            except KeyboardInterrupt as e:
                self.logger.msg(f"Interrupt: {e}")
                self.connection.close()
                self.logger.msg("Connection Closed")
            except Exception as e:
                self.logger.msg(f"Exception: {e}")
                if self.connection.is_open:
                    self.connection.close()
                self.logger.msg("Connection Exception")
                # self.connection.ioloop.start()
                if not self._stopping:
                    self.connection.ioloop.call_later(self.open_retry_interval, self.start)
                    self.logger.msg(f"Connection Retry Interval {self.open_retry_interval}")
                    self.connection.ioloop.start()
            
        except Exception as e:
            self.logger.info(e)
            
        self.logger.info("Thread execution finished")

    def close(self) -> None:
        self._stopping = True
        if self.channel and self.channel.is_open:
            self.channel.close()
            self.logger.info("Channel closed")

        if not self.channel:
            self.logger.info("No channel exists to close")
        elif not self.channel.is_open:
            self.logger.info("Channel is already closed")
        
        if self.connection and self.connection.is_open:
            self.connection.close()
            self.connection.ioloop.stop()
        
        # if self.thread.is_alive():
        #     self.thread._stop()
    
    def on_open(self, channel):
        channel.add_on_close_callback(self.on_close)
        self.logger.info("Channel Established")
        self.channel = channel
        self.configure()


    def on_close(self, channel, *args, **kwargs):
        self.logger.critical("Channel Closed")
        if not self._stopping:
            if self.connection.is_open:
                self.channel.open()

    def configure(self):
        on_exchange_declare = lambda x: self.schedule_messaging()
        self.channel.exchange_declare(self.queue_config.get('exchange'), 
                                      exchange_type=self.queue_config.get('exchange_type'), 
                                      passive=self.queue_config.get('exchange_passive', False),
                                      durable=self.queue_config.get('exchange_durable', False),
                                      auto_delete=self.queue_config.get('exchange_auto_delete', False),
                                      callback=on_exchange_declare)

    def schedule_messaging(self):
        if self._message_queue.empty() == False and self.channel and self.channel.is_open:
            try:
                message_obj = self._message_queue.get()
                sent = self.publish(message_obj.get('key'), 
                            message_obj.get('message'), 
                            message_obj.get('routing_key_prefix', None))
                if sent == False:
                    self._message_queue.put(message_obj)
                elif self.should_auto_close == True and self._message_queue.empty() == True:
                    self.logger.msg(f"Message sent and closing automatically")
                    self.close()
                    return 
            except Exception as e:
                self.logger.error(f"Publishing Error: {e}")
                traceback.print_exc()
        
        if not self._stopping:
            self.connection.ioloop.call_later(0.3, self.schedule_messaging)


    def publish(self, key, message, routing_key_prefix=None):
        if not self.channel or not self.channel.is_open:
            self.logger.info(f"Skipping Message: {message}")
            self._message_queue.put({
                'key': key,
                'message': message,
                'routing_key_prefix': routing_key_prefix
            })
            return False

        routing_key = (routing_key_prefix or self.queue_config.get("routing_key_prefix") or "") + key
        self.logger.msg(f"Routing Key: {routing_key}")
        self.channel.basic_publish(self.queue_config.get("exchange"),
                                   routing_key,
                                   message)
        return True
    
    def push(self, key, message, routing_key_prefix=None):
        if not self.thread.is_alive:
            self.start()
        self._message_queue.put({
            'key': key,
            'message': message,
            'routing_key_prefix': routing_key_prefix
        })
        
    