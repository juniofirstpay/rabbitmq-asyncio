import time
import socket
import ast
import traceback
from typing import Optional, Callable
from structlog import get_logger
from .consumer_async import ConsumerAsync

logger = get_logger(__name__)

class ConsumerServer(object):
    
    def __init__(self, 
                 credentials: "dict",
                 consumer: "dict",
                 ip: "str"='0.0.0.0', 
                 port: "int"=8000, 
                 handle_method: "Optional[Callable]"=None,
                 log=False, 
                 retry_count=5):
        
        super(ConsumerServer, self).__init__()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__ip = ip
        self.__port = port
        self.log = log
        self.retry_count = retry_count
        self.current_try_count = 0

        self.handle_method = handle_method or self.__default_handle_method
        
        self.__credentials = credentials
        self.__consumer = consumer
    
    def __default_handle_method(self):
        """ Simple handle method that accepts the connection then closes.
        """
        while True:
            if self.log:
                logger.info("Waiting for a connection")

            connection, client_address = self.sock.accept()
            try:
                if self.log:
                    logger.info("Client connected: {0}".format(client_address))
                connection.sendall("TEST_COMPLETE".encode("utf-8"))
            finally:
                connection.shutdown(1)
                
    def __on_message(self, channel, delivery, properties, message):
        try:
            payload = message.decode("utf-8")
            payload = ast.literal_eval(payload)
            self.on_message(payload)
        except Exception as e:
            logger.error(e, type="MessageProcesssingException")
            
    def on_message(self, payload: "dict"):
        raise NotImplementedError()
    
    def __run_consumer(self):
        consumer = ConsumerAsync(
            self.__credentials,
            {
                "queue": self.__consumer.get('queue'),
                "prefetch_size": self.__consumer.get('prefetch_size'),
                "prefetch_count": self.__consumer.get('prefetch_count'),
                "exchange": self.__consumer.get('exchange'),
                "exchange_type": self.__consumer.get('exchange_type'),
                "exchange_durable": self.__consumer.get('exchange_durable'),
                "routing_key": self.__consumer.get('routing_key'),
                "durable": self.__consumer.get('durable')
            },
            lambda channel, delivery, properties, message: self.__on_message(channel, delivery, properties, message)
        )
        consumer.start()
    
    def __run_socket_server(self):
        server_address = (self.__ip, self.__port)
        
        if self.log:
            logger.info("Starting health server on {0} port {1}".format(self.__ip, self.__port))
        
        if self.current_try_count > self.retry_count:
            if self.log:
                logger.info("Unable to start health server on {0} port {1}".format(self.__ip, self.__port))
            return
    
        try:
            self.current_try_count = self.current_try_count + 1
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(server_address)
            self.sock.listen(1)
            self.default_handle_method()
        except socket.error as e:
            if self.log:
                logger.debug(traceback.format_exc())
                logger.error(e)
                logger.error("Unable to start health server...retrying in 5s.")
            time.sleep(5)
            self.start()
        except Exception as e:
            logger.error("Unable to start health server due to unknown exception {0}"
                  .format(e))
    
    def start(self):
        try:
            self.__run_consumer()
        except Exception as e:
            logger.error(e)
            time.sleep(10)
            self.__run_consumer()
        
        try:
            self.__run_socket_server()
        except Exception as e:
            logger.error(e)
            time.sleep(10)
            self.__run_socket_server()