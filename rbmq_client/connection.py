import pika
import structlog
logger = structlog.get_logger()


def get_async_connection(host, port, username, password, callback=None):
    logger.info("Connection Parameters")
    credentials = pika.PlainCredentials(username, password)
    connection_parameters = pika.ConnectionParameters(host=host, port=port, credentials=credentials)
    connection = pika.SelectConnection(connection_parameters)
    logger.info("Connection Configured")
    try:
        def on_connection_close(*args, **kwargs):
            logger.info("Connection closed callback")

        def on_connection_open(connection):
            logger.info("Connection Open. Callback received")
            try: 
                if callback:
                    callback.__call__(connection)
            except Exception as e:
                print(e)
        
        connection.add_on_open_callback(on_connection_open)
        connection.add_on_close_callback(on_connection_close)
        logger.info("Starting IOLoop")
        connection.ioloop.start()

    except Exception as e:
        logger.info(f"Exception: {e}")
        connection.close()
        logger.info("Connection Closed")
        connection.ioloop.start()
    except KeyboardInterrupt as e:
        logger.info(f"Interrupt: {e}")
        connection.close()
        logger.info("Connection Closed")

def get_sync_connection(host, port, username, password):
    logger = structlog.get_logger()
    logger.info("Connection Parameters")
    credentials = pika.PlainCredentials(username, password)
    connection_parameters = pika.ConnectionParameters(host=host, port=port,credentials=credentials, heartbeat=0)
    connection = pika.BlockingConnection(connection_parameters)
    logger.info("Connection Configured")
    return connection