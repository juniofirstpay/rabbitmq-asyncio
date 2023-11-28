from rbmq_client.connection import get_async_connection, logger
from rbmq_client.publisher import Publisher

try:
   connection = get_async_connection(
      "10.0.0.46",
      5672,
      "queue_user",
      "queue_user_password",
      Publisher(logger).set_config({
         'queue': 'test_queue',
         'prefetch_size': 0,
         'prefetch_count': 1,
         'exchange': 'test_queue_exchange',
         'exchange_type': 'topic',
         'exchange_durable': True,
         'routing_key': 'events.test.queue.*',
         'durable': True,
      }))
   # publisher = 
   
   # transaction.add('test1')
   # transaction.add('test2')
   # transaction.add('test3')
   # transaction.add('test4')
   # transaction.add('test5')
   # transaction.confirm()
   # transaction.stop()
except Exception as e:
   print(e)
