from connection import get_sync_connection, logger
from publisher import Publisher

try:
   connection = get_sync_connection()
   publisher = Publisher(connection, logger)
   transaction = publisher.set_config({
      'queue': 'test_queue',
      'prefetch_size': 0,
      'prefetch_count': 1,
      'exchange': 'test_queue_exchange',
      'exchange_type': 'topic',
      'exchange_durable': True,
      'routing_key': 'events.test.queue.*',
      'durable': True,
   }).transaction()
   transaction.add('test1')
   transaction.add('test2')
   transaction.add('test3')
   transaction.add('test4')
   transaction.add('test5')
   transaction.confirm()
   transaction.stop()
except Exception as e:
   print(e)
