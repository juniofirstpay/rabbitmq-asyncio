from connection import get_sync_connection, logger
from publisher import Publisher

try:
   connection = get_sync_connection()
   publisher = Publisher(connection, logger)
   publisher.set_config({
      'queue': 'test_queue',
      'prefetch_size': 0,
      'prefetch_count': 1,
      'exchange': 'test_queue_exchange',
      'exchange_type': 'topic',
      'exchange_durable': True,
      'routing_key': 'events.test-1.queue.*',
      'durable': True,
   })
   publisher.push('test1')
   publisher.push('test2')
   publisher.push('test3')
   publisher.push('test4')
   publisher.push('test5')
except Exception as e:
   print(e)
