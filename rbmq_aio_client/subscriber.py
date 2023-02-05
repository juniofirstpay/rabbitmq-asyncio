import json
import addict
import asyncio
import aio_pika
import aio_pika.abc
import pprint


class Subscriber:
    
    def __init__(self, config: "dict"):
        self.__config = addict.Dict(config)

    async def main(self, loop, connection_type: "str", queue: "str"):
        print("Connecting xxxxxxxxxxxxxxxxxxxx")
        connection_args = self.__config.connections.get(connection_type)
        queue_args = self.__config.queues.get(queue)
        exchange_args = self.__config.exchanges.get(queue_args.exchange)
        
        connection: aio_pika.RobustConnection = await aio_pika.connect_robust(connection_args.uri, loop=loop)
        
        print("Connection established")
        channel: aio_pika.abc.AbstractChannel = await connection.channel()
        
        print("Channel established")
        exchange: aio_pika.Exchange = await channel.declare_exchange(exchange_args.name, 
                                                                     exchange_args.type, 
                                                                     durable=exchange_args.durable,
                                                                     auto_delete=exchange_args.auto_delete,
                                                                     internal=exchange_args.internal,
                                                                     passive=exchange_args.passive,
                                                                     timeout=exchange_args.timeout)
        print("Exchange established")
        # exchange = await channel.get_exchange(exchange_args.name)
        async with connection:
            # Creating channel
            channel: aio_pika.abc.AbstractChannel = await connection.channel()
            # Declaring queue
            queue: aio_pika.abc.AbstractQueue = await channel.declare_queue(queue_args.name,
                                                                            auto_delete=queue_args.auto_delete)
            await queue.bind(exchange, 
                             routing_key=queue_args.routing_key)

            async with queue.iterator() as queue_iter:
                # Cancel consuming after __aexit__
                async for message in queue_iter:
                    message_info = message.info()
                    pprint.pprint(message_info)
                    try:
                        if message_info.get('redelivered') == False and message_info.get('headers').get('x-index') % 2 == 0:
                            raise Exception("Free Exception")
                        async with message.process(requeue=True):
                            pprint.pprint([
                                { 'redelivered': message_info.get('redelivered') },
                                json.loads(message.body.decode())
                            ])
                    except Exception as e:
                        await message.reject(requeue=True)

    def run(self, connection, queue):
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.main(loop, connection, queue))
            loop.close()
        except Exception as e:
            print(e)

if __name__ == '__main__':
    Subscriber()