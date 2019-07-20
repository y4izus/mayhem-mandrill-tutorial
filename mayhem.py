import asyncio
import logging
import random
import string
import uuid
from dataclasses import dataclass, field


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


@dataclass
class PubSubMessage:
    instance_name: str
    message_id: int = field(repr=False)
    hostname: str = field(repr=False, init=False)
    restarted: bool = field(repr=False, default=False)

    def __post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'


async def restart_host(msg):
    await asyncio.sleep(random.random())
    msg.restarted = True
    logging.info(f'Restarted {msg.hostname}')


# Publish only n messages
# async def publish(queue, n):
#     choices = string.ascii_lowercase + string.digits
#     for x in range(1, n + 1):
#         host_id = ''.join(random.choices(choices, k=4))
#         instance_name = f'cattle-{host_id}'
#         msg = PubSubMessage(message_id=x, instance_name=instance_name)
#         # publish an item
#         await queue.put(msg)
#         logging.info(f'Published {x} of {n} messages')

#     # indicate the publisher is done
#     await queue.put(None)


# Publish messages forever (asynchronous)
async def publish(queue, label):
    choices = string.ascii_lowercase + string.digits

    while True:
        msg_id = str(uuid.uuid4())
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(message_id=msg_id, instance_name=instance_name)
        # publish an item
        asyncio.create_task(queue.put(msg))
        logging.info(f'{label} >>> Published message {msg}')
        # simulate randomness of publishing messages
        await asyncio.sleep(random.random())


# Synchronous
# async def consume(queue):
#     while True:
#         # wait for an item from the publisher
#         msg = await queue.get()

#         # the publisher emits None to indicate that it is done
#         if msg is None:
#             break

#         # process the msg
#         logging.info(f'Consumed {msg}')
#         await restart_host(msg)


# Asynchronous
async def consume(queue):
    while True:
        # wait for an item from the publisher
        msg = await queue.get()

        # process the msg
        logging.info(f'Consumed {msg}')
        asyncio.create_task(restart_host(msg))
        

# Execute one task, then another and then finish        
# def main():
#     queue = asyncio.Queue()
#     asyncio.run(publish(queue, 5))
#     asyncio.run(consume(queue))


# Execute one service and run forever
# def main():
#     queue = asyncio.Queue()
#     loop = asyncio.get_event_loop()
#     loop.create_task(publish(queue, 5))
#     loop.create_task(consume(queue))
#     loop.run_forever()
#     loop.close()
#     logging.info('Successfully shutdown the Mayhem service.')


# Execute one service and run forever and show message when interrupted
# def main():
#     queue = asyncio.Queue()
#     loop = asyncio.get_event_loop()

#     try:
#         loop.create_task(publish(queue, 5))
#         loop.create_task(consume(queue))
#         loop.run_forever()
#     except KeyboardInterrupt:
#         logging.info('Process interrupted')
#     finally:
#         loop.close()
#         logging.info('Successfully shutdown the Mayhem service.')


def main():
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    try:
        loop.create_task(publish(queue, 'Q1'))
        loop.create_task(publish(queue, 'Q2'))
        loop.create_task(consume(queue))
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info('Process interrupted')
    finally:
        loop.close()
        logging.info('Successfully shutdown the Mayhem service.')


if __name__ == '__main__':
    main()