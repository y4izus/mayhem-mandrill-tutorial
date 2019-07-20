import asyncio
import logging
import random
import string
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


async def publish(queue, n):
    choices = string.ascii_lowercase + string.digits
    for x in range(1, n + 1):
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(message_id=x, instance_name=instance_name)
        # publish an item
        await queue.put(msg)
        logging.info(f'Published {x} of {n} messages')

    # indicate the publisher is done
    await queue.put(None)


async def consume(queue):
    while True:
        # wait for an item from the publisher
        msg = await queue.get()

        # the publisher emits None to indicate that it is done
        if msg is None:
            break

        # process the msg
        logging.info(f'Consumed {msg}')
        await restart_host(msg)


def main():
    queue = asyncio.Queue()
    asyncio.run(publish(queue, 5))
    asyncio.run(consume(queue))


if __name__ == '__main__':
    main()