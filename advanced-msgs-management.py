import asyncio
import logging
import random
import string
import uuid
from dataclasses import dataclass, field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


@dataclass
class PubSubMessage:
    instance_name: str
    message_id: int = field(repr=False)
    hostname: str = field(repr=False, init=False)
    restarted: bool = field(repr=False, default=False)
    saved: bool = field(repr=False, default=False)
    ack: bool = field(repr=False, default=False)

    def __post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'


async def save(msg):
    await asyncio.sleep(random.random())
    msg.saved = True
    logging.info(f'Saved {msg}')


async def cleanup(msg):
    msg.ack = True
    logging.info(f'Done. Acked {msg}')


async def handle_message(msg):
    save_msg = save(msg)
    restart = restart_host(msg)
    await asyncio.gather(save_msg, restart)
    asyncio.create_task(cleanup(msg))


async def restart_host(msg):
    await asyncio.sleep(random.random())
    msg.restarted = True
    logging.info(f'Restarted {msg.hostname}')


async def publish(queue):
    choices = string.ascii_lowercase + string.digits

    while True:
        msg_id = str(uuid.uuid4())
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(message_id=msg_id, instance_name=instance_name)
        # publish an item
        asyncio.create_task(queue.put(msg))
        logging.info(f'Published message {msg}')
        # simulate randomness of publishing messages
        await asyncio.sleep(random.random())


async def consume(queue):
    while True:
        # wait for an item from the publisher
        msg = await queue.get()

        # the publisher emits None to indicate that it is done
        if msg is None:
            break

        # process the msg
        logging.info(f'Consumed {msg}')
        asyncio.create_task(handle_message(msg))


def main():
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    try:
        loop.create_task(publish(queue))
        loop.create_task(consume(queue))
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info('Process interrupted')
    finally:
        loop.close()
        logging.info('Successfully shutdown the Mayhem service.')


if __name__ == '__main__':
    main()