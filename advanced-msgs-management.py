import asyncio
import logging
import random
import string
import uuid
import signal
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
    saved: bool = field(repr=False, default=False)
    ack: bool = field(repr=False, default=False)

    def __post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'


async def save(msg):
    await asyncio.sleep(random.random())

    if random.randint(1, 5) == 3:
        raise SaveError(f'Could not save msg {msg}')

    msg.saved = True
    logging.info(f'Saved {msg}')


async def restart_host(msg):
    await asyncio.sleep(random.random())

    if random.randint(1, 5) == 3:
        raise RestartError(f'Could not restart {msg}')

    msg.restarted = True
    logging.info(f'Restarted {msg.hostname}')


async def cleanup(msg):
    msg.ack = True
    logging.info(f'Done. Acked {msg}')


async def handle_message(msg):
    results = await asyncio.gather(
        restart_host(msg), save(msg), return_exceptions=True)
    asyncio.create_task(handle_results(results, msg))
    await cleanup(msg)


async def handle_results(results, msg):
    for result in results:
        if isinstance(result, SaveError):
            logging.error(f'Saving msg {msg} failed.')

        elif isinstance(result, RestartError):
            logging.warning(f'Retrying restarting host: {msg.hostname}')

        elif isinstance(result, Exception):
            logging.error(f'Handling general error: {result}')


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

        if random.randint(1, 10) == 5:
            raise Exception(f'Could not restart {msg.hostname}')

        # process the msg
        logging.info(f'Consumed {msg}')
        asyncio.create_task(handle_message(msg))


def handle_exception(loop, context):
    # context['message'] will always be there; but context['exception'] may not
    msg = context.get('exception', context['message'])
    logging.error(f'Caught exception: {msg}')
    logging.info('Shutting down...')
    asyncio.create_task(shutdown(loop))


class SaveError(Exception):
    pass


class RestartError(Exception):
    pass


async def shutdown(loop, signal=None):
    if signal:
        logging.info(f'Received exit signal {signal.name}...')

    logging.info('Closing database connections')

    logging.info('Nacking outstanding messages')
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    logging.info(f'Cancelling {len(tasks)} outstanding tasks')
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    logging.info(f'Flushing metrics')
    loop.stop()


def main():
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT, signal.SIGQUIT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop)))
    loop.set_exception_handler(handle_exception)

    try:
        loop.create_task(publish(queue))
        loop.create_task(consume(queue))
        loop.run_forever()
    finally:
        loop.close()
        logging.info('Successfully shutdown the Mayhem service.')


if __name__ == '__main__':
    main()