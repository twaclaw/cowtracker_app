import argparse
import asyncio
from contextlib import AsyncExitStack
import logging
from pathlib import Path
import os
import ssl
from typing import Dict, Optional, Tuple

from asyncio_mqtt import Client, MqttError
import yaml

logger = logging.getLogger('lns')


class TTNClient():
    def __init__(self, config: Dict[str, str], tls_context: ssl.SSLContext):
        self.__hostname = config['host']
        self.__port = config['port']
        self.__username = config['appid']
        self.__password = config['appkey']
        self.__client = Client(hostname=self.__hostname,
                               port=self.__port,
                               username=self.__username,
                               password=self.__password,
                               tls_context=tls_context)
        self.tasks = set()

    async def run(self, topics: Tuple):
        async with AsyncExitStack() as stack:
            stack.push_async_callback(self.cancel_tasks, self.tasks)
            await stack.enter_async_context(self.__client)

            for topic_filter in topics:
                # Log all messages that matches the filter
                manager = self.__client.filtered_messages(topic_filter)
                messages = await stack.enter_async_context(manager)
                template = f'[topic_filter="{topic_filter}"] {{}}'
                task = asyncio.create_task(
                    self.log_messages(messages, template))
                self.tasks.add(task)

            for topic in topics:
                await self.__client.subscribe(topic)

            await asyncio.gather(*self.tasks)

    async def run_retry(self, topics: Tuple, reconnect_interval: Optional[int] = 3):
        while True:
            try:
                await self.run(topics)
            except MqttError as error:
                logger.info(f"Error: {error}. Reconnecting in {reconnect_interval} s")
            finally:
                await asyncio.sleep(reconnect_interval)

    async def downlink(self, topics: Tuple):
        for topic in topics:
            message = 'Message'
            print(f'[topic="{topic}"] Publishing message={message}')
            await self.__client.publish(topic, message, qos=1)

    async def log_messages(self, messages, template):
        async for message in messages:
            # 🤔 Note that we assume that the message paylod is an
            # UTF8-encoded string (hence the `bytes.decode` call).
            print(template.format(message.payload.decode()))

    async def cancel_tasks(self, tasks):
        for task in tasks:
            if task.done():
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


async def hi_and_sleep(s: int):
    while True:
        print("hello world")
        await asyncio.sleep(s)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", type=str, default='./config.yaml',
        help="Configuration file. Default: %(default)s")

    try:
        args = parser.parse_args()
    except Exception as ex:
        print("Argument parsing failed!")
        raise ex

    try:
        config = yaml.safe_load(
            Path(os.path.realpath(args.config)).read_text())
    except Exception as ex:
        print("Invalid configuration file!")
        raise ex

    lns_config = config['lns']

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations('./tc.trust')
    topics = ("+/devices/+/up",)
    ttn_client = TTNClient(lns_config, context)
    await asyncio.gather(
        hi_and_sleep(2),
        ttn_client.run_retry(topics)
    )

if __name__ == '__main__':
    asyncio.run(main())
