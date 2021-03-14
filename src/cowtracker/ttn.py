from datetime import datetime
import asyncio
from asyncio_mqtt import Client, MqttError
from contextlib import AsyncExitStack
import logging
import ssl
from typing import Dict, Optional, Tuple
import ujson

from cowtracker.messages import Message


logger = logging.getLogger('ttn')


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
                               tls_context=None)
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
                logger.info(
                    f"Error: {error}. Reconnecting in {reconnect_interval} s")
            finally:
                await asyncio.sleep(reconnect_interval)

    async def downlink(self, topics: Tuple):
        for topic in topics:
            message = 'Message'
            logger.info(f'[topic="{topic}"] Publishing message={message}')
            await self.__client.publish(topic, message, qos=1)

    async def log_messages(self, messages, template):
        async for msg in messages:
            try:
                payload = msg.payload
                uplink = Message(ujson.loads(payload.decode()))
                logger.info(f"Got uplink message {uplink}")
                try:
                    message = uplink.decode()
                    try:
                        if message is not None:
                            if len(message['status']) > 0:
                                logger.info(
                                    f"Not storing message with status: {message['status']} to db.")
                            else:
                                await uplink.store()
                    except Exception:
                        logger.exception(
                            f"Error storing message to db: {message}")
                except Exception:
                    logger.exception(f"Error decoding message {uplink}")
            except Exception:
                logger.exception(f"Invalid message received: {msg.payload}")

    async def cancel_tasks(self, tasks):
        for task in tasks:
            if task.done():
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
