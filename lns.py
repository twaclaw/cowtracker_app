import argparse
import asyncio
from base64 import b64decode
from contextlib import AsyncExitStack
from enum import Enum, auto
import logging
from pathlib import Path
import os
import ssl
from typing import Dict, List, Optional, Tuple
import ujson

from asyncio_mqtt import Client, MqttError
import yaml

logger = logging.getLogger('lns')


class MessageStatus(Enum):
    ERROR = auto()
    NOFIX = auto()
    INDOOR = auto()


class Message():
    def __init__(self, data: Dict[str, str]):
        self.__data = data
        self.serial = self.__data['hardware_serial']
        self.port = self.__data['port']
        self.__raw = self.__data['payload_raw']
        self.__base64 = b64decode(self.__raw)

    @property
    def payload(self):
        return f"0x{self.__base64.hex()}"

    @staticmethod
    def _signed(val: int, bits: int):
        return val - (1 << bits) if val >= (1 << (bits - 1)) else val

    @staticmethod
    def _rdlsbf(bytes_: List, offset: int, length: int):
        val = 0
        offset += length - 1
        while length:
            offset -= 1
            length -= 1
            val = (val << 8) + bytes_[offset]
        return val

    def decode(self):
        """
        Decodes message on port 136:

        Byte:   0       1        2      3 4 5 6   7 8 9 10
        Field:  Status  Battery  Temp   Lat       Lon
        """
        if self.port == 136:
            # Byte 0: status
            # Bit 4: GSP module error
            # Bit 3: no fix
            # Bit 2: indoor
            flags = self.__base64[0]
            self.status = set()
            if flags & (1 << 4):
                self.status.add(MessageStatus.ERROR)
            if flags & (1 << 3):
                self.status.add(MessageStatus.NOFIX)
            if flags & (1 << 2):
                self.status.add(MessageStatus.INDOOR)

            # Byte 1: battery
            # Bits[3:0] unsigned value ν, range 1 – 14, battery voltage in V = (25 + ν) ÷ 10.
            # Bits [7:4] unsigned value κ, range 0 – 15 remaining battery capacity in % = 100 × (κ ÷ 15).
            self.battery = ((self.__base64[1] & 0x0F) + 25) / 10.0
            self.batt_capacity = (self.__base64[1] >> 4) * 100 / 15

            # Byte 2: temperature
            # Bits [6:0] unsigned value τ, range 0 – 127; temperature in °C = τ - 32
            self.temperature = (self.__base64[2] & 0x7F) - 32

            # Byte 3-6: latitude (lsbf)
            # Bits[27:0] signed value φ, range - 90, 000, 000 – 90, 000, 000; WGS84 latitude in ° = φ ÷ 1, 000, 000.
            # Bits [31:28] RFU
            self.latitude = Message._signed(Message._rdlsbf(
                self.__base64, 3, 4) & 0x0FFFFFFF, 28) / 1000000

            # Byte 7-10: longitude+accuracy (lsbf)
            # Bits [28:0] signed value λ, range -179,999,999 – 180,000,000; WGS84 longitude in ° = λ ÷ 1,000,000.
            # Bits [31:29] unsigned value α, range 0-7; position accuracy estimate in m = 2 α+2 (max).
            # The value 7 represents an accuracy estimate of worse than 256m.
            lonacc = Message._rdlsbf(self.__base64, 7, 4)
            self.longitude = Message._signed(
                lonacc & 0x1FFFFFFF, 29) / 1000000
            self.accuracy = 2 * ((lonacc >> 29) & 0x7) + 2

            return {
                "Battery": self.battery,
                "latitude": self.latitude,
                "longitude": self.longitude,
                "accuracy": self.accuracy,
                "status": self.status
            }

    async def store(self):
        """
        Store message to db
        """
        pass


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
                logger.info(
                    f"Error: {error}. Reconnecting in {reconnect_interval} s")
            finally:
                await asyncio.sleep(reconnect_interval)

    async def downlink(self, topics: Tuple):
        for topic in topics:
            message = 'Message'
            print(f'[topic="{topic}"] Publishing message={message}')
            await self.__client.publish(topic, message, qos=1)

    async def log_messages(self, messages, template):
        async for msg in messages:
            try:
                payload = msg.payload
                uplink = Message(ujson.loads(payload.decode()))
                logger.info(f"Got uplink message {uplink.payload}")
            except Exception as ex:
                logger.exception("Invalid message received")

            print(uplink.decode())

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
