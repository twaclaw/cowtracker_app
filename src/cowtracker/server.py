import argparse
import asyncio
import logging
from pathlib import Path
import os
import ssl
import yaml

from cowtracker.ttn import TTNClient

logger = logging.getLogger('server')

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
