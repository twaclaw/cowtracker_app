import asyncio
import argparse
from datetime import datetime
from cowtracker.db import conf_db_uri, connection
from pathlib import Path
import os
import random
import yaml

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", type=str, default='./config.yaml',
        help="Configuration file. Default: %(default)s")

    parser.add_argument(
        "--id", type=int,
        required=True,
        help="tracker id")

    parser.add_argument(
        "--batt-cap", type=int,
        default=100
        )


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

    pg_conf = config['postgres']

    conf_db_uri(pg_conf['host'],
                pg_conf['user'],
                pg_conf['port'],
                pg_conf['database']
                )

    async with await connection() as conn:
        accuracy = random.randint(2, 8)
        lat = round(random.uniform(6.71, 6.75), 6)
        lon = round(random.uniform(-72.76, -72.79), 6)
        pos = (lat, lon)
        batt_cap = args.batt_cap - random.randint(0, 20)

        sql = f'''
            INSERT INTO meas
            (
                deveui,
                t,
                pos,
                accuracy,
                batt_V,
                batt_cap,
                temp,
                rssi,
                snr
            )
            VALUES
            (
                {args.id},
                '{datetime.utcnow()}',
                '{pos}',
                {accuracy},
                3.6,
                {batt_cap},
                '20', 
                '0',
                '0'
            );
            '''
        await conn.execute(sql)


if __name__ == '__main__':
    asyncio.run(main())
