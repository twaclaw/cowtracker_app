from aiohttp import web
import argparse
import asyncio
import logging
import logging.config
from pathlib import Path
import os
from typing import Dict, Optional
import yaml

from cowtracker.db import conf_db_uri
from cowtracker.ttn import TTNClient
from cowtracker.cows import Cows, set_warn_levels
from cowtracker.email import Email

logger = logging.getLogger('server')

# global variables
routes = web.RouteTableDef()
app = web.Application()
cows_obj = Cows()
frontend_folder: Optional[str] = None


# ------------------------------------------------------------
# Application routes
# ------------------------------------------------------------

@routes.get('/')
async def ui_home(request):
    return web.FileResponse(
        os.path.join(frontend_folder, "index.html"))


@routes.get('/api/v1/names')
async def handler_get_cow_names(request):
    data = await cows_obj.get_names()
    return web.json_response(data)


@routes.get('/warnings')
async def handler_get_warnings(request):
    pass


@routes.get('/api/v1/meas/{name}')
async def handler_meas(request):
    cow = request.match_info['name']
    if cow != 'all':
        try:
            data = await cows_obj.get_last_coords(cow, 10)
            return web.json_response(data)
        except Exception:
            raise web.HTTPBadRequest()
    else:
        data = await cows_obj.get_current_pos_all_cows()
        return web.json_response(data)


async def web_start(config_nginx: Dict, dev_mode: bool):
    app.router.add_static('/static', path=frontend_folder)
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    if dev_mode:
        logger.info("Starting TCP site")
        site = web.TCPSite(runner)
    else:
        socket = config_nginx['socket']
        logger.info(f"Starting UnixSite on socket: {socket}")
        site = web.UnixSite(
            runner, socket, ssl_context=None)
    await site.start()


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

    # context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # context.load_verify_locations('./tc.trust')
    topics = (f"v3/{lns_config['appid']}/devices/+/up",)
    ttn_client = TTNClient(lns_config, None)

    pg_conf = config['postgres']

    conf_db_uri(pg_conf['host'],
                pg_conf['user'],
                pg_conf['port'],
                pg_conf['database']
                )

    try:
        logging.config.dictConfig(config['logger'])
    except Exception as ex:
        print("Invalid logging configuration")
        raise ex

    set_warn_levels(config['warnings'])

    email_conf = config['email']
    email_sender = Email(email_conf)
    await cows_obj.aioinit(email_sender)

    global frontend_folder
    frontend_folder = config['frontend_folder']

    dev_mode: bool = True
    try:
        config_nginx = config['nginx']
        dev_mode = True
    except Exception:
        dev_mode = True
        logger.info("No nginx configuration found, assuming development mode")

    if 'dev_mode' in config:
        dev_mode = config['dev_mode']

    await asyncio.gather(
        web_start(config_nginx, dev_mode),
        ttn_client.run_retry(topics)
    )

if __name__ == '__main__':
    asyncio.run(main())
