import argparse
from importlib import import_module

from oasis.api.web import WebService
from oasis.utils.config import config
from oasis.utils.logger import logger

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--app_name', help='Specify App Name',
                        choices=['kes', 'khbase'],
                        type=str, required=True)
    parser.add_argument('-p', '--port', help='Specify Port, Default load from conf',
                        type=int, default=None, required=False)

    args = parser.parse_args()
    app_name = args.app_name
    port = args.port

    routes = getattr(import_module(f'oasis.api.{app_name}.route'), 'API_ROUTES')

    app_conf = {k: v for k, v in config[app_name].items()}
    app = WebService(app_name, routes, app_conf)
    logger.init_logger(app_name, app_name)
    logger.info(app, f'start web {app_name}')
    app.run(port=port)
