import argparse

from oasis.utils.config import config
from oasis.utils.logger import logger
from oasis.worker.manager import Manager

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--manager_name', help='Specify Unique Manager Name, ',
                        type=str, required=True)

    args = parser.parse_args()
    name = args.manager_name

    manager_conf = {k: v for k, v in config['manager'].items()}
    manager = Manager(name, **manager_conf)
    logger.init_logger('manager', name)
    logger.info(manager, f'start manager {name}')
    manager.run()
