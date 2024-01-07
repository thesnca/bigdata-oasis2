import argparse

from oasis.utils.config import config
from oasis.utils.logger import logger
from oasis.worker import Worker

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--worker_name', help='Specify Unique Worker Name',
                        type=str, required=True)

    args = parser.parse_args()
    worker_name = args.worker_name

    worker_conf = {k: v for k, v in config['worker'].items()}
    worker = Worker(worker_name, **worker_conf)
    logger.init_logger('worker', worker_name)
    logger.info(worker, f'start worker {worker_name}')
    worker.run()
