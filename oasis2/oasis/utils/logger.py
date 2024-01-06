from datetime import datetime
import gzip
import logging
from logging.handlers import BaseRotatingHandler
import os

import logs
from oasis.utils.config import config


class DailyRotatingFileHandler(BaseRotatingHandler):
    """
        Resolve bug of default rotating when using multi process.
    """

    def __init__(self, filename, encoding=None):
        BaseRotatingHandler.__init__(self, filename, 'a', encoding=encoding)
        self.computeRollover()

    def computeRollover(self):
        if os.path.exists(self.baseFilename):
            self.log_date = datetime.date(datetime.fromtimestamp(os.stat(self.baseFilename)[8]))
            return
        self.log_date = datetime.date(datetime.now())

    def shouldRollover(self, record):
        date_now = datetime.date(datetime.now())
        if date_now != self.log_date:
            return 1
        return 0

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None

        old_filename = f'{self.baseFilename}.{self.log_date}'
        if not os.path.exists(old_filename):
            self.rotate(self.baseFilename, old_filename)
        self.computeRollover()
        self.stream = self._open()
        self.gzip_log_files()

    def gzip_log_files(self):
        log_dir = os.path.realpath(os.path.dirname(self.baseFilename))
        # Gzip log files 14 days ago
        for root, _, files in os.walk(log_dir):
            if root != log_dir:
                continue
            for file_name in files:
                file_path = f'{log_dir}/{file_name}'
                if '.log' not in file_name:
                    continue
                if file_name.endswith('.log'):  # Current log file
                    continue
                if file_name.endswith('.gz'):  # Already gzipped
                    continue
                log_date = datetime.fromtimestamp(os.stat(file_path)[8])
                daysdiff = (datetime.now() - log_date).days
                if daysdiff < 14:
                    continue

                g = gzip.open(f'{file_path}.gz', 'wb')
                for line in open(file_path, 'rb'):
                    g.write(line)
                g.write(b' zipped log '.center(20, b'='))
                g.close()

                os.remove(file_path)


class Logger:
    def __getattr__(self, func):
        def _log(*args, **kwargs):
            if not args:
                return
            msgs = []
            if not args:
                return

            if len(args) == 1:
                msg = args[0]
            elif len(args) > 1:
                clazz = args[0]
                msg = args[1]

                msgs.append(clazz.__class__.__name__)
                account_id = getattr(clazz, 'account_id', None)
                if type(account_id) == str:
                    msgs.append(f'account: [{account_id}]')

                request_id = getattr(clazz, 'request_id', None)
                if type(request_id) == str:
                    msgs.append(f'request: [{request_id}]')

                job_id = getattr(clazz, 'job_id', None)
                if type(job_id) == str:
                    msgs.append(f'job: [{job_id}]')

                task_id = getattr(clazz, 'task_id', None)
                if type(task_id) == str:
                    msgs.append(f'task: [{task_id}]')

            msgs.append(msg)
            return getattr(self.logger, func)(', '.join(msgs), **kwargs)

        return _log

    def init_logger(self, service, name):
        log_name = config.get(service, 'log_name', fallback=service)
        path = config.get(service, 'log_path', fallback=None)
        log_format = config.get(service, 'log_format', fallback=None)
        test_env = config.getboolean('oasis', 'test_env', fallback=True)

        self.path = os.path.dirname(os.path.abspath(logs.__file__)) \
            if not path else path

        log_format = f'%(asctime)s %(name)s %(levelname)s: %(message)s' \
            if not log_format else log_format
        self.logger = logging.getLogger(log_name)
        self.logger.name = name
        self.logger.setLevel(logging.DEBUG if test_env else logging.INFO)
        logging.basicConfig(format=log_format)

        # normal_log = handlers.TimedRotatingFileHandler(f'{self.path}/{log_name}.log',
        #                                                when='MIDNIGHT',
        #                                                encoding='utf-8')
        normal_log = DailyRotatingFileHandler(f'{self.path}/{log_name}.log', encoding='utf-8')
        normal_log.setFormatter(logging.Formatter(log_format))
        normal_log.setLevel(logging.INFO)
        normal_log.setLevel(logging.DEBUG if test_env else logging.INFO)
        self.logger.addHandler(normal_log)

        # error_log = handlers.TimedRotatingFileHandler(f'{self.path}/error.log',
        #                                               when='MIDNIGHT',
        #                                               encoding='utf-8')
        error_log = DailyRotatingFileHandler(f'{self.path}/error.log', encoding='utf-8')
        error_log.setFormatter(logging.Formatter(log_format))
        error_log.setLevel(logging.ERROR)
        self.logger.addHandler(error_log)


logger = Logger()
