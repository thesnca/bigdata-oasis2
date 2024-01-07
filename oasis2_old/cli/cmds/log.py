import os

from nubia import argument
from nubia import command
from termcolor import cprint


@command
class Log:
    """
        Job commands
    """

    @command
    @argument('keyword', description='Log keyword')
    @argument('date', description='Log date, 2021-03-09')
    @argument('job_id', description='The log of the job')
    @argument('task_id', description='The log of the task')
    def worker(self, keyword: str = '', date: str = None, job_id: str = None, task_id: str = None):
        """
            Watch worker log
        """

        log_cmd = f'grep "{keyword}" /data/projects/oasis2/logs/worker.log'

        if date:
            log_cmd += f'.{date}'

        if job_id:
            log_cmd += f' | grep {job_id}'

        if task_id:
            log_cmd += f' | grep {task_id}'

        # TODO judge gz file
        os.system(f'{log_cmd} | less ')

        return 0

    @command
    @argument('keyword', description='Log keyword')
    @argument('date', description='Log date, 2021-03-09')
    @argument('request_id', description='The log of the request')
    def kes(self, keyword: str = '', date: str = None, request_id: str = None):
        """
            Watch kes log
        """

        log_cmd = f'grep "{keyword}" /data/projects/oasis2/logs/kes.log'

        if date:
            log_cmd += f'.{date}'

        if request_id:
            log_cmd += f' | grep {request_id}'

        # TODO judge gz file
        os.system(f'{log_cmd} | less ')

        return 0

    @command
    @argument('keyword', description='Log keyword')
    @argument('date', description='Log date, 2021-03-09')
    @argument('request_id', description='The log of the request')
    def khbase(self, keyword: str = '', date: str = None, request_id: str = None):
        """
            Watch khbase log
        """

        log_cmd = f'grep "{keyword}" /data/projects/oasis2/logs/khbase.log'

        if date:
            log_cmd += f'.{date}'

        if request_id:
            log_cmd += f' | grep {request_id}'

        # TODO judge gz file
        os.system(f'{log_cmd} | less ')

        return 0

    @command
    @argument('keyword', description='Log keyword')
    @argument('date', description='Log date, 2021-03-09')
    def manager(self, keyword: str = '', date: str = None):
        """
            Watch manager log
        """

        log_cmd = f'grep "{keyword}" /data/projects/oasis2/logs/manager.log'

        if date:
            log_cmd += f'.{date}'

        # TODO judge gz file
        os.system(f'{log_cmd} | less ')

        return 0

    @command
    @argument('keyword', description='Log keyword')
    @argument('level', description='Log level', choices=['INFO', 'WARNING', 'ERROR'])
    def gringotts(self, keyword: str = '', level: str = 'INFO'):
        """
            Watch gringotts log
        """

        log_cmd = f'grep "{keyword}" /data/projects/gringotts-server-new/log/gringotts-server-new/gringotts-server.{level}'
        cprint(log_cmd)

        # TODO judge gz file
        os.system(f'{log_cmd} | less ')

        return 0
