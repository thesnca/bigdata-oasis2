import asyncio
import time

from nubia import argument
from nubia import command
from prettytable import PrettyTable
from termcolor import cprint

from cli.methods.job import print_total_job
from cli.methods.job import retry_job
from cli.methods.job import rollback_job
from cli.methods.job import skip_task
from oasis.db.models import get_model_by_id
from oasis.db.models.job import JobModel


@command
class Job:
    """
        Job commands
    """

    @command
    @argument('job_id', description='Job uuid', positional=True)
    def watch(self, job_id: str):
        """
            Watch Job with uuid
        """

        job_status = 'Unknown'
        while job_status not in [JobModel.STATUS.Init,
                                 JobModel.STATUS.Done,
                                 JobModel.STATUS.Rolled,
                                 JobModel.STATUS.Error]:
            job_status = asyncio.run(print_total_job(job_id))
            if job_status not in [JobModel.STATUS.Done,
                                  JobModel.STATUS.Error]:
                time.sleep(2)
        return 0

    @command
    @argument('job_id', description='Job uuid', positional=True)
    def describe(self, job_id: str):
        """
            Describe Job with uuid
        """

        job = asyncio.get_event_loop().run_until_complete(get_model_by_id(JobModel, job_id))

        base_info = {
            'job_name': job.name,
            'status': job.status,
            'cluster_id': job.cluster_id,
        }

        table = PrettyTable(field_names=['task_id', 'name', 'status',
                                         'created_at', 'updated_at',
                                         'worker'])
        tasks = job.tasks
        for task in tasks:
            table.add_row([
                task.id,
                task.name,
                task.status,
                task.created_at,
                task.updated_at,
                task.worker,
            ])

        cprint(f'Job Id: {job_id}', 'green')
        cprint(base_info, 'cyan')
        cprint(table, 'white')

    @command
    @argument('job_id', description='Job uuid', positional=True)
    def retry(self, job_id: str):
        """
            Retry Job with uuid
        """

        cprint(f'Job Id: {job_id}', 'green')
        res = asyncio.get_event_loop().run_until_complete(retry_job(job_id))
        if res:
            cprint(f'Retry Job : {job_id} succeed', 'white')

    @command
    @argument('job_id', description='Job uuid', positional=True)
    def rollback(self, job_id: str):
        """
            Rollback Job with uuid, DANGEROUS Command, CAUTION!!!
        """

        cprint(f'Job Id: {job_id}', 'green')
        res = asyncio.get_event_loop().run_until_complete(rollback_job(job_id))
        if res:
            cprint(f'Rollback Job : {job_id} succeed', 'white')


@command
class Task:
    """
        Task commands
    """

    @command
    @argument('task_id', description='Task uuid', positional=True)
    def skip(self, task_id: str):
        """
            Skip specified task and continue job
        """
        job_id = asyncio.get_event_loop().run_until_complete(skip_task(task_id))
        cprint(f'Skip task: {task_id} done, job id: {job_id}', 'white')
