from datetime import datetime
import os

from prettytable import PrettyTable
from termcolor import cprint

from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.job import JobModel
from oasis.db.models.task import TaskModel
from oasis.db.service import redis_client
from oasis.utils.generator import gen_cluster_lock


async def find_running_job_by_cluster(cluster_id):
    job_id = await redis_client.get(gen_cluster_lock(cluster_id))
    return job_id


async def find_error_job_by_cluster(cluster_id):
    query = model_query(JobModel)
    query = query.filter(JobModel.cluster_id == cluster_id). \
        filter(JobModel.status == JobModel.STATUS.Error)

    error_jobs = await query.query_all()

    return error_jobs


async def print_total_job(job_id):
    job = await get_model_by_id(JobModel, job_id)

    base_info = {
        'job_name': job.name,
        'status': job.status,
        'cluster_id': job.cluster_id,
    }
    start_time = job.created_at
    end_time = job.updated_at \
        if job.status in [JobModel.STATUS.Done,
                          JobModel.STATUS.Error] \
        else datetime.utcnow()

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
    os.system('clear')
    cprint(f'Job Id: {job_id}', 'green')
    cprint(base_info, 'cyan')
    cprint(f'Duration: {int((end_time - start_time).total_seconds())} s', 'red')
    cprint(table, 'white')

    return job.status


async def retry_job(job_id):
    job = await get_model_by_id(JobModel, job_id)
    if job.status not in [JobModel.STATUS.Error]:
        cprint(f'Retry failed, Job {job_id} is in status: {job.status}', 'red')
        return False

    await job.save({'status': JobModel.STATUS.Doing})
    for task in job.tasks:
        if task.status == TaskModel.STATUS.Failed:
            await task.save({'status': TaskModel.STATUS.Init})

    return True


async def rollback_job(job_id):
    job = await get_model_by_id(JobModel, job_id)
    if job.status not in [JobModel.STATUS.Error]:
        cprint(f'Retry failed, Job {job_id} is in status: {job.status}', 'red')
        return False

    await job.save({'status': JobModel.STATUS.Rolling})
    for task in job.tasks:
        if task.status in [TaskModel.STATUS.RollFailed]:
            await task.save({'status': TaskModel.STATUS.Failed})

    return True


async def skip_task(task_id):
    task = await get_model_by_id(TaskModel, task_id)
    job_id = task.job_id
    job = await get_model_by_id(JobModel, job_id)

    await task.save({'status': TaskModel.STATUS.Done})
    await job.save({'status': JobModel.STATUS.Doing})
    return job_id
