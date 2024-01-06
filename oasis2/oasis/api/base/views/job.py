from oasis.api import BaseView
from oasis.api import error_response
from oasis.db.models import get_model_by_id
from oasis.db.models.job import JobModel
from oasis.db.models.task import TaskModel
from oasis.utils.logger import logger
from oasis.utils.sdk import feishu_client
from oasis.worker import TaskSendFeishu


class JobView(BaseView):
    async def describe_job(self, *args, **kwargs):
        job_id = kwargs.get('job_id', None)
        if not job_id:
            return error_response(f'Please specify job id, got {job_id}')
        logger.info(self, 'describe job')
        job_res = await get_model_by_id(JobModel, model_id=job_id)

        job = job_res.to_dict() if job_res else {}
        job.setdefault('tasks', [task.to_dict() for task in job_res.tasks])
        return {'job': job}

    async def retry_job(self, *args, **kwargs):
        job_id = kwargs.get('job_id', None)
        if not job_id:
            return error_response(f'Please specify job id, got {job_id}')
        job_res = await get_model_by_id(JobModel, model_id=job_id)
        if job_res.status not in [JobModel.STATUS.Error]:
            return error_response(f'Can not retry job {job_id}, status is {job_res.status}')
        logger.info(self, f'start retry job, previous status {job_res.status}')
        await job_res.save({'status': JobModel.STATUS.Doing})
        for task in job_res.tasks:
            if task.status == TaskModel.STATUS.Failed:
                await task.save({'status': TaskModel.STATUS.Init})
        await TaskSendFeishu(job_id=job_id, args={
            'cluster_id': job_res.cluster_id,
            'state': feishu_client.STATE.RETRY,
        }).run()
        return {'job': job_res.to_dict()}

    async def rollback_job(self, *args, **kwargs):
        job_id = kwargs.get('job_id', None)
        if not job_id:
            return error_response(f'Please specify job id, got {job_id}')
        job_res = await get_model_by_id(JobModel, model_id=job_id)
        if job_res.status != JobModel.STATUS.Error:
            return error_response(f'Can not retry job {job_id}, status is {job_res.status}')
        logger.info(self, f'start rolling back job, previous status {job_res.status}')
        await job_res.save({'status': JobModel.STATUS.Rolling})
        for task in job_res.tasks:
            if task.status == TaskModel.STATUS.RollFailed:
                await task.save({'status': TaskModel.STATUS.Failed})

        return {'job': job_res.to_dict()}
