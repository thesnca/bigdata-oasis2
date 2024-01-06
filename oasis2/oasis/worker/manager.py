import asyncio
import json
import signal

from oasis.db.models import model_query
from oasis.db.models.job import JobModel
from oasis.db.models.task import TaskModel
from oasis.db.service import competition_mq
from oasis.utils.logger import logger
from oasis.utils.redlock import lock_cluster
from oasis.utils.redlock import redlock
from oasis.utils.redlock import unlock_cluster
from oasis.utils.sdk import feishu_client
from oasis.worker import TaskSendFeishu
from oasis.worker.planner import get_next_rollbacks_from_db
from oasis.worker.planner import get_next_tasks_from_db
from oasis.worker.tasks import del_job_context
from oasis.worker.tasks import get_job_context
from oasis.worker.tasks import set_job_context


class Manager:
    def __init__(self, name, **conf):
        self.name = name
        self.stream = conf.get('stream', 'default_stream')
        self.interval = int(conf.get('interval', 10))
        self.enable = True
        self.lock_key = conf.get('manager_lock', '/oasis/manager_lock/kes')
        self.lock_iden = None
        self.lock_timeout = self.interval * 3

    async def _start_manager(self):
        logger.info(self, f'Start manager, name: {self.name}, stream: {self.stream}, interval: {self.interval}')
        while self.enable:
            if self.lock_iden:
                ttl = await redlock.get_lock_ttl(self.lock_key, self.lock_iden)
                if not ttl:
                    logger.info(self, f'Acquire lock ttl failed! Lock key {self.lock_key}, '
                                      f'identifier {self.lock_iden}, ttl {ttl}')
                    self.lock_iden = None
                    continue
                await redlock.acquire_lock(self.lock_key, self.lock_timeout, self.lock_iden)
                await self._check_jobs()
            elif not await redlock.is_locked(self.lock_key):
                self.lock_iden = await redlock.acquire_lock(self.lock_key, self.lock_timeout)
                await self._check_jobs()
            else:
                sleep_time = self.lock_timeout + self.interval
                logger.info(self, f'Stand by {sleep_time} s...')
                await asyncio.sleep(sleep_time)

            await asyncio.sleep(self.interval)

    async def _check_jobs(self):
        logger.info(self, f'Start checking jobs...')
        jobs_query = model_query(JobModel)
        undone_jobs = await jobs_query.where(JobModel.__table__.c.status
                                             .in_([JobModel.STATUS.Doing, JobModel.STATUS.Rolling])
                                             ).query_all()
        for undone_job in undone_jobs:
            logger.debug(self, f'==wuhsh==undone_jobs==>{undone_job.to_dict()}')

            job_id = undone_job.id
            cluster_id = undone_job.cluster_id
            if undone_job.status == JobModel.STATUS.Doing:
                next_exec_tasks = await get_next_tasks_from_db(job_id)
                if next_exec_tasks == 'All Done':
                    logger.info(self, f'job finished!')
                    await undone_job.save({'status': JobModel.STATUS.Done})
                    pre_job_context = await get_job_context(job_id)
                    await del_job_context(job_id)
                    await unlock_cluster(cluster_id, job_id)

                    sub_jobs_query = model_query(JobModel)
                    sub_jobs = await sub_jobs_query \
                        .filter(JobModel.status == JobModel.STATUS.Init) \
                        .filter(JobModel.parent_job == job_id) \
                        .query_all()

                    for sub_job in sub_jobs:
                        await set_job_context(sub_job.id, pre_job_context)
                        await sub_job.save({'status': JobModel.STATUS.Doing})

                    continue
                if next_exec_tasks:
                    logger.info(self, f'Job {job_id}, send next exec tasks, {next_exec_tasks}')
                    await lock_cluster(cluster_id, job_id)
                    await self._send_tasks(next_exec_tasks, 'exec')

            elif undone_job.status == JobModel.STATUS.Rolling:
                query = model_query(TaskModel)
                doing_tasks = await query.where(TaskModel.job_id == job_id).where(
                    TaskModel.status == TaskModel.STATUS.Doing).query_all()
                if doing_tasks:
                    logger.info(self, f'Rollback job {job_id} waiting executing tasks to be done...')
                    continue

                next_roll_tasks = await get_next_rollbacks_from_db(job_id)
                if next_roll_tasks == 'All Rolled':
                    logger.info(self, f'job rolled back!')
                    await undone_job.save({'status': JobModel.STATUS.Rolled})
                    await del_job_context(job_id)
                    await unlock_cluster(cluster_id, job_id)
                    await TaskSendFeishu(job_id=job_id, args={
                        'state': feishu_client.STATE.ROLLED,
                        'cluster_id': cluster_id,
                    }).run()
                    continue
                if next_roll_tasks:
                    logger.info(self, f'Job {job_id}, send next roll tasks, {next_roll_tasks}')
                    if await lock_cluster(cluster_id, job_id):
                        await self._send_tasks(next_roll_tasks, 'rollback')

    async def _shutdown(self, sig, loop):
        logger.info(self, f'Start shutdown manager {self.name}, signal {sig.name}')
        self.enable = False
        await redlock.release_lock(self.lock_key, self.lock_iden)

    async def _send_tasks(self, tasks, task_type):
        if not tasks:
            return
        for task_id in tasks:
            task_model = TaskModel()
            task_model.id = task_id
            new_status = TaskModel.STATUS.Doing if task_type == 'exec' else TaskModel.STATUS.Rolling
            await task_model.save({'status': new_status})

            send_dict = json.dumps({'task_id': task_id, 'task_type': task_type})
            await competition_mq.xadd(stream_id=self.stream, fields={b'task_msg': send_dict.encode('utf8')})

    def run(self):
        loop = asyncio.get_event_loop()
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for s in signals:
            loop.add_signal_handler(
                s, lambda sd=s: asyncio.create_task(self._shutdown(s, loop)))

        asyncio.get_event_loop().run_until_complete(self._start_manager())

        for s in signals:
            loop.remove_signal_handler(s)
        logger.info(self, f'{self.name} exit success.')
        loop.close()
