import asyncio
from asyncio.exceptions import CancelledError
from datetime import datetime
import json
import signal
import traceback

from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.job import JobModel
from oasis.db.models.task import TaskModel
from oasis.db.service import competition_mq
from oasis.utils.config import config
from oasis.utils.logger import logger
from oasis.utils.redlock import redlock
from oasis.utils.redlock import unlock_cluster
from oasis.utils.sdk import feishu_client
from oasis.worker.tasks import RegisteredTasks
from oasis.worker.tasks import fill_task_args
from oasis.worker.tasks.notify import TaskSendFeishu


class Worker:
    def __init__(self, name, **conf):
        self.name = name
        self.stream = config.get('worker', 'stream', fallback='default_stream')
        self.group = config.get('worker', 'group', fallback='group')
        self.consumer_list = []
        self.timeout = config.getint('worker', 'timeout', fallback=3600)
        self.concurrent = config.getint('worker', 'concurrent', fallback=5)
        self.lock_key = f'/oasis/lock/worker/{self.name}'
        self.lock_timeout = config.getint('worker', 'lock_timeout', fallback=120)
        self.lock_iden = None
        self.enable = False

    async def _doing_task(self, task_msg):
        task_dict = json.loads(task_msg.get('task_msg'))
        task_id = task_dict.get('task_id')
        task_type = task_dict.get('task_type')
        task_model = await get_model_by_id(TaskModel, task_id)
        job_id = task_model.job_id
        await task_model.save({'worker': self.name})

        # Refresh job model status
        job_model = await get_model_by_id(JobModel, model_id=job_id)
        cluster_id = job_model.cluster_id
        new_status = None

        task_clazz = RegisteredTasks.get(task_model.name)
        rollback_on_fail = task_model.rollback_on_fail

        if task_type == 'exec' and task_model.status == TaskModel.STATUS.Doing:
            try:
                if not task_clazz:
                    raise Exception(
                        f'Could not find task {task_model.name}, job id {job_id}, task id {task_id}')

                results = await task_clazz(task_id=task_id, job_id=job_id,
                                           args=task_model.args).run()
            except Exception as e:
                cluster_id = job_model.cluster_id

                if cluster_id and job_model.name in ['launch_cluster',
                                                     'scale_out',
                                                     'scale_in',
                                                     'freeze_cluster',
                                                     'unfreeze_cluster',
                                                     'delete_cluster', ]:
                    cluster = await get_model_by_id(ClusterModel, cluster_id)
                    if cluster:
                        await cluster.save({'status': ClusterModel.STATUS.ERROR})
                await task_model.save({'info': f'{e}, {traceback.format_exc()}',
                                       'status': TaskModel.STATUS.Failed})
                try:
                    await TaskSendFeishu(job_id=job_id, args={
                        'state': feishu_client.STATE.ERROR,
                        'cluster_id': cluster_id,
                    }).run()
                except:
                    pass

                if rollback_on_fail:
                    # Other status no need to rollback
                    if job_model.status in (JobModel.STATUS.Doing, JobModel.STATUS.Error):
                        new_status = JobModel.STATUS.Rolling
                        logger.error(self,
                                     f'Job [{job_id}], Task [{task_id}],'
                                     f'Task {task_model.name} Failed, Error: {e},'
                                     f'Rollback start. \n'
                                     f'{traceback.format_exc()}')
                else:
                    # Do not change rolling job status
                    if job_model.status == JobModel.STATUS.Doing:
                        new_status = JobModel.STATUS.Error
                        logger.error(self,
                                     f'Job [{job_id}], Task [{task_id}],'
                                     f'Task {task_model.name} Failed, Error: {e},'
                                     f'Set job status to error. \n'
                                     f'{traceback.format_exc()}')
                        await unlock_cluster(cluster_id, job_id)

                if new_status:
                    job_model = JobModel()
                    job_model.id = job_id
                    await job_model.save({'status': new_status})

                return

            # write required results into next tasks args
            next_task_ids = task_model.next_tasks
            next_task_query = model_query(TaskModel).where(TaskModel.id.in_(next_task_ids))
            next_task_models = await next_task_query.query_all()
            for next_task in next_task_models:
                if next_task.args:
                    await next_task.save({'args': fill_task_args(next_task.args, results)})

            task_model.status = TaskModel.STATUS.Done
            await task_model.save({
                'status': TaskModel.STATUS.Done,
                'results': results,
            })

        elif task_type == 'rollback' and task_model.status == TaskModel.STATUS.Rolling:
            try:
                if not task_clazz:
                    raise Exception(f'Could not find rolling back task {task_model.name}, '
                                    f'job id {job_id}, task id {task_id}')

                results = await task_clazz(task_id=task_id, job_id=job_id,
                                           args=task_model.args, results=task_model.results).rollback()
            except Exception as e:
                logger.info(self, f'Task Rollback Failed, Error: {e}, '
                                  f'result {task_model.info}.\n'
                                  f'{traceback.format_exc()}')
                await task_model.save({
                    'status': TaskModel.STATUS.RollFailed,
                    'info': str(e),
                })
                new_status = JobModel.STATUS.Error
                if new_status:
                    job_model = JobModel()
                    job_model.id = job_id
                    await job_model.save({'status': new_status})
                    await unlock_cluster(cluster_id, job_id)

                return

            await task_model.save({
                'status': TaskModel.STATUS.Rolled,
                'results': results,
            })

        await asyncio.sleep(5)

    def _standby(self):
        async def _run(seq):
            consumer = f'{self.name}_{seq}_{datetime.utcnow()}'
            self.consumer_list.append(consumer)
            logger.info(self, f'Started , stream: {self.stream} , consumer: {consumer}')
            try:
                await competition_mq.xgroup_create(stream_id=self.stream,
                                                   group=self.group,
                                                   last_id='0',
                                                   mk_stream=True)
            except:
                pass

            while self.enable:
                async for new_task in competition_mq.xread_group(stream_id=self.stream,
                                                                 group=self.group,
                                                                 consumer=consumer,
                                                                 count=1,
                                                                 timeout=self.timeout):
                    if not new_task:
                        continue

                    try:
                        # Run task
                        stream_id, msg_id, task_msg = new_task
                        logger.info(self,
                                    f'Receive task from stream: [{stream_id}], msg_id: [{msg_id}], task_msg: {task_msg}')
                        await asyncio.gather(self._doing_task(task_msg))

                    except Exception as e:
                        logger.info(self, f'Run into exception, Error: {e}.\n'
                                          f'{traceback.format_exc()}')

        return [_run(seq) for seq in range(self.concurrent)]

    async def _watch_dog(self):
        while True:
            if self.lock_iden:
                ttl = await redlock.get_lock_ttl(self.lock_key, self.lock_iden)
                if not ttl:
                    logger.info(self, f'Shutdown watchdog...')
                    break

                # acquire lock anyway, let shutdown method to release lock
                await redlock.acquire_lock(self.lock_key, self.lock_timeout, self.lock_iden)
                if not self.enable:
                    break
                logger.debug(self, f'{self.name} is working hard...')
            elif not await redlock.is_locked(self.lock_key):
                logger.info(self, f'{self.name} starts working...')
                self.lock_iden = await redlock.acquire_lock(self.lock_key, self.lock_timeout)
                self.enable = True
                await asyncio.gather(*self._standby())
            else:
                sleep_time = self.lock_timeout
                logger.debug(self, f'Worker {self.name} already in use, '
                                   f'stand by to wait...')
                await asyncio.sleep(sleep_time)

            await asyncio.sleep(self.lock_timeout / 2)

    async def _shutdown(self, sig, loop):
        logger.info(self, f'Start shutdown worker {self.name}, signal {sig.name}')
        self.enable = False
        xgroup_del_tasks = [competition_mq.xgroup_del_consumer(self.stream, self.group, consumer_id)
                            for consumer_id in self.consumer_list]
        await asyncio.gather(*xgroup_del_tasks)
        remain_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in remain_tasks:
            logger.debug(self, f'Remain task {task.get_coro().__name__}')

        logger.info(self, f'{self.name} remain tasks cancelling...')
        remain_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

        await asyncio.gather(*remain_tasks, return_exceptions=True)
        if self.lock_iden:
            await asyncio.gather(redlock.release_lock(self.lock_key, self.lock_iden))
            logger.debug(self, f'Unlock worker lock {self.lock_key}')
        loop.stop()

    def run(self):
        loop = asyncio.get_event_loop()
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for s in signals:
            loop.add_signal_handler(
                s, lambda sd=s: asyncio.create_task(self._shutdown(s, loop)))

        try:
            loop.run_until_complete(self._watch_dog())
        except CancelledError:
            pass
        finally:
            for s in signals:
                loop.remove_signal_handler(s)
            logger.info(self, f'{self.name} exit success.')
            loop.close()
