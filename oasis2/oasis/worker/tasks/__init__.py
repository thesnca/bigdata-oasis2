from abc import ABC
from abc import abstractmethod
import json

from oasis.db.service import redis_client
from oasis.utils.logger import logger


async def get_job_context(job_id):
    res_dict = await redis_client.hgetall(f'/context/job/{job_id}/')
    if type(res_dict) == dict:
        res_dict = {k: json.loads(v) for k, v in res_dict.items()}
    return res_dict


async def set_job_context(job_id, values):
    values = {k: json.dumps(v) for k, v in values.items()}
    await redis_client.hmset_dict(f'/context/job/{job_id}/', **values)


async def del_job_context(job_id):
    await redis_client.expire(f'/context/job/{job_id}/', 0)


def fill_task_args(args, res_dict):
    if not args:
        args = {}
    if not res_dict or not type(res_dict) == dict:
        return args
    needed_args = [arg.replace('$$$', '') for arg in args
                   if arg.startswith('$$$')]
    for k, v in res_dict.items():
        args.setdefault(k, v)
        if k in needed_args:
            args.pop(f'$$${k}$$$')
    return args


def check_task(func):
    async def _inner(self, *args, **kwargs):
        self.context = await get_job_context(self.job_id)
        logger.debug(self, f'Init task {self.__class__.__name__}, got job context {self.context}')

        # Fill needed args from context
        self.args = fill_task_args(self.args, self.context)

        logger.info(self, f'Start task {self.__class__.__name__}, args {self.args}')
        res = await func(self, *args, **kwargs)
        logger.info(self, f'Finish task {self.__class__.__name__}, results {res}')

        if self.context:
            await set_job_context(self.job_id, self.context)
        self.result = res
        return self.result

    return _inner


def check_rollback(func):
    async def _inner(self, *args, **kwargs):
        logger.info(self, f'Start rolling back task {self.__class__}, args {self.args}')

        self.context = await get_job_context(self.job_id)

        res = await func(self, *args, **kwargs)

        logger.info(self, f'Finish rolling back task {self.__class__}, results {res}')
        return res

    return _inner


async def get_rolled_instance(instance_id):
    res = await redis_client.hget('/useless/rolled_instances/', instance_id)
    return res is not None


async def set_rolled_instance(instance_id, cluster_id):
    return await redis_client.hset('/useless/rolled_instances/', instance_id, cluster_id)


class BaseTask(ABC):
    def __init__(self, task_id=None, job_id=None, args=None, results=None):
        self.task_id = task_id
        self.job_id = job_id
        self.args = args if args else {}
        self.context = {}
        self.results = results if results else {}

    @abstractmethod
    @check_task
    async def run(self):
        pass

    @abstractmethod
    @check_rollback
    async def rollback(self):
        pass


def __get_registered_tasks():
    from importlib import import_module
    from pkgutil import walk_packages
    from inspect import getmembers, isclass
    task_dict = {}
    for _, modname, _ in walk_packages(path=__path__):
        module = import_module(f'{__name__}.{modname}')
        for task_name, task_clazz in getmembers(module, isclass):
            if issubclass(task_clazz, BaseTask) and task_name.startswith('Task'):
                task_dict.setdefault(task_name, task_clazz)
    return task_dict


RegisteredTasks = __get_registered_tasks()
