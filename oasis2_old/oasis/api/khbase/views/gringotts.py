from oasis.api import BaseView
from oasis.api import openapi
from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.job import JobModel
from oasis.db.models.task import TaskModel
from oasis.utils.convert import dict_snake2camel
from oasis.utils.convert import snake2camel
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger
from oasis.utils.redlock import lock_cluster
from oasis.utils.sdk import feishu_client
from oasis.utils.sdk import gringotts_client
from oasis.worker.planner import save_task_graph
from oasis.worker.tasks import set_job_context


class GringottsView(BaseView):
    """
        # Gringotts
        routes.append(('/ClusterServiceRestartCheck', GringottsView))
        routes.append(('/GetClusterServiceStatus', GringottsView))
        routes.append(('/ListClusterServiceStatus', GringottsView))
        routes.append(('/ServiceControl', GringottsView))
        routes.append(('/UpdateServiceConfiguration', GringottsView))
        routes.append(('/ListServiceConfigurations', GringottsView))
        routes.append(('/ListServiceConfigurationHistory', GringottsView))
        routes.append(('/DescribeClusterOperation', GringottsView))
        routes.append(('/ListClusterOperations', GringottsView))
        routes.append(('/CheckClusterIdle', GringottsView))

        # Gringotts for KHBASE
        routes.append(('/ComponentControl', GringottsView))
        routes.append(('/UpdateComponentConfiguration', GringottsView))
        routes.append(('/ListServicesIdle', GringottsView))
        routes.append(('/ListConnections', GringottsView))
    """

    def __getattr__(self, http_method):
        def __inner(*args, **kwargs):
            async def __pre_check():
                final_res = await getattr(BaseView, '__getattr__')(self, http_method)(*args, **kwargs)

                if http_method in ['get', 'post', 'put', 'delete']:
                    return final_res

                # To avoid exception
                kwargs.pop('token', None)

                cluster_id = kwargs.get('cluster_id', None)
                account_id = self.account_id
                if not cluster_id:
                    raise Exception(f'Please specify cluster id, got {cluster_id}')

                cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
                if not cluster:
                    raise Exception(f'Cluster not found, id {cluster_id}')

                gg_http_method = snake2camel(http_method)
                data = dict_snake2camel(kwargs)

                logger.info(self, f'Passthrough gringotts api {gg_http_method}, params {kwargs}')
                res = await getattr(
                    gringotts_client,
                    gg_http_method,
                )(token=self.user_token, **data)

                return res

            return __pre_check()

        return __inner

    async def service_control(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if cluster.status != ClusterModel.STATUS.ACTIVE:
            raise Exception(f'Cannot restart cluster, cluster status: {cluster.status}')

        job = JobModel(name='restart_cluster', status=JobModel.STATUS.Init, cluster_id=cluster_id)
        job_id = gen_uuid4()
        job.id = job_id

        lock_res = await lock_cluster(cluster_id, job_id)
        if not lock_res:
            raise Exception(f'Cluster has other tasks, please wait...')

        await job.save()

        self.context = {
            'product': self.product,
            'region': self.region,
            'availability_zone': kwargs.get('availability_zone'),
            'charge_type': kwargs.get('charge_type'),
            'distribution': kwargs.get('distribution'),
            'cluster_id': cluster_id,
            'cluster_type': 'KHBASE',
            'account_id': self.account_id,
            'tenant_id': self.tenant_id,
            'auth_token': self.user_token,
        }

        await set_job_context(job_id, self.context)

        task_send_feishu_init = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.INIT,
            }
        )

        # gringotts重启集群
        task_service_control = TaskModel(
            name='TaskGringottsServiceControl',
            args=kwargs,
        )

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_service_control: [task_send_feishu_done],
            task_send_feishu_done: [],
        }

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    @openapi
    async def restart_cluster(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if cluster.status != ClusterModel.STATUS.ACTIVE:
            raise Exception(f'Cannot restart cluster, cluster status: {cluster.status}')

        job = JobModel(name='restart_cluster', status=JobModel.STATUS.Init, cluster_id=cluster_id)
        job_id = gen_uuid4()
        job.id = job_id

        lock_res = await lock_cluster(cluster_id, job_id)
        if not lock_res:
            raise Exception(f'Cluster has other tasks, please wait...')

        await job.save()

        self.context = {
            'product': self.product,
            'region': self.region,
            'availability_zone': kwargs.get('availability_zone'),
            'charge_type': kwargs.get('charge_type'),
            'distribution': kwargs.get('distribution'),
            'cluster_id': cluster_id,
            'cluster_type': 'KHBASE',
            'account_id': self.account_id,
            'tenant_id': self.tenant_id,
            'auth_token': self.user_token,
        }

        await set_job_context(job_id, self.context)

        task_send_feishu_init = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.INIT,
            }
        )

        # gringotts重启集群
        task_service_control = TaskModel(
            name='TaskGringottsServiceControl',
            args=kwargs,
        )

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_service_control: [task_send_feishu_done],
            task_send_feishu_done: [],
        }

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }
