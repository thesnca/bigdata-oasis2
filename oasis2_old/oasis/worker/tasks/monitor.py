from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.utils.sdk import eagles_client
from oasis.worker.tasks import BaseTask
from oasis.worker.tasks import check_rollback
from oasis.worker.tasks import check_task


class TaskAddClusterMonitor(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.pop('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        await eagles_client.service_add_cluster_monitor(cluster.id, cluster.name, cluster.cluster_type,
                                                        account_id=account_id)

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskAddInstanceMonitor(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.pop('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        new_instance_ids = self.args.get('new_instance_ids', [])

        for ig in cluster.instance_groups:
            for instance in ig.instances:
                if instance.instance_id in new_instance_ids:
                    await eagles_client.service_add_instances_monitor(
                        cluster_id,
                        cluster.name,
                        instance.id,
                        instance.instance_name,
                        instance.internal_ip,
                        cluster.cluster_type,
                        ig.instance_group_type,
                        account_id=account_id,
                    )

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskRemoveClusterMonitor(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.pop('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        await eagles_client.service_remove_cluster_monitor(cluster.id, cluster.cluster_type,
                                                           account_id=account_id)

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskRemoveInstanceMonitor(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.pop('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        scale_in_instance_ids = self.args.get('scale_in_instance_ids', None)

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        for ig in cluster.instance_groups:
            instance_ids = []
            for instance in ig.instances:
                if not scale_in_instance_ids or instance.instance_id in scale_in_instance_ids:
                    instance_ids.append(instance.id)
            if instance_ids:
                await eagles_client.service_remove_instance_monitor(
                    instance_ids,
                    cluster.cluster_type,
                    ig.instance_group_type,
                    account_id=account_id,
                )

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True
