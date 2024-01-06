from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.utils.config import config
from oasis.utils.convert import dict_snake2camel
from oasis.utils.logger import logger
from oasis.utils.sdk import gringotts_client
from oasis.worker.tasks import BaseTask
from oasis.worker.tasks import check_rollback
from oasis.worker.tasks import check_task


class TaskGringottsLaunchCluster(BaseTask):
    @check_task
    async def run(self):
        token = self.args.get('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        service_multi_instance = []

        ig_id_dict = {ig.instance_group_type: ig.id for ig in cluster.instance_groups}
        instance_groups = self.args.get('instance_groups', [])
        for instance_group in instance_groups:
            ig_id = ig_id_dict.get(instance_group.get('instance_group_type', ''))
            instance_group.setdefault('id', ig_id)
            instance_group_type = instance_group.get('instance_group_type', '')
            multi_instance_count = instance_group.get('multi_instance_count', 1)
            service_multi_instance.append({
                'instance_group_type': instance_group_type,
                'multi_instance_count': multi_instance_count,
            })

        self.args.setdefault('service_multi_instance', service_multi_instance)

        data = dict_snake2camel(self.args)
        logger.info(self, f'Start gringotts launch cluster... data: {data}, token:{token}')
        _op_id = await gringotts_client.launch_cluster(data, token=token)
        await gringotts_client.wait_gg_op_active(_op_id, token=token)

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskGringottsGetEsFreeNodes(BaseTask):
    @check_task
    async def run(self):
        token = self.args.get('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        scale_in_instance_ids = self.args.get('scale_in_instance_ids', None)
        if not scale_in_instance_ids:
            raise Exception('Please specify scale_in_instance_ids')

        data = dict_snake2camel(self.args)
        logger.info(self, f'Start gringotts get es free nodes... data: {data}, token:{token}')
        res = await gringotts_client.get_es_free_nodes(data, token=token)
        no_indices_instance_ids = res.get('InstanceIds', [])

        is_proper_subset = set(scale_in_instance_ids) <= set(no_indices_instance_ids)
        logger.info(self,
                    f'scale_in_instance_ids {scale_in_instance_ids}, no_indices_instance_ids {no_indices_instance_ids}')
        if not is_proper_subset:
            raise Exception(
                f'Cluster can not scale in: scale in instance ids have indices , scale_in_instance_ids {scale_in_instance_ids}, no_indices_instance_ids {no_indices_instance_ids}')

        return res

    @check_rollback
    async def rollback(self):
        cluster_id = self.context.get('cluster_id', None)
        cluster = await get_model_by_id(ClusterModel, cluster_id)

        if cluster:
            await cluster.save({'status': ClusterModel.STATUS.ACTIVE})

        return True


class TaskGringottsScaleInCluster(BaseTask):
    @check_task
    async def run(self):
        token = self.args.get('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        instance_groups = self.args.get('instance_groups', None)
        if not instance_groups:
            raise Exception('Please specify instance_groups')

        # for i, ig in enumerate(instance_groups):
        #     for j, ins in enumerate(ig.get('instances', [])):
        #         instance_groups[i]['instances'][j] = {'id': instance_groups[i]['instances'][j]['instance_id']}

        data = dict_snake2camel(self.args)
        logger.info(self, f'Start gringotts scale in cluster... data: {data}, token:{token}')
        try:
            _op_id = await gringotts_client.scale_in_cluster(data, token=token)
            await gringotts_client.wait_gg_op_active(_op_id, token=token)
        except Exception as e:
            logger.error(self, f'Gringotts scale in cluster error {e}, skip this task. Cluster id {cluster_id}')

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskGringottsScaleOutCluster(BaseTask):
    @check_task
    async def run(self):
        token = self.args.get('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        new_instance_ids = self.args.get('new_instance_ids', None)

        ig_ins_dict = {}
        for ig in cluster.instance_groups:
            # instance.id for gringotts, instance.instance_id for oasis!
            ig_ins_dict.setdefault(ig.id, [{'id': ins.id} for ins in ig.instances
                                           if ins.instance_id in new_instance_ids])

        service_multi_instance = []

        instance_groups = self.args.get('instance_groups', [])
        for instance_group in instance_groups:
            ins_ids = ig_ins_dict.get(instance_group.get('id', ''))
            instance_group.update({'instances': ins_ids})
            multi_instance_count = instance_group.get('multi_instance_count', 1)
            instance_group_type = instance_group.get('instance_group_type', '')
            service_multi_instance.append({
                'instance_group_type': instance_group_type,
                'multi_instance_count': multi_instance_count,
            })

        self.args.setdefault('service_multi_instance', service_multi_instance)

        data = dict_snake2camel(self.args)
        logger.info(self, f'Start gringotts launch cluster... data: {data}, token:{token}')
        _op_id = await gringotts_client.scale_out_cluster(data, token=token)
        await gringotts_client.wait_gg_op_active(_op_id, token=token)

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskGringottsDeleteCluster(BaseTask):
    @check_task
    async def run(self):
        token = self.args.get('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        try:
            _op_id = await gringotts_client.delete_cluster(cluster_id, token=token)
            await gringotts_client.wait_gg_op_active(_op_id, token=token)
        except Exception as e:
            logger.error(self, f'Gringotts delete cluster error, skip this task. Cluster id {cluster_id}')

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskGringottsFreezeCluster(BaseTask):
    @check_task
    async def run(self):
        token = self.args.get('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        try:
            _op_id = await gringotts_client.freeze_cluster(cluster_id, token=token)
            await gringotts_client.wait_gg_op_active(_op_id, token=token)
        except Exception as e:
            logger.error(self, f'Gringotts freeze cluster error, skip this task. Cluster id {cluster_id}')

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskGringottsUnfreezeCluster(BaseTask):
    @check_task
    async def run(self):
        token = self.args.get('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        try:
            _op_id = await gringotts_client.un_freeze_cluster(cluster_id, token=token)
            await gringotts_client.wait_gg_op_active(_op_id, token=token)
        except Exception as e:
            logger.error(self, f'Gringotts unfreeze cluster error, skip this task. Cluster id {cluster_id}')

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskGringottsServiceControl(BaseTask):
    @check_task
    async def run(self):
        token = self.args.pop('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        _op_id = await gringotts_client.service_control(token=token, **self.args)
        await gringotts_client.wait_gg_op_active(_op_id, token=token)

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskGringottsEnableXpack(BaseTask):
    @check_task
    async def run(self):
        token = self.args.pop('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        _op_id = await gringotts_client.enable_xpack(token=token, **self.args)
        await gringotts_client.wait_gg_op_active(_op_id, token=token)

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskGringottsDisableXpack(BaseTask):
    @check_task
    async def run(self):
        token = self.args.pop('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        _op_id = await gringotts_client.disable_xpack(token=token, **self.args)
        await gringotts_client.wait_gg_op_active(_op_id, token=token)

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskGringottsUpgradeCluster(BaseTask):
    @check_task
    async def run(self):
        token = self.args.get('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        self.args.setdefault('is_upgrade_kec', False)
        self.args.setdefault('is_upgrade_ebs', False)
        self.args.setdefault('is_upgrade_local', False)

        data = dict_snake2camel(self.args)
        logger.info(self, f'Start gringotts launch cluster... data: {data}, token:{token}')
        _op_id = await gringotts_client.upgrade_instance_groups(data, token=token)
        await gringotts_client.wait_gg_op_active(_op_id, token=token)

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskGringottsSnapshotOn(BaseTask):
    @check_task
    async def run(self):
        token = self.args.get('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        ks3_endpoint = config.get('infra', 'ks3_endpoint')
        ks3_region = config.get('infra', 'ks3_region')
        ks3_endpoint = 'https://' + ks3_endpoint
        self.args.setdefault('ks3_endpoint', ks3_endpoint)
        self.args.setdefault('ks3_region', ks3_region)
        # self.args['endpoint'] =  config.get('infra', 'ks3_endpoint')
        # self.args['region'] = config.get('infra', 'ks3_region')

        logger.info(self, f'Start gringotts snapshot on... data: {self.args}, token:{token}')
        _op_id = await gringotts_client.snapshot_on(token=token, **self.args)
        await gringotts_client.wait_gg_op_active(_op_id, token=token)

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True
