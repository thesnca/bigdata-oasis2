import base64
from datetime import datetime

from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.instance_group import InstanceGroupModel
from oasis.utils.config import config
from oasis.utils.convert import dict_snake2camel
from oasis.utils.generator import gen_group_name
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger
from oasis.utils.sdk.charging.base import replace_service_type
from oasis.utils.sdk.iam import get_user_ak_sk_by_id
from oasis.worker.tasks import BaseTask
from oasis.worker.tasks import check_rollback
from oasis.worker.tasks import check_task


class TaskInitClusterCreate(BaseTask):
    @check_task
    async def run(self):
        values = self.args
        if await get_user_ak_sk_by_id(values.get('account_id'), values.get('product')) == None:
            account_id = values.get('account_id')
            raise Exception(f'User_ak not found, id {account_id}')
        user_ak, user_sk = await get_user_ak_sk_by_id(values.get('account_id'), values.get('product'))

        cluster = ClusterModel()
        instance_groups = values.pop('instance_groups', [])
        cluster.id = values.get('cluster_id', gen_uuid4())
        cluster.name = values.get('cluster_name', None)
        cluster.status = ClusterModel.STATUS.SPAWNING
        cluster.distribution_version = values.get('distribution', None)
        cluster.image_id = config.get('image_id', cluster.distribution_version)
        cluster.extra = values.get('extra', {})
        cluster.ksc_user_id = values.get('account_id', None)
        cluster.ksc_sub_user_id = values.get('sub_user_id', None)
        cluster.tenant_id = values.get('tenant_id', None)
        cluster.ks3_credential = base64.encodebytes(f'{user_ak}:{user_sk}'.encode('utf8')).decode('utf8')
        cluster.update(values)
        await cluster.save()

        product_details = values.get('product_details', None)

        for ig in instance_groups:
            instance_group = InstanceGroupModel()
            instance_group.name = gen_group_name(ig.get('instance_group_type', None))

            volume_count = ig.pop('volume_count', None)
            volume_size = ig.pop('volume_size', None)

            if ig.get('resource_type', None) == 'EPC':
                # for gg launch EPC
                # instance_type_code: CAL, instance_type: ES.epc.24C256G
                # ==> instance_type_code: CAL-ES.epc.24C256G
                instance_type = ig.pop('instance_type')
                instance_type_code = ig.pop('instance_type_code')
                instance_group.instance_type_code = f'{instance_type_code}-{instance_type}'
                instance_group.multi_instance_count = ig.pop('multi_instance_count')
            else:  # 直连盘
                service_type = replace_service_type(
                    values.get('cluster_type'), ig.get('resource_type'), ig.get('volume_type'))
                instance_type = ig.get('instance_type')
                product_info = product_details.get(service_type, {}).get(instance_type, {})
                # 直连盘 4*7400-SATA HDD，其他None
                local_disk = product_info.get('local_disk', None)
                if local_disk:
                    volume_count = int(local_disk.split('*')[0])
                    volume_size = int(local_disk.split('*')[1].split('-')[0])

            instance_group.cluster_id = cluster.id
            instance_group.availability_zone = cluster.availability_zone
            instance_group.image_id = cluster.image_id
            instance_group.count = 0
            instance_group.dest_count = int(ig.get('instance_count'))
            instance_group.vpc_domain_id = cluster.vpc_domain_id
            instance_group.vpc_subnet_id = ig.get('vpc_subnet_id', None) or cluster.vpc_subnet_id
            instance_group.resource_attr = dict_snake2camel(ig.get('resource_attributes', []))
            instance_group.status = InstanceGroupModel.STATUS.ACTIVE
            instance_group.volume_count = volume_count
            instance_group.volume_size = volume_size
            instance_group.update(ig)
            await instance_group.save()

        cluster_model = await get_model_by_id(ClusterModel, cluster.id)
        if not cluster_model:
            raise Exception(f'Init Cluster Failed')
        self.results = cluster_model.to_dict(full_info=True)
        return self.results

    @check_rollback
    async def rollback(self):
        cluster_id = self.context.get('cluster_id', None)
        cluster = await get_model_by_id(ClusterModel, cluster_id)
        await cluster.delete()

        return True


class TaskUpdateCluster(BaseTask):
    @check_task
    async def run(self):
        cluster_id = self.args.pop('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        activate = self.args.pop('activate', False)
        if activate:
            time_now = datetime.utcnow()
            self.args['activated_at'] = time_now
            logger.info(f'Record cluster activate time:{time_now}')

        terminate = self.args.pop('terminate', False)
        if terminate:
            time_now = datetime.utcnow()
            self.args['terminated_at'] = time_now
            logger.info(f'Record cluster terminate time:{time_now}')

        # Sometimes args send error params
        self.args.pop('cluster_id', None)
        self.args.pop('order_id', None)

        await cluster.save(self.args)
        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskInitClusterScale(BaseTask):
    @check_task
    async def run(self):
        order_id = self.args.pop('order_id', None)
        cluster_id = self.args.pop('cluster_id', None)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        extra_dict = dict(cluster.extra)
        extra_dict['scale_out_order_id'] = order_id
        await cluster.save({'extra': extra_dict})

        scale_out_instance_groups = self.args.get('scale_out_instance_groups', [])

        for scale_ig in scale_out_instance_groups:
            scale_ig_id = scale_ig.get('id', None)
            instance_group = await get_model_by_id(InstanceGroupModel, scale_ig_id)

            if instance_group:
                instance_group = await get_model_by_id(InstanceGroupModel, scale_ig_id)
                scale_out_count = scale_ig.get('instance_count', 0)
                await instance_group.save({'dest_count': instance_group.count + scale_out_count})
            else:
                instance_group = InstanceGroupModel()
                instance_group.id = scale_ig_id
                instance_group.name = gen_group_name(scale_ig.get('instance_group_type', None))
                if scale_ig.get('resource_type', None) == 'EPC':
                    # for gg launch EPC
                    # instance_type_code: CAL, instance_type: ES.epc.24C256G
                    # ==> instance_type_code: CAL-ES.epc.24C256G
                    instance_type = scale_ig.pop('instance_type')
                    instance_type_code = scale_ig.pop('instance_type_code')
                    instance_group.instance_type_code = f'{instance_type_code}-{instance_type}'
                    instance_group.multi_instance_count = scale_ig.pop('multi_instance_count')
                instance_group.cluster_id = cluster.id
                instance_group.availability_zone = cluster.availability_zone
                instance_group.image_id = cluster.image_id
                instance_group.count = 0
                instance_group.dest_count = int(scale_ig.get('instance_count'))
                instance_group.vpc_domain_id = cluster.vpc_domain_id
                instance_group.vpc_subnet_id = scale_ig.get('vpc_subnet_id', None) or cluster.vpc_subnet_id
                instance_group.resource_attr = dict_snake2camel(scale_ig.get('resource_attributes', []))
                instance_group.status = InstanceGroupModel.STATUS.ACTIVE
                instance_group.update(scale_ig)
                await instance_group.save()

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        cluster_id = self.context.get('cluster_id', None)
        scale_out_instance_groups = self.args.get('scale_out_instance_groups', [])
        cluster = await get_model_by_id(ClusterModel, cluster_id)
        extra_dict = dict(cluster.extra)
        extra_dict.pop('scale_out_order_id')
        await cluster.save({'extra': extra_dict,
                            'status': 'Active'})

        for scale_out_instance_group in scale_out_instance_groups:
            scale_out_instance_group_id = scale_out_instance_group.get('id', None)
            scale_out_count = scale_out_instance_group.get('instance_count', 0)

            ig = await get_model_by_id(InstanceGroupModel, scale_out_instance_group_id)
            if not ig:
                continue

            if ig.dest_count == scale_out_count:
                await ig.delete(hard=True)
            else:
                origin_count = ig.dest_count - scale_out_count
                await ig.save({
                    'dest_count': origin_count,
                    'count': origin_count,
                })

        return True
