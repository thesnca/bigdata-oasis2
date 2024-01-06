from conf.charge_conf import PRODUCT_USE_MAP
from conf.infra_conf import DEFAULT_LINK, TAG_REP
from oasis.api import BaseView
from oasis.api.base.methods import slb_check_listener
from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.job import JobModel
from oasis.db.models.task import TaskModel
from oasis.db.models.eip import EIPModel
from oasis.utils import sdk
from oasis.utils.generator import get_url_suffix
from oasis.utils.sdk import feishu_client
from oasis.utils.sdk import ks3_client
from oasis.utils.sdk import price_client
from oasis.utils.sdk import product_client
from oasis.utils.sdk.iam import get_user_ak_sk_by_id
from oasis.utils.sdk.platform.tag import TagResource
from oasis.worker.planner import save_task_graph
from oasis.worker.tasks import set_job_context
from oasis.utils.logger import logger


class PlatformView(BaseView):

    async def bind_tags(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        kwargs.setdefault('exec_mode', TagResource.EXEC.ALL)
        # kwargs.setdefault('is_clear', True)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        job = JobModel(name='replace_resources_tags', status=JobModel.STATUS.Init, cluster_id=cluster_id)
        await job.save()
        job_id = job.id

        self.context = {
            'product': self.product,
            'region': self.region,
            'availability_zone': kwargs.get('availability_zone'),
            'charge_type': kwargs.get('charge_type'),
            'distribution': kwargs.get('distribution'),
            'cluster_id': cluster_id,
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

        task_replace_resources_tags = TaskModel(name='TaskReplaceResourcesTags',
                                                args=kwargs)

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_replace_resources_tags: [task_send_feishu_done],
            task_send_feishu_done: [],
        }

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    async def check_tags(self, *args, **kwargs):

        result = {}

        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if not cluster.tags:
            return result

        tag_ids = [int(tag.get('tag_id', 0)) for tag in cluster.tags]

        # 存在kec+epc 混合模式。
        kec_replace_tags = []
        epc_replace_tags = []
        ebs_replace_tags = []
        eip_replace_tags = []
        slb_replace_tags = []
        # 业务唯一
        service_replace_tags = []

        # region KEC/EPC/EBS//KMR/KES/KHBASE
        for instance_group in cluster.instance_groups:
            tag_ec_key = instance_group.resource_type.upper()
            for instance in instance_group.instances:
                if tag_ec_key == 'KEC':
                    kec_replace_tags.append(instance.instance_id)
                elif tag_ec_key == 'EPC':
                    epc_replace_tags.append(instance.instance_id)

                if instance.volumes:
                    ebs_replace_tags += instance.volumes

                if instance.service_instance_id:
                    service_replace_tags.append(instance.service_instance_id)
        # endregion

        # region EIP/SLB
        eip_info_query = model_query(EIPModel).filter(
            EIPModel.cluster_id == cluster_id,
            EIPModel.status == EIPModel.STATUS.BINDED)
        eip_infos = await eip_info_query.query_all()

        for eip_info in eip_infos:
            if eip_info.allocate_address_id:
                eip_replace_tags.append(eip_info.allocate_address_id)
            if eip_info.load_balancer_id:
                slb_replace_tags.append(eip_info.load_balancer_id)
        # endregion

        tag_client = getattr(sdk, f'tag_client_{cluster.cluster_type.lower()}')

        # 满足需求，分tag
        for tagid in tag_ids:
            result[str(tagid)] = {}

        # 分业务线
        temp_check_dict = {}
        temp_check_dict[cluster.cluster_type.upper()] = service_replace_tags
        temp_check_dict['KEC'] = kec_replace_tags
        temp_check_dict['EPC'] = epc_replace_tags
        temp_check_dict['EBS'] = ebs_replace_tags
        temp_check_dict['EIP'] = eip_replace_tags
        temp_check_dict['SLB'] = slb_replace_tags

        logger.info(self, f'Check Tags, temp_check_dict:{temp_check_dict}')

        # 分别判断
        for service_key, replace_tags in temp_check_dict.items():
            if replace_tags:
                try:
                    real_tags = await tag_client.list_tags_id_by_resource_ids(TAG_REP.get(service_key, ''), replace_tags, account_id=account_id)
                    for tagid in tag_ids:
                        for instance_id in replace_tags:
                            if tagid not in real_tags.get(instance_id, []):
                                if service_key in result[str(tagid)]:
                                    if instance_id not in result[str(tagid)][service_key]:
                                        result[str(tagid)][service_key].append(instance_id)
                                else:
                                    result[str(tagid)][service_key] = [instance_id]

                except Exception as e:
                    logger.warn(self, f'Check Tags, Type:{service_key} ,Error:{e}')

        return result

    # 纯透传，提供给前端。
    async def create_tag(self, *args, **kwargs):
        result = {}

        account_id = self.account_id

        key = kwargs.get('key', None)
        value = kwargs.get('value', None)

        if not key:
            raise Exception(f'Please specify tag key, got {key}')

        if not value:
            raise Exception(f'Please specify tag value, got {value}')

        tag_client = getattr(sdk, f'tag_client_{self.product}')
        result = await tag_client.create_tag(key, value, account_id=account_id)

        return result

    async def list_tag_keys(self, *args, **kwargs):
        result = {}

        account_id = self.account_id

        tag_client = getattr(sdk, f'tag_client_{self.product}')
        result = await tag_client.list_tag_keys(account_id=account_id)

        return result

    async def list_tag_values(self, *args, **kwargs):
        result = {}

        account_id = self.account_id

        tag_keys = kwargs.get('tag_keys', None)

        if not tag_keys:
            raise Exception(f'Please specify tag keys, got {tag_keys}')

        tag_client = getattr(sdk, f'tag_client_{self.product}')
        result = await tag_client.list_tag_values(tag_keys, account_id=account_id)

        return result
