from conf.charge_conf import PRODUCT_USE_MAP
from conf.infra_conf import DEFAULT_LINK
from oasis.api import BaseView
from oasis.api.base.methods import slb_check_listener
from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.eip import EIPModel
from oasis.db.models.job import JobModel
from oasis.db.models.task import TaskModel
from oasis.utils import sdk
from oasis.utils.generator import get_url_suffix
from oasis.utils.sdk import feishu_client
from oasis.utils.sdk import ks3_client
from oasis.utils.sdk import price_client
from oasis.utils.sdk import product_client
from oasis.utils.sdk.charging import product_platform
from oasis.utils.sdk.iam import get_user_ak_sk_by_id
from oasis.utils.sdk.platform.tag import TagResource
from oasis.worker.planner import save_task_graph
from oasis.worker.tasks import set_job_context
import copy


class IamView(BaseView):
    """
        routes.append(('/CreateSlbProduct', IamView))
        routes.append(('/CreateInternalEip', IamView))
        routes.append(('/BindInternalEip', IamView))
        routes.append(('/ListKs3Buckets', IamView))
        routes.append(('/BindEip', IamView))
        routes.append(('/UnbindEip', IamView))
        routes.append(('/BindPrivateSlb', IamView))
        routes.append(('/UnbindPrivateSlb', IamView))
        routes.append(('/CheckConnectivityStatus', IamView))
        routes.append(('/GetLinkInfos', IamView))
    """

    async def create_slb_product(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        project_id = kwargs.get('project_id', 0)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        region = cluster.region
        availability_zone = cluster.availability_zone

        basic_items = {
            "productUse": PRODUCT_USE_MAP['BUY'],
            "productWhat": 1,
            "source": 1,
        }
        slb_bill_type = await price_client.get_slb_bill_type(self.account_id)
        slb_items = product_platform.form_slb_items(basic_items, availability_zone, slb_bill_type, cluster_id,
                                                    project_id)
        res = await product_client.create_eip_product(slb_items, self.account_id, region, product_type='SLB')
        res['num'] = 1
        total_price = 0.0
        total_price += res['num'] * res['price']
        return {'info': {'slb': res}}

    async def create_internal_eip(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        eip_line_id = kwargs.get('eip_line_id', None)
        eip_charge_type = kwargs.get('eip_charge_type', None)
        availability_zone = kwargs.get('availability_zone', None)
        eip_band_width = kwargs.get('eip_band_width', None)
        eip_purchase_time = kwargs.get('eip_purchase_time', None)
        eip_purchase_time_unit = kwargs.get('eip_purchase_time_unit', None)
        num = kwargs.get('num', None)
        project_id = kwargs.get('project_id', 0)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if not eip_line_id or not eip_charge_type:
            raise Exception(f'Please specify eip_line_id and eip_charge_type, got {eip_line_id}, {eip_charge_type}.')

        basic_items = {
            'productUse': PRODUCT_USE_MAP['BUY'],
            'productWhat': 1,
            'source': 1,
        }

        eip_instance_type = await price_client.get_eip_instance_type(self.account_id, self.region, value=10)
        eip_items = product_platform.form_eip_items(
            basic_items, availability_zone, eip_line_id, eip_instance_type, eip_band_width,
            eip_charge_type, eip_purchase_time, cluster_id,
            eip_purchase_time_unit, num, project_id=project_id
        )

        res = await product_client.create_eip_product(eip_items, self.account_id, self.region)
        res['num'] = num
        total_price = 0.0
        total_price += float(res['num']) * float(res['price'])
        res['price'] = total_price
        return {'info': {'eip': res}}

    async def bind_internal_eip(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        kwargs.setdefault('enable_eip', True)
        kwargs.setdefault('inner_eip_order_id',
                          kwargs.pop('order_id', None))

        job = JobModel(name='bind_internal_eip', status=JobModel.STATUS.Init, cluster_id=cluster_id)
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

        task_bind_internal_eip = TaskModel(name='TaskBindInternalEIP',
                                           args=kwargs)

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_bind_internal_eip: [task_send_feishu_done],
            task_send_feishu_done: [],
        }

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    async def list_ks3_buckets(self, *args, **kwargs):
        user_ak, user_sk = await get_user_ak_sk_by_id(self.account_id, self.product)
        buckets = await ks3_client.list_buckets(ak=user_ak, sk=user_sk)
        return {'body': buckets}

    async def bind_eip(self, *args, **kwargs):
        allocation_id = kwargs.get('allocation_id', None)
        ip_addr = kwargs.get('ip_addr', None)
        cluster_id = kwargs.get('cluster_id', None)
        slb_order_id = kwargs.get('slb_order_id', None)
        account_id = self.account_id

        kwargs.setdefault('sub_orders', {'cluster': {'slb': [slb_order_id]}})

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        if not allocation_id:
            raise Exception('Please specify allocation_id')

        if not ip_addr:
            raise Exception('Please specify ip_addr')

        if not slb_order_id:
            raise Exception('Please specify slb_order_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        job = JobModel(name='bind_eip', status=JobModel.STATUS.Init, cluster_id=cluster_id)
        await job.save()
        job_id = job.id

        self.context = {
            'product': self.product,
            'region': self.region,
            'availability_zone': kwargs.get('availability_zone'),
            'charge_type': kwargs.get('charge_type'),
            'distribution': kwargs.get('distribution'),
            'cluster_id': cluster_id,
            'cluster_type': cluster.cluster_type,
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

        # Prepare EIP
        task_create_eip = TaskModel(name='TaskCreateEIP',
                                    args={
                                        'ip_addr': ip_addr,
                                        'allocation_id': allocation_id,
                                    })

        # # Prepare SLB
        task_create_slb = TaskModel(name='TaskCreateSLB',
                                    args=kwargs)

        # Allocate EIP to SLB
        task_allocate_eip = TaskModel(name='TaskAllocateEIP2SLB',
                                      args={
                                          '$$$slb_id$$$': None,
                                          '$$$allocation_id$$$': None,
                                      })

        task_replace_resources_tags_bind_eip = TaskModel(name='TaskReplaceResourcesTags',
                                                         args={
                                                             'tags': cluster.tags,
                                                             'exec_mode': TagResource.EXEC.BIND,
                                                             '$$$new_instance_ids$$$': None
                                                         })

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_create_eip: [task_allocate_eip],
            task_create_slb: [task_allocate_eip],
            task_allocate_eip: [task_replace_resources_tags_bind_eip],
            task_replace_resources_tags_bind_eip: [task_send_feishu_done],
            task_send_feishu_done: [],
        }

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    async def unbind_eip(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        is_delete = kwargs.get('is_delete', 0)
        account_id = self.account_id

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        # JIRA问题 @PMUED-7361
        eip_info_query = model_query(EIPModel).filter(
            EIPModel.cluster_id == cluster_id,
            EIPModel.load_balancer_type == 0,
            EIPModel.status != EIPModel.STATUS.DELETED)
        eip_infos = await eip_info_query.query_all()
        if not eip_infos:
            raise Exception(self, f'Can not find any eip info of cluster {cluster_id}')

        # eip_client = getattr(sdk, f'eip_client_{cluster.cluster_type.lower()}')
        # slb_client = getattr(sdk, f'slb_client_{cluster.cluster_type.lower()}')

        # for eip_info in eip_infos:
        #     eip_id = eip_info.allocate_address_id
        #     slb_id = eip_info.load_balancer_id
        #     listener_ids = [listener_id for listener_id in eip_info.listener_id.values()]

        #     if eip_info.status == EIPModel.STATUS.BINDED:
        #         real_eip_info = await eip_client.describe_address(eip_id, account_id=account_id)
        #         real_state = real_eip_info.get('State', 'unknown')
        #         real_instance_type = real_eip_info.get('InstanceType', 'unknown')
        #         real_instance_id = real_eip_info.get('InstanceId', 'unknown')

        #         if real_state != 'associate':
        #             raise Exception(f'Unbind EIP failed, EIP {eip_id} status not [associate].')

        #         if real_instance_type != 'Slb' or real_instance_id != slb_id:
        #             raise Exception(f'Unbind EIP failed, EIP {eip_id} binding info was wrong, '
        #                             f'please contact administrator.')

        #         real_listener_ids = await slb_client.describe_listeners(
        #             slb_id, account_id=account_id
        #         )

        #         for real_listener_id in real_listener_ids:
        #             if real_listener_id not in listener_ids:
        #                 raise Exception(f'listener id {real_listener_id} of slb id {slb_id} not in OASIS db, '
        #                                 f'please contact administrator...')

        job = JobModel(name='unbind_eip', status=JobModel.STATUS.Init, cluster_id=cluster_id)
        await job.save()
        job_id = job.id

        self.context = {
            'product': self.product,
            'region': self.region,
            'availability_zone': kwargs.get('availability_zone'),
            'charge_type': kwargs.get('charge_type'),
            'distribution': kwargs.get('distribution'),
            'cluster_id': cluster_id,
            'cluster_type': cluster.cluster_type,
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

        # Disassociate EIP
        task_disassociate_eip = TaskModel(name='TaskDisassociateEIP', args={
            'is_delete': is_delete
        })

        # Delete EIP
        task_delete_slb = TaskModel(name='TaskDeleteSLB', args={
            'is_delete': is_delete,
            'unbind_slb_type': 0,
            '$$$slb_ids$$$': None,
            '$$$listener_ids$$$': None,
        })

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_disassociate_eip: [task_delete_slb],
            task_delete_slb: [task_send_feishu_done],
            task_send_feishu_done: [],
        }

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    async def bind_private_slb(self, *args, **kwargs):
        subnet_id = kwargs.get('subnet_id', None)
        private_ip_address = kwargs.get('private_ip_address', None)
        cluster_id = kwargs.get('cluster_id', None)
        slb_order_id = kwargs.get('slb_order_id', None)
        account_id = self.account_id

        kwargs.setdefault('sub_orders', {'cluster': {'slb': [slb_order_id]}})
        kwargs.setdefault('load_balancer_type', 'internal')
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        if not subnet_id:
            raise Exception('Please specify subnet_id')

        if not private_ip_address:
            raise Exception('Please specify private_ip_address')

        if not slb_order_id:
            raise Exception('Please specify slb_order_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        job = JobModel(name='bind_private_slb', status=JobModel.STATUS.Init, cluster_id=cluster_id)
        await job.save()
        job_id = job.id

        self.context = {
            'product': self.product,
            'region': self.region,
            'availability_zone': kwargs.get('availability_zone'),
            'charge_type': kwargs.get('charge_type'),
            'distribution': kwargs.get('distribution'),
            'cluster_id': cluster_id,
            'cluster_type': cluster.cluster_type,
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

        # # Prepare SLB
        task_create_slb = TaskModel(name='TaskCreateSLB',
                                    args=kwargs)

        # Allocate EIP to SLB
        task_bind_private_slb = TaskModel(name='TaskBindPrivateSLB',
                                          args={
                                              'private_ip_address': private_ip_address,
                                              '$$$slb_id$$$': None,
                                              '$$$allocation_id$$$': None,
                                          })

        task_replace_resources_tags_bind_eip = TaskModel(name='TaskReplaceResourcesTags',
                                                         args={
                                                             'tags': cluster.tags,
                                                             'exec_mode': TagResource.EXEC.BIND,
                                                             '$$$new_instance_ids$$$': None
                                                         })

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_create_slb: [task_bind_private_slb],
            task_bind_private_slb: [task_replace_resources_tags_bind_eip],
            task_replace_resources_tags_bind_eip: [task_send_feishu_done],
            task_send_feishu_done: [],
        }

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    async def unbind_private_slb(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        is_delete = kwargs.get('is_delete', 0)
        account_id = self.account_id
        slb_ids = []

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        eip_info_query = model_query(EIPModel).filter(
            EIPModel.cluster_id == cluster_id,
            EIPModel.load_balancer_type == 1,
            EIPModel.status != EIPModel.STATUS.DELETED)
        eip_infos = await eip_info_query.query_all()
        if not eip_infos:
            raise Exception(self, f'Can not find any eip info of cluster {cluster_id}')

        # slb_client = getattr(sdk, f'slb_client_{cluster.cluster_type.lower()}')

        # for eip_info in eip_infos:
        #     slb_id = eip_info.load_balancer_id
        #     slb_ids.append(slb_id)
        #     listener_ids = [listener_id for listener_id in eip_info.listener_id.values()]

        #     if eip_info.status == EIPModel.STATUS.BINDED:
        #         real_listener_ids = await slb_client.describe_listeners(
        #             slb_id, account_id=account_id
        #         )

        #         for real_listener_id in real_listener_ids:
        #             if real_listener_id not in listener_ids:
        #                 raise Exception(f'listener id {real_listener_id} of slb id {slb_id} not in OASIS db, '
        #                                 f'please contact administrator...')

        job = JobModel(name='unbind_private_slb', status=JobModel.STATUS.Init, cluster_id=cluster_id)
        await job.save()
        job_id = job.id

        self.context = {
            'product': self.product,
            'region': self.region,
            'availability_zone': kwargs.get('availability_zone'),
            'charge_type': kwargs.get('charge_type'),
            'distribution': kwargs.get('distribution'),
            'cluster_id': cluster_id,
            'cluster_type': cluster.cluster_type,
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

        # Delete SLB
        task_delete_slb = TaskModel(name='TaskDeleteSLB', args={
            'slb_ids': slb_ids,
            'unbind_slb_type': 1,
            'is_delete': is_delete,
            '$$$listener_ids$$$': None,
        })

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_delete_slb: [task_send_feishu_done],
            task_send_feishu_done: [],
        }

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    async def get_link_infos(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        eip_info_query = model_query(EIPModel).filter(
            EIPModel.cluster_id == cluster_id,
            EIPModel.status == EIPModel.STATUS.BINDED
        )
        eip_infos = await eip_info_query.query_all()

        result = {}

        for eip in eip_infos:

            # region 兼容老集群
            temp_link = {}
            listener_ids = eip.listener_id or {}
            _key = 'public' if eip.load_balancer_type == 0 else 'private'
            _conf = copy.deepcopy(DEFAULT_LINK.get(cluster.cluster_type.lower()).get(_key, {}))
            for service_name, service_port in _conf.items():
                if service_port in listener_ids:
                    temp_link[service_name] = service_port
            # endregion

            if eip.load_balancer_type == 0:
                result['public_infos'] = {
                    'address': eip.eip_address,
                    'links': temp_link,
                    'mask': get_url_suffix(cluster_id, product_type=cluster.cluster_type.lower())
                }
            else:
                result['private_infos'] = {
                    'address': eip.eip_address,
                    'links': temp_link
                }

        return result

    async def check_connectivity_status(self, *args, **kwargs):
        public_link = -1
        private_link = -1
        # public_msg = f'未知异常,请检查链路状态.（通常代表公网未绑定资源'
        # private_msg = f'未知异常,请检查链路状态.（通常代表私网未绑定资源'
        public_msg = ''
        private_msg = ''
        instance_ids = []

        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        for ig in cluster.instance_groups:
            for instance in ig.instances:
                instance_ids.append(instance.instance_id)

        eip_info_query = model_query(EIPModel).filter(
            EIPModel.cluster_id == cluster_id,
            EIPModel.status == EIPModel.STATUS.BINDED
        )
        eip_infos = await eip_info_query.query_all()

        eip_client = getattr(sdk, f'eip_client_{cluster.cluster_type.lower()}')
        slb_client = getattr(sdk, f'slb_client_{cluster.cluster_type.lower()}')

        # 理论上最多只会有两条记录，这里不进行迭代优化。

        for eip in eip_infos:
            # 公网 -> EIP check
            if eip.load_balancer_type == 0:
                if not eip.allocate_address_id:
                    # 用户绑定了EIP，但是数据库里没有记录：不提示，数据库自动补数据
                    public_msg = f''
                    continue
                real_eip_info = await eip_client.describe_address(eip.allocate_address_id, account_id=account_id)
                if not real_eip_info:
                    # 用户绑定了EIP，在EIP控制台操作了删除：弹性IP与集群负载均衡已解绑，访问地址不可用
                    public_msg = f'弹性IP与集群负载均衡已解绑，访问地址不可用'
                    continue
                elif real_eip_info.get('State', '') != 'associate' \
                        or real_eip_info.get('InstanceId', '') != eip.load_balancer_id:
                    # 用户绑定了EIP，在EIP控制台操作了解绑：弹性IP与集群负载均衡已解绑，访问地址不可用
                    public_msg = f'弹性IP与集群负载均衡已解绑，访问地址不可用'
                    continue

            if not eip.load_balancer_id:
                # 用户绑定了SLB，但是数据库里没有记录：不提示，数据库自动补数据
                public_msg = f''
                private_msg = f''
                continue

            # SLB echeck
            real_slb_infos = await slb_client.describe_load_balancers([eip.load_balancer_id], account_id=account_id)
            if not real_slb_infos:
                # 用户绑定了SLB，在LB控制台操作了删除：负载均衡与集群已解绑，访问地址不可用
                public_msg = f'负载均衡与集群已解绑，访问地址不可用'
                private_msg = f'负载均衡与集群已解绑，访问地址不可用'
                continue

            # 删除real_slb.get('LbStatus', '') != 'active' 校验，银河lb没这个参数
            elif len(real_slb_infos) > 0:
                real_slb = real_slb_infos[0]
                if real_slb.get('State', '') != 'associate' \
                        or real_slb.get('LoadBalancerState', '') != 'start':
                    # 用户绑定了SLB，LB状态异常（公网SLB，没有关联EIP）：弹性IP与集群负载均衡已解绑，访问地址不可用
                    public_msg = f'弹性IP与集群负载均衡已解绑，访问地址不可用'
                    private_msg = f'弹性IP与集群负载均衡已解绑，访问地址不可用'
                    continue

            # listener/rs check
            real_listener_info = await slb_client.describe_listeners_all(eip.load_balancer_id, account_id=account_id)
            db_listener_info = eip.listener_id

            if not real_listener_info:
                # 用户创建了监听器，但是库里没有记录：不提示，数据库自动补数据
                public_msg = f''
                private_msg = f''
                continue

            if not db_listener_info:
                # 用户创建了监听器，在LB控制台操作了删除：监听器异常，请检查您的监听器
                public_msg = f'监听器异常，请检查您的监听器'
                private_msg = f'监听器异常，请检查您的监听器'
                continue

            listener_status, listener_msg = slb_check_listener(real_listener_info, db_listener_info, instance_ids)
            if not listener_status:
                public_msg = listener_msg
                private_msg = listener_msg
                continue

            if eip.load_balancer_type == 0:
                public_link = 1
                public_msg = ''
            elif eip.load_balancer_type == 1:
                private_link = 1
                private_msg = ''

        return {
            'cluster_id': cluster_id,
            'public_link': public_link,
            'private_link': private_link,
            'public_msg': public_msg,
            'private_msg': private_msg,
        }
