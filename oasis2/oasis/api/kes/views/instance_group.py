import copy

from conf.charge_conf import PRODUCT_GROUP_ID_MAP
from oasis.api import BaseView
from oasis.api import openapi
from oasis.api.kes.methods import list_instance_groups
from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.es_plugin import EsPluginModel
from oasis.db.models.job import JobModel
from oasis.db.models.task import TaskModel
from oasis.utils.exceptions import ValidationError
from oasis.utils.generator import gen_uuid4, validate_instance_type_code
from oasis.utils.redlock import lock_cluster
from oasis.utils.sdk import feishu_client
from oasis.utils.sdk import price_client
from oasis.utils.sdk.platform.tag import TagResource
from oasis.utils.logger import logger
from oasis.worker.planner import save_task_graph
from oasis.worker.tasks import set_job_context
from oasis.worker.tasks.gringotts import TaskGringottsGetEsFreeNodes


class InstanceGroupView(BaseView):
    """
        routes.append(('/ListInstanceGroups', InstanceGroupView))
        routes.append(('/ScaleInInstanceGroups', InstanceGroupView))
        routes.append(('/ScaleOutInstanceGroups', InstanceGroupView))
        routes.append(('/UpgradeInstanceGroups', InstanceGroupView))
    """

    async def list_instance_groups(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        res = await list_instance_groups(cluster_id, account_id=account_id)
        return res

    async def rolling_restart_instance_groups(self, *args, **kwargs):
        """
        TODO: 此接口目前只是为了测试升配kec后滚动重启（gg服务侧+kec资源侧），不包含升配逻辑，方便测试
              以后如果有此需要，也可以将此接口转正为通用接口
        """
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if cluster.status not in [ClusterModel.STATUS.ACTIVE, ClusterModel.STATUS.FREEZE]:
            raise Exception(f'Cannot do operation, cluster status is {cluster.status}.')

        job = JobModel(name='rolling_restart_instances',
                       status=JobModel.STATUS.Init,
                       cluster_id=cluster_id)
        job_id = gen_uuid4()
        job.id = job_id

        lock_res = await lock_cluster(cluster_id, job_id)
        if not lock_res:
            raise Exception(f'Cluster has other tasks, please wait...')

        await job.save()

        self.context = {
            'product': self.product,
            'region': cluster.region,
            'availability_zone': cluster.availability_zone,
            'charge_type': cluster.charge_type,
            'distribution': cluster.distribution_version,
            'cluster_id': cluster_id,
            'cluster_type': cluster.cluster_type,
            'account_id': self.account_id,
            'tenant_id': self.tenant_id,
            'auth_token': self.user_token,
            'upgrade_instance_group': kwargs.get('instance_groups', [{}])[0],
            'is_upgrade_kec': True
        }

        await set_job_context(job_id, self.context)

        task_send_feishu_init = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.INIT,
            }
        )

        task_rolling_restart = TaskModel(name='TaskRollingRestart',
                                         args={})

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_rolling_restart: [task_send_feishu_done],
            task_send_feishu_done: []
        }

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    async def upgrade_instance_groups(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if cluster.status not in [ClusterModel.STATUS.ACTIVE, ClusterModel.STATUS.FREEZE]:
            raise Exception(f'Cannot do operation, cluster status is {cluster.status}.')

        if len(kwargs.get('instance_groups', [])) != 1:
            raise Exception('The number of instance groups does not meet expectations.')

        # 目标配置
        upgrade_instance_group = kwargs.get('instance_groups', [])[0]
        origin_instance_group = None
        # region 过滤升配项
        is_upgrade_kec = False
        is_upgrade_ebs = False
        is_upgrade_service = False
        is_upgrade_local = False

        upgrade_volume_type = upgrade_instance_group.get('volume_type', 'LOCAL_SSD')
        upgrade_volume_size = upgrade_instance_group.get('volume_size', '20')

        upgrade_resource_type = upgrade_instance_group['resource_type']
        upgrade_instance_type = upgrade_instance_group['instance_type_code']

        for ig in cluster.instance_groups:
            if ig.resource_type == 'KEC' and ig.id == upgrade_instance_group.get('id', ''):
                origin_instance_group = ig

        if not origin_instance_group:
            temp_id = upgrade_instance_group.get('id', '')
            raise Exception(f'Upgrade instance group not matched. {temp_id}')

        # 本地SSD型：若CPU、内存变更，需要处理KES和KEC订单。
        #           若CPU、内存保存不变，本地盘变更，只需要处理 KEC订单
        # 本地HDD型：只要有变更，就需要处理KES和KEC订单
        if upgrade_volume_type == origin_instance_group.volume_type and int(
                upgrade_volume_size) > int(origin_instance_group.volume_size):
            if upgrade_volume_type.startswith('CLOUD_'):
                is_upgrade_ebs = True
            elif upgrade_volume_type.startswith('LOCAL_'):
                is_upgrade_kec = True
                is_upgrade_local = True
                if upgrade_volume_type == 'LOCAL_HDD':
                    is_upgrade_service = True

        # cpu or mem '都是' 目标大于当前
        flag_instance_type_code = validate_instance_type_code(
            upgrade_instance_type, origin_instance_group.instance_type_code)
        if upgrade_resource_type == 'KEC' and flag_instance_type_code == 1:
            is_upgrade_kec = True
            is_upgrade_service = True
        # endregion

        kwargs.setdefault('product', self.product)
        kwargs.setdefault('account_id', self.account_id)
        kwargs.setdefault('cluster_type', cluster.cluster_type)
        kwargs.setdefault('distribution', cluster.distribution_version)

        job = JobModel(name='upgrade_instances', status=JobModel.STATUS.Init, cluster_id=cluster_id)
        job_id = gen_uuid4()
        job.id = job_id

        lock_res = await lock_cluster(cluster_id, job_id)
        if not lock_res:
            raise Exception(f'Cluster has other tasks, please wait...')

        await job.save()

        self.context = {
            'product': self.product,
            'region': cluster.region,
            'availability_zone': cluster.availability_zone,
            'charge_type': cluster.charge_type,
            'distribution': cluster.distribution_version,
            'cluster_id': cluster_id,
            'cluster_type': cluster.cluster_type,
            'account_id': self.account_id,
            'tenant_id': self.tenant_id,
            'auth_token': self.user_token,
            'upgrade_instance_group': upgrade_instance_group,
            'is_upgrade_kec': is_upgrade_kec,
            'is_upgrade_ebs': is_upgrade_ebs,
            'is_upgrade_service': is_upgrade_service,
            'is_upgrade_local': is_upgrade_local,
            'origin_instance_group_type_code': origin_instance_group.instance_type_code,
            'origin_instance_group_volume_size': origin_instance_group.volume_size or 0
        }

        await set_job_context(job_id, self.context)

        task_send_feishu_init = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.INIT,
            }
        )
        # 创建升配商品
        # Source参数不传，则默认来源是OpenAPI，控制台必须带上Source=1
        # task_upgrade_product = TaskModel(
        #     name='TaskUpgradeProduct', rollback_on_fail=True,
        #     args=kwargs,
        # )

        # # 创建升配订单
        # task_create_order = TaskModel(
        #     name='TaskCreateOrder', rollback_on_fail=True,
        #     args={
        #         'cluster_type': kwargs.get('cluster_type', None),
        #         'order_id': kwargs.get('order_id', None),
        #         '$$$order_product_details$$$': None,
        #     },
        # )

        task_milestone_1 = TaskModel(name='TaskUpdateCluster',
                                     args={
                                         'status': ClusterModel.STATUS.UPGRADING,
                                     })
        # 订单入库
        task_init_upgrade_order = TaskModel(name='TaskInitUpgradeOrder',
                                            args={
                                                'order_id': kwargs.get('order_id', None)
                                            })
        # 升配KEC
        task_upgrade_instance = TaskModel(name='TaskUpgradeInstance',
                                          args={})
        # 升配EBS
        task_upgrade_ebs = TaskModel(name='TaskUpgradeEbs',
                                     args={'$$$order_info_res$$$': None, })

        # 通知订单
        task_notify_order_for_upgrade = TaskModel(name='TaskNotifyOrderForUpgrade',
                                                  args={
                                                      'order_id': kwargs.get('order_id', None),
                                                      '$$$order_info_res$$$': None,
                                                  })

        # 服务侧升配
        task_gringotts_upgrade_cluster = TaskModel(name='TaskGringottsUpgradeCluster',
                                                   args=kwargs)

        # # 升配kec后滚动重启（gg服务侧+kec资源侧）
        # 滚动重启每一台机器都作为单独的一个任务（方便某一台机器重启有问题，可以skip单台）
        rolling_restart_tasks = []
        for origin_instance in origin_instance_group.instances:
            # 升配kec后滚动重启（gg服务侧+kec资源侧）
            task_rolling_restart = TaskModel(name='TaskRollingRestart',
                                             args={
                                                 '$$$order_info_res$$$': None,
                                                 'restart_instance_id': origin_instance.instance_id,
                                             })
            rolling_restart_tasks.append(task_rolling_restart)

        task_milestone_2 = TaskModel(name='TaskUpdateCluster',
                                     args={
                                         'status': ClusterModel.STATUS.ACTIVE,
                                     })
        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_milestone_1: [task_init_upgrade_order],
            task_init_upgrade_order: [task_upgrade_instance],
            task_upgrade_instance: [task_upgrade_ebs, task_notify_order_for_upgrade],
            task_upgrade_ebs: [task_notify_order_for_upgrade],
            task_notify_order_for_upgrade: [task_gringotts_upgrade_cluster],
            # task_gringotts_upgrade_cluster: [task_rolling_restart0],
            # task_rolling_restart0: [task_rolling_restart1],
            # task_rolling_restart1: [task_rolling_restart2],
            # task_rolling_restart2: [task_milestone_2, task_send_feishu_done],
            task_milestone_2: [],
            task_send_feishu_done: []
        }

        for index, restart_task in enumerate(rolling_restart_tasks):
            if index == 0:
                task_graph.setdefault(task_gringotts_upgrade_cluster, [restart_task])
                task_graph.setdefault(restart_task, [rolling_restart_tasks[1]])
            elif index < len(rolling_restart_tasks) - 1:
                task_graph.setdefault(restart_task, [rolling_restart_tasks[index+1]])
            else:
                task_graph.setdefault(restart_task, [task_milestone_2, task_send_feishu_done])

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    async def scale_in_instance_groups(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if cluster.status not in [ClusterModel.STATUS.ACTIVE]:
            raise Exception(f'Cannot do operation, cluster status is {cluster.status}.')

        # 待缩实例
        scale_in_instance_ids = []
        # 待缩实例组（含实例
        scale_in_instance_groups = dict()

        # region 缩容实例检查
        # 待缩容实例组（前端参数风格
        scale_in_instance_groups_base = kwargs.get('instance_groups', [])
        if not scale_in_instance_groups_base:
            raise Exception(f'Please specify the instance groups to scale in.')

        # region 简化参数结构
        '''
        {'id':'xx','instances':[{'instance_id':'xx1'}]}
        ===>
        {'xx':['xx1']}
        '''
        for scale_in_instance_group in scale_in_instance_groups_base:
            scale_in_instance_group_id = scale_in_instance_group.get('id')
            scale_in_instances = [i.get('instance_id') for i in scale_in_instance_group.get('instances')]
            if not scale_in_instances:
                raise Exception(f'Please specify the instances to scale in.')
            scale_in_instance_groups[scale_in_instance_group_id] = scale_in_instances
        # endregion

        # 校验参数用结构
        # 校验参数是否有效
        v_scale_in_instance_groups = copy.deepcopy(scale_in_instance_groups)

        for exist_instance_group in cluster.instance_groups:
            scale_in_instance = v_scale_in_instance_groups.pop(exist_instance_group.id, [])
            if not scale_in_instance:
                continue

            # 只允许缩容 data/warm/coordinator,不允许缩容master节点
            instance_group_type = exist_instance_group.instance_group_type.upper()
            if instance_group_type in ['MASTER']:
                raise Exception(f'The current instance group type <{instance_group_type}> does not support this operation.')

            # 填充待缩实例ID
            scale_in_instance_ids.extend(scale_in_instance)
            exist_instance_ids = [i.instance_id for i in exist_instance_group.instances
                                  if i.instance_id in scale_in_instance]
            if not exist_instance_ids or len(scale_in_instance) != len(exist_instance_ids):
                raise Exception(f'Some Instances not exist in the node group <{exist_instance_group.id}>.')

        if v_scale_in_instance_groups:
            raise Exception(f'Please specify the effective instance group to scale in.')

        if not scale_in_instance_ids:
            raise Exception(f'Please specify the effective instance to scale in.')
        # TODO VALID CHARGE TYPE
        # endregion

        logger.info(self, f'[{cluster_id}] ==>scale_in_instance_ids: {scale_in_instance_ids}')

        scale_in_sum = len(scale_in_instance_ids)

        job = JobModel(name='scale_in', status=JobModel.STATUS.Init, cluster_id=cluster_id)
        job_id = gen_uuid4()
        job.id = job_id

        lock_res = await lock_cluster(cluster_id, job_id)
        if not lock_res:
            raise Exception(f'Cluster has other tasks, please wait...')

        await job.save()

        self.context = {
            'product': self.product,
            'region': cluster.region,
            'availability_zone': cluster.availability_zone,
            'charge_type': cluster.charge_type,
            'distribution': cluster.distribution_version,
            'cluster_id': cluster_id,
            'cluster_type': cluster.cluster_type,
            'account_id': self.account_id,
            'tenant_id': self.tenant_id,
            'auth_token': self.user_token,
            'scale_in_sum': scale_in_sum
        }

        await set_job_context(job_id, self.context)

        task_send_feishu_init = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.INIT,
            }
        )

        task_gringotts_get_es_free_nodes = TaskModel(name='TaskGringottsGetEsFreeNodes',
                                                     args={
                                                         'scale_in_instance_ids': scale_in_instance_ids,
                                                     })

        task_milestone_1 = TaskModel(name='TaskUpdateCluster',
                                     rollback_on_fail=True,
                                     args={
                                         'status': ClusterModel.STATUS.SCALE_IN_WAITING,
                                     })

        task_gringotts_scale_in_cluster = TaskModel(name='TaskGringottsScaleInCluster',
                                                    args={
                                                        'instance_groups': scale_in_instance_groups_base,
                                                    })

        # 批量退订实例（SALE）
        task_delete_service_instance = TaskModel(name='TaskDeleteServiceInstance',
                                                 args={
                                                     'scale_in_instance_ids': scale_in_instance_ids,
                                                 })

        task_remove_instance_monitor = TaskModel(name='TaskRemoveInstanceMonitor',
                                                 args={
                                                     'scale_in_instance_ids': scale_in_instance_ids,
                                                 })

        task_release_internal_eip = TaskModel(name='TaskReleaseInternalEIP',
                                              args={
                                                  'scale_in_instance_ids': scale_in_instance_ids,
                                              })

        task_scale_in_release_slb = TaskModel(name='TaskScaleInReleaseSlb',
                                              args={
                                                  'unbind_slb_type': -1,
                                                  'scale_in_instance_ids': scale_in_instance_ids,
                                              })

        task_delete_inner_lb = TaskModel(name='TaskDeleteInnerLB',
                                         args={
                                             'scale_in_instance_ids': scale_in_instance_ids,
                                         })

        task_delete_ebs = TaskModel(name='TaskDeleteEbs',
                                    args={
                                        'scale_in_instance_ids': scale_in_instance_ids,
                                    })

        task_delete_instance = TaskModel(name='TaskDeleteInstance',
                                         args={
                                             'scale_in_instance_groups': scale_in_instance_groups,
                                             'scale_in_instance_ids': scale_in_instance_ids,
                                         })

        task_milestone_2 = TaskModel(name='TaskUpdateCluster',
                                     args={
                                         'status': ClusterModel.STATUS.ACTIVE,
                                     })

        task_send_feishu_done = TaskModel(name='TaskSendFeishu',
                                          args={
                                              'state': feishu_client.STATE.DONE,
                                          })

        task_graph = {
            task_send_feishu_init: [],

            # TODO: 本次kes缩容任务顺序(gg增加：检查缩容的节点组，是否有索引，即没有索引才可以缩此节点组)
            task_gringotts_get_es_free_nodes: [task_milestone_1],

            task_milestone_1: [task_gringotts_scale_in_cluster],
            task_gringotts_scale_in_cluster: [task_remove_instance_monitor],
            task_remove_instance_monitor: [task_delete_service_instance],
            task_delete_service_instance: [task_release_internal_eip],
            task_release_internal_eip: [task_scale_in_release_slb],
            task_scale_in_release_slb: [task_delete_inner_lb],
            task_delete_inner_lb: [task_delete_ebs],
            task_delete_ebs: [task_delete_instance],
            task_delete_instance: [task_milestone_2],
            task_milestone_2: [task_send_feishu_done],
            task_send_feishu_done: []
        }

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    @openapi
    async def scale_out_instance_groups(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if cluster.status not in [ClusterModel.STATUS.ACTIVE, ClusterModel.STATUS.FREEZE]:
            raise Exception(f'Cannot do operation, cluster status is {cluster.status}.')

        scale_out_instance_groups = kwargs.get('instance_groups', [])
        if not scale_out_instance_groups:
            raise Exception(f'Please specify the instance groups to scale out.')

        charge_type = kwargs.get('charge_type', None)
        purchase_time = int(kwargs.get('purchase_time', 0))
        if charge_type == 'FreeTrial' and purchase_time <= 0:
            raise Exception(f'Can not scale out cluster with purchase_time {purchase_time}')

        # TODO cache
        product_details = await price_client.get_product_details(
            self.account_id, PRODUCT_GROUP_ID_MAP[cluster.cluster_type], '1')

        exist_instance_group_ids = [ig.id for ig in cluster.instance_groups]
        scale_out_sum = 0
        for scale_out_ig in scale_out_instance_groups:
            validate_instance_type = False
            scale_out_ig['volume_size'] = int(scale_out_ig.get('volume_size', 0))
            scale_out_ig['volume_count'] = int(scale_out_ig.get('volume_count', 0))
            scale_count = scale_out_ig.get('instance_count', 0)
            if type(scale_count) != int:
                raise ValidationError(f'Invalid instance count, got {scale_count}')
            if scale_count < 1:
                raise ValidationError(f'Scale out instance count must greater than 0, got {scale_count}.')

            scale_out_ig_id = scale_out_ig.get('id', None)
            if scale_out_ig_id not in exist_instance_group_ids:
                scale_out_ig_id = gen_uuid4()
                scale_out_ig.setdefault('id', scale_out_ig_id)

            scale_out_sum += scale_count
            instance_type_code = scale_out_ig.get('instance_type_code', '')
            for product_detail in product_details.values():
                if instance_type_code in product_detail:
                    validate_instance_type = True
                    break
            if not validate_instance_type:
                raise ValidationError(f'\'InstanceType\' not found, '
                                      f'got {instance_type_code}.')

        kwargs.setdefault('product', self.product)
        kwargs.setdefault('account_id', self.account_id)

        kwargs.setdefault('cluster_type', cluster.cluster_type)
        kwargs.setdefault('availability_zone', cluster.availability_zone)
        kwargs.setdefault('region', cluster.region)
        kwargs.setdefault('charge_type', cluster.charge_type)
        kwargs.setdefault('distribution', cluster.distribution_version)

        user_install_plugins = [ep.ks3_address for ep in cluster.es_plugins
                                if ep.plugin_type == EsPluginModel.SOURCE.USER_DEFINE_PLUGIN and
                                ep.status == EsPluginModel.STATUS.INSTALL_STATUS]
        kwargs.setdefault('user_install_plugins', user_install_plugins)

        job = JobModel(name='scale_out', status=JobModel.STATUS.Init, cluster_id=cluster_id)
        job_id = gen_uuid4()
        job.id = job_id

        lock_res = await lock_cluster(cluster_id, job_id)
        if not lock_res:
            raise Exception(f'Cluster has other tasks, please wait...')

        await job.save()

        self.context = {
            'product': self.product,
            'region': cluster.region,
            'availability_zone': cluster.availability_zone,
            'charge_type': cluster.charge_type,
            'distribution': cluster.distribution_version,
            'cluster_id': cluster_id,
            'cluster_type': cluster.cluster_type,
            'account_id': self.account_id,
            'tenant_id': self.tenant_id,
            'auth_token': self.user_token,
            'scale_out_sum': scale_out_sum,
            'product_details': product_details,
        }

        await set_job_context(job_id, self.context)

        task_send_feishu_init = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.INIT,
            }
        )

        # 创建商品
        # Source参数不传，则默认来源是OpenAPI，控制台必须带上Source=1
        task_create_product = TaskModel(
            name='TaskCreateProduct', rollback_on_fail=True,
            args=kwargs,
        )

        # 创建订单
        task_create_order = TaskModel(
            name='TaskCreateOrder', rollback_on_fail=True,
            args={
                'cluster_type': kwargs.get('cluster_type', None),
                'order_id': kwargs.get('order_id', None),
                '$$$order_product_details$$$': None,
            },
        )

        task_init_cluster = TaskModel(
            name='TaskInitClusterScale', rollback_on_fail=True,
            args={
                '$$$order_id$$$': None,
                'scale_out_instance_groups': scale_out_instance_groups,
            })

        task_milestone_1 = TaskModel(name='TaskUpdateCluster',
                                     rollback_on_fail=True,
                                     args={
                                         'status': ClusterModel.STATUS.SCALE_OUT_WAITING,
                                     })

        task_get_security_group = TaskModel(
            name='TaskGetSecurityGroup', rollback_on_fail=True,
            args={
                'security_group_id': kwargs.get('security_group_id', None),
            })

        # CONTROL安全组
        task_create_control_security_group = TaskModel(name='TaskCreateControlSecurityGroup')

        # KEC / EPC
        task_create_instance = TaskModel(name='TaskCreateInstance',
                                         rollback_on_fail=True,
                                         args={
                                             '$$$order_id$$$': None,
                                             '$$$security_group_id$$$': None,
                                             '$$$product_details$$$': None,
                                         })

        # ELB
        task_create_inner_lb = TaskModel(name='TaskCreateInnerLB',
                                         rollback_on_fail=True,
                                         args={
                                             '$$$new_instance_ids$$$': None,
                                         })

        task_milestone_2 = TaskModel(name='TaskUpdateCluster',
                                     rollback_on_fail=True,
                                     args={
                                         'status': ClusterModel.STATUS.SCALE_OUT_PREPARING,
                                     })

        # SG && CSG && await_net_work
        task_provision_control_security_group = TaskModel(name='TaskProvisionControlSecurityGroup',
                                                          rollback_on_fail=True,
                                                          args={
                                                              '$$$control_security_group_id$$$': None,
                                                              '$$$new_instance_ids$$$': None,
                                                          })

        task_check_instance_ready = TaskModel(name='TaskCheckInstanceReady', rollback_on_fail=True)

        # EBS
        task_create_ebs = TaskModel(name='TaskCreateEbs',
                                    rollback_on_fail=True,
                                    args={
                                        '$$$order_id$$$': None,
                                        '$$$new_instance_ids$$$': None,
                                    })

        task_attach_ebs = TaskModel(name='TaskAttachEbs',
                                    rollback_on_fail=True,
                                    args={
                                        '$$$new_instance_ids$$$': None,
                                    })

        task_mount_ebs = TaskModel(name='TaskMountEbs',
                                   rollback_on_fail=True,
                                   args={
                                       '$$$new_instance_ids$$$': None,
                                   })

        task_milestone_3 = TaskModel(name='TaskUpdateCluster',
                                     args={
                                         'status': ClusterModel.STATUS.SCALE_OUT_CONFIGURING,
                                     })

        # 配置实例（注入ssh 秘钥等
        task_install_gringotts_agent = TaskModel(name='TaskInstallGringottsAgent',
                                                 args={
                                                     '$$$new_instance_ids$$$': None,
                                                 })

        task_config_hostname = TaskModel(name='TaskConfigHostname',
                                         args={
                                             '$$$new_instance_ids$$$': None,
                                         })

        # 回写订单，实例生命周期，集中回写
        task_notify_order = TaskModel(name='TaskNotifyOrder',
                                      args={
                                          '$$$order_id$$$': None,
                                      })
        task_scale_out_bind_slb = TaskModel(name='TaskScaleOutBindSlb',
                                            args={
                                                '$$$new_instance_ids$$$': None,
                                            })
        # add monitor
        task_add_cluster_monitor = TaskModel(name='TaskAddClusterMonitor')

        task_add_instance_monitor = TaskModel(name='TaskAddInstanceMonitor',
                                              args={
                                                  '$$$new_instance_ids$$$': None,
                                              })

        # gringgotts
        kwargs.setdefault('$$$new_instance_ids$$$', None)
        task_gringotts_scale_out_cluster = TaskModel(name='TaskGringottsScaleOutCluster', args=kwargs)

        task_milestone_4 = TaskModel(name='TaskUpdateCluster',
                                     args={
                                         'status': ClusterModel.STATUS.ACTIVE,
                                     })

        task_bind_internal_eip = TaskModel(name='TaskBindInternalEIP',
                                           args={
                                               'inner_eip_order_id': kwargs.get('order_id', None),
                                               'project_id': kwargs.get('project_id', None),
                                               'line_id': kwargs.get('eip_line_id', None),
                                               'charge_type': kwargs.get('eip_charge_type', None),
                                               'band_width': kwargs.get('eip_band_width', None),
                                               '$$$new_instance_ids$$$': None,
                                           })

        task_replace_resources_tags_create = TaskModel(name='TaskReplaceResourcesTags',
                                                       args={
                                                           'tags': cluster.tags,
                                                           'exec_mode': TagResource.EXEC.SCALE,    # task_bind_internal_eip?all
                                                           '$$$new_instance_ids$$$': None,
                                                       })

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        # 大数据云网络接口-扩容回调
        task_send_scale_notification = TaskModel(name='TaskSendScaleNotification',
                                                 args={
                                                     'action': 'Scaleout',
                                                     '$$$new_instance_ids$$$': None,
                                                 })

        task_graph = {
            task_send_feishu_init: [],
            task_create_product: [task_create_order],
            task_create_order: [task_init_cluster,
                                task_notify_order,
                                task_create_instance,
                                task_create_ebs],
            task_init_cluster: [task_milestone_1],
            task_milestone_1: [task_get_security_group, task_create_control_security_group],
            task_get_security_group: [task_create_instance],
            task_create_control_security_group: [task_provision_control_security_group],
            task_create_instance: [task_create_inner_lb,
                                   task_install_gringotts_agent,
                                   task_config_hostname,
                                   task_add_instance_monitor,
                                   task_provision_control_security_group,
                                   task_create_ebs,
                                   task_attach_ebs,
                                   task_mount_ebs,
                                   task_scale_out_bind_slb,
                                   task_bind_internal_eip,
                                   task_replace_resources_tags_create,
                                   task_gringotts_scale_out_cluster],
            task_create_inner_lb: [task_milestone_2],
            task_milestone_2: [task_provision_control_security_group],
            task_provision_control_security_group: [task_check_instance_ready],
            task_check_instance_ready: [task_create_ebs],
            task_create_ebs: [task_attach_ebs],
            task_attach_ebs: [task_mount_ebs],
            task_mount_ebs: [task_milestone_3],
            task_milestone_3: [task_config_hostname, task_notify_order],
            task_config_hostname: [task_install_gringotts_agent],
            task_install_gringotts_agent: [task_gringotts_scale_out_cluster],
            task_notify_order: [task_replace_resources_tags_create, task_milestone_4],
            task_add_cluster_monitor: [task_add_instance_monitor],
            task_add_instance_monitor: [task_replace_resources_tags_create, task_milestone_4],
            task_replace_resources_tags_create: [task_milestone_4],
            task_gringotts_scale_out_cluster: [task_milestone_4],
            task_milestone_4: [task_scale_out_bind_slb, task_bind_internal_eip],
            task_scale_out_bind_slb: [task_send_feishu_done, task_send_scale_notification],
            task_bind_internal_eip: [task_send_feishu_done, task_send_scale_notification],
            task_send_feishu_done: [],
            task_send_scale_notification: [],
        }
        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }
