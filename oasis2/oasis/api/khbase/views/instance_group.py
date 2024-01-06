from conf.charge_conf import PRODUCT_GROUP_ID_MAP
from oasis.api import BaseView
from oasis.api import openapi
from oasis.api.khbase.methods import list_instance_groups
from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.job import JobModel
from oasis.db.models.task import TaskModel
from oasis.utils.exceptions import ValidationError
from oasis.utils.generator import gen_uuid4
from oasis.utils.redlock import lock_cluster
from oasis.utils.sdk import feishu_client
from oasis.utils.sdk import price_client
from oasis.worker.planner import save_task_graph
from oasis.worker.tasks import set_job_context


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

    async def scale_in_instance_groups(self, *args, **kwargs):

        # TODO scale in
        # if await redlock.is_locked(gen_cluster_lock(cluster_id)):
        #     raise Exception(f'Cluster has other tasks, please wait...')
        #
        # job = JobModel(name='delete_cluster', status=JobModel.STATUS.Doing, cluster_id=cluster_id)
        # await job.save()
        # job_id = job.id
        #
        # lock_res = await lock_cluster(cluster_id, job_id)
        # if not lock_res:
        #     raise Exception(f'Cluster has other tasks, please wait...')

        return 'Currently did not support.'

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

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

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
            task_notify_order: [task_milestone_4],
            task_add_cluster_monitor: [task_add_instance_monitor],
            task_add_instance_monitor: [task_milestone_4],
            task_gringotts_scale_out_cluster: [task_milestone_4],
            task_milestone_4: [task_bind_internal_eip],
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

    async def upgrade_instance_groups(self, *args, **kwargs):
        pass
