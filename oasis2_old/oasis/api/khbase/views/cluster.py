from conf.charge_conf import PRODUCT_GROUP_ID_MAP
from oasis.api import BaseView
from oasis.api import openapi
from oasis.api.khbase.methods import describe_cluster_from_db
from oasis.api.khbase.methods import list_clusters_from_db
from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.eip import EIPModel
from oasis.db.models.instance import InstanceModel
from oasis.db.models.job import JobModel
from oasis.db.models.task import TaskModel
from oasis.utils import sdk
from oasis.utils.chaos import CLUSTER_STATUS_CONVERT_MAP
from oasis.utils.chaos import DISTRIBUTION_SCHEMAS
from oasis.utils.convert import str2datetime
from oasis.utils.convert import translate_marker_str
from oasis.utils.exceptions import ValidationError
from oasis.utils.generator import gen_name
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger
from oasis.utils.redlock import lock_cluster
from oasis.utils.sdk import feishu_client
from oasis.utils.sdk import gringotts_client
from oasis.utils.sdk import price_client
from oasis.worker.planner import save_task_graph
from oasis.worker.tasks import set_job_context


class ClusterView(BaseView):
    """
        routes.append(('/DescribeCluster', ClusterView))
        routes.append(('/ListClusters', ClusterView))
        routes.append(('/ModifyClusterName', ClusterView))
        routes.append(('/LaunchCluster', ClusterView))
        routes.append(('/DeleteClusterProtection', ClusterView))
        routes.append(('/DeleteCluster', ClusterView))
        routes.append(('/FreezeCluster', ClusterView))
        routes.append(('/UnfreezeCluster', ClusterView))
    """

    @openapi
    async def describe_cluster(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')
        cluster = await describe_cluster_from_db(cluster_id, account_id=account_id)

        # For openapi
        if self.source == 'user':
            return cluster

        proxy_services = await gringotts_client.describe_cluster_force(cluster_id, token=self.user_token)
        cluster.setdefault('ProxyServices', proxy_services.get('ProxyServices', []) or [])

        return cluster

    @openapi
    async def list_clusters(self, *args, **kwargs):
        account_id = self.account_id

        # reformat kwargs
        created_before = str2datetime(kwargs.pop('created_before', None))
        created_after = str2datetime(kwargs.pop('created_after', None))

        marker_str = kwargs.pop('marker', 'offset=0 & limit=10')
        marker = translate_marker_str(marker_str)
        offset = marker.get('offset', 0)
        limit = marker.get('limit', 10)

        cluster_status = [sta for status in kwargs.pop('cluster_status', [])
                          for sta in CLUSTER_STATUS_CONVERT_MAP.get(status, ['NonDeleted'])]

        # Fuzzy/Name/Id
        filters = kwargs.pop('filters', [])

        count, f_clusters = await list_clusters_from_db(filters, offset, limit,
                                                        cluster_status=cluster_status,
                                                        created_after=created_after,
                                                        created_before=created_before,
                                                        account_id=account_id)

        marker_str = None
        if count > (marker['limit'] + marker['offset']):
            marker_str = 'offset=%d & limit=%d' % (marker['offset'] + marker['limit'], marker['limit'])

        result = {
            'Clusters': f_clusters,
            'Total': count,
            'Marker': marker_str,
        }

        return result

    async def modify_cluster_name(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')
        cluster_name = kwargs.get('cluster_name', None)
        if not cluster_name:
            raise Exception(f'Please specify cluster name, got {cluster_name}')
        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        await cluster.save({'name': cluster_name})
        return cluster

    @openapi
    async def launch_cluster(self, *args, **kwargs):
        if kwargs.get('dry_run', False):
            return {"DryRun": True}

        '''
        kwargs = {
            'name': 'test-kes',
            'cluster_type': 'KES',
            'distribution': 'kes-1.0.0',
            'main_version': '1.4.12',
            'termination_protected': True,
            'instance_groups':
                [
                    {'instance_group_type': 'MASTER',
                     'resource_type': 'KEC',
                     'instance_type_code': 'ES.basic.2C4G',
                     'instance_count': 3,
                     'resouce_attributes': [
                         {'name': 'bandwidth', 'value': '2000'},
                         {'name': 'pps', 'value': '500000'},
                         {'name': 'bond_type', 'value': '0'},
                         {'name': 'raid_type', 'value': 'raid50'}
                     ],
                     'system_volume_type': 'LOCAL',
                     'system_volume_size': 20,
                     'volume_type': 'CLOUD_SSD',
                     'volume_size': 2000,
                     'volume_count': 3,
                     'vpc_id': 'vvvv-pppp-ccccc-1111',
                     'vpc_subnet_id': 'vvvv-pppp-ccccc-ssss',
                     'avalability_zone': 'cn-beijing-6a',
                     'charge_type': 'Monthly',
                     'purchase_time': '',
                     'expire_time': '',
                     'order_id': 'oooo-rrrr-dddd-eeee'
                     },
                    {'instance_group_type': 'CORE',
                     'resource_type': 'KEC',
                     'instance_type_code': 'ES.basic.2C4G',
                     'instance_count': 3,
                     'resouce_attributes':
                         [
                             {'name': 'bandwidth', 'value': '2000'},
                             {'name': 'pps', 'value': '500000'},
                             {'name': 'bond_type', 'value': '0'},
                             {'name': 'raid_type', 'value': 'raid50'}
                         ],
                     'system_volume_type': 'LOCAL',
                     'system_volume_size': 20,
                     'volume_type': 'CLOUD_SSD',
                     'volume_size': 2000,
                     'volume_count': 3,
                     'vpc_id': 'vvvv-pppp-ccccc-1111',
                     'vpc_subnet_id': 'vvvv-pppp-ccccc-ssss',
                     'avalability_zone': 'cn-beijing-6a',
                     'charge_type': 'Monthly',
                     'purchase_time': '',
                     'expire_time': '',
                     'order_id': 'oooo-rrrr-dddd-eeee'}
                ],
            'enable_eip': True,
            'eip_line_id': 'bgp',
            'eip_charge_type': 'Monthly',
            'eip_band_width': 1,
            'eip_purchase_time': 10,
            'eip_purchase_time_unit': 2,
            'ip_addr': '172.0.0.1',
            'allocation_id': 'aaaa-llll-llll-oooo',
            'availability_zone': 'cn-beijing-6a',
            'vpc_domain_id': 'vvvv-pppp-cccc-dddd',
            'vpc_subnet_id': 'vvvv-ssss-iiii-dddd',
            'charge_type': 'Monthly',
            'purchase_time': 2,
            'purchase_time_unit': 2,
            'expire_time': '2020-03-05 12:00:12',
            'order_id': 'oooo-rrrr-dddd-eeee',
            'project_id': 'pppp-rrrr-oooo-jjjj',
            'launch_resume_type': 'all',
            'request_id': 'rrrr-eeee-qqqq-uuuu',
            'dry_run': False
        }
        '''
        cluster_id = kwargs.get('cluster_id', None)
        cluster_name = kwargs.get('cluster_name', None)
        cluster_type = 'KHBASE'
        distribution = kwargs.get('distribution', None)

        if not cluster_id:
            cluster_id = gen_uuid4()
            kwargs.setdefault('cluster_id', cluster_id)

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if cluster:
            raise Exception(f'Cluster already exist, id {cluster_id}')

        if not cluster_name:
            cluster_name = gen_name(self.product, 'timestamp')
            kwargs.setdefault('cluster_name', cluster_name)

        if distribution not in DISTRIBUTION_SCHEMAS.get('KHBASE', []):
            raise Exception(f'KHBASE did not support distribution {distribution}')

        # TODO cache
        product_details = await price_client.get_product_details(
            self.account_id, PRODUCT_GROUP_ID_MAP[cluster_type], '1')
        logger.info(self, f'==product_details: {product_details}')
        kwargs.setdefault('product_details', product_details)

        instance_groups = kwargs.get('instance_groups', [])
        for ig in instance_groups:
            validate_instance_type = False
            ig['volume_size'] = int(ig.get('volume_size', 0))
            ig['volume_count'] = int(ig.get('volume_count', 0))
            instance_type_code = ig.get('instance_type_code', '')
            for product_detail in product_details.values():
                if instance_type_code in product_detail:
                    validate_instance_type = True
                    break
            if not validate_instance_type:
                raise ValidationError(f'\'InstanceType\' not found, '
                                      f'got {instance_type_code}.')

        job = JobModel(name='launch_cluster', status=JobModel.STATUS.Init, cluster_id=cluster_id)
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
            'cluster_type': cluster_type,
            'account_id': self.account_id,
            'tenant_id': self.tenant_id,
            'auth_token': self.user_token,
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
                'order_id': kwargs.get('order_id', None),
                '$$$order_product_details$$$': None,
            },
        )

        # 生成数据库
        init_args = {
            '$$$order_id$$$': None,
        }
        init_args.update(kwargs)
        task_init_cluster = TaskModel(name='TaskInitClusterCreate', rollback_on_fail=True, args=init_args)

        #  生成key
        task_create_epc_key = TaskModel(name='TaskCreateSshKey', rollback_on_fail=True)

        # 安全组
        task_create_security_group = TaskModel(name='TaskCreateSecurityGroup',
                                               rollback_on_fail=True,
                                               args={
                                                   'security_group_id': kwargs.get('security_group_id', None),
                                               })

        # CONTROL安全组
        task_create_control_security_group = TaskModel(name='TaskCreateControlSecurityGroup', rollback_on_fail=True)

        # 子网
        task_create_subnet = TaskModel(name='TaskCreateSubnet',
                                       rollback_on_fail=True,
                                       args={
                                           '$$$security_group_id$$$': None,
                                       })

        task_milestone_1 = TaskModel(name='TaskUpdateCluster',
                                     rollback_on_fail=True,
                                     args={
                                         'status': ClusterModel.STATUS.WAITING,
                                     })

        # KEC / EPC
        task_create_instance = TaskModel(name='TaskCreateInstance',
                                         rollback_on_fail=True,
                                         args={
                                             '$$$security_group_id$$$': None,
                                             '$$$order_id$$$': None,
                                             '$$$product_details$$$': None,
                                         })

        # ELB
        task_create_inner_lb = TaskModel(name='TaskCreateInnerLB',
                                         rollback_on_fail=True,
                                         args={
                                             '$$$new_instance_ids$$$': None,
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

        task_milestone_2 = TaskModel(name='TaskUpdateCluster',
                                     args={
                                         'status': ClusterModel.STATUS.CONFIGURING,
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

        task_milestone_3 = TaskModel(name='TaskUpdateCluster',
                                     args={
                                         'status': ClusterModel.STATUS.STARTING,
                                         'activate': True,
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
        task_gringotts_launch_cluster = TaskModel(name='TaskGringottsLaunchCluster', args=kwargs)

        task_milestone_4 = TaskModel(name='TaskUpdateCluster',
                                     args={
                                         'status': ClusterModel.STATUS.ACTIVE,
                                         'is_terminate_protection': True,
                                     })

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_create_product: [task_create_order],
            task_create_order: [task_init_cluster,
                                task_notify_order,
                                task_create_instance,
                                task_create_ebs],
            task_init_cluster: [task_create_security_group, task_create_epc_key, task_send_feishu_init],
            task_send_feishu_init: [],
            task_create_epc_key: [task_create_instance],
            task_create_security_group: [task_create_control_security_group,
                                         task_create_subnet,
                                         task_create_instance],
            task_create_control_security_group: [task_create_subnet,
                                                 task_provision_control_security_group],
            task_create_subnet: [task_milestone_1],
            task_milestone_1: [task_create_instance],
            task_create_instance: [task_create_inner_lb,
                                   task_install_gringotts_agent,
                                   task_config_hostname,
                                   task_add_instance_monitor,
                                   task_provision_control_security_group,
                                   task_create_ebs,
                                   task_attach_ebs,
                                   task_mount_ebs,
                                   ],
            task_create_inner_lb: [task_provision_control_security_group],
            task_provision_control_security_group: [task_check_instance_ready],
            task_check_instance_ready: [task_create_ebs],
            task_create_ebs: [task_attach_ebs],
            task_attach_ebs: [task_mount_ebs],
            task_mount_ebs: [task_milestone_2],
            task_milestone_2: [task_config_hostname],
            task_config_hostname: [task_install_gringotts_agent],
            task_install_gringotts_agent: [task_milestone_3],
            task_milestone_3: [task_notify_order,
                               task_add_cluster_monitor,
                               task_gringotts_launch_cluster],
            task_notify_order: [task_milestone_4],
            task_add_cluster_monitor: [task_add_instance_monitor],
            task_add_instance_monitor: [task_milestone_4],
            task_gringotts_launch_cluster: [task_milestone_4],
            task_milestone_4: [task_send_feishu_done],
            task_send_feishu_done: [],
        }

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        eip_line_id = kwargs.get('eip_line_id', None)
        allocation_id = kwargs.get('allocation_id', None)
        need_bind_eip = eip_line_id or allocation_id

        if need_bind_eip:
            # Move Create EIP into individual job
            job_1 = JobModel(name='bind_eip', status=JobModel.STATUS.Init, cluster_id=cluster_id,
                             parent_job=job_id)
            await job_1.save()
            job_1_id = job_1.id

            task_eip_send_feishu_init = TaskModel(
                name='TaskSendFeishu',
                args={
                    'state': feishu_client.STATE.INIT,
                }
            )

            # EIP
            task_create_eip = TaskModel(name='TaskCreateEIP',
                                        rollback_on_fail=True,
                                        args=kwargs)

            # Prepare SLB
            task_create_slb = TaskModel(name='TaskCreateSLB',
                                        rollback_on_fail=True,
                                        args=kwargs)

            # Allocate EIP to SLB
            task_allocate_eip = TaskModel(name='TaskAllocateEIP2SLB',
                                          rollback_on_fail=True,
                                          args={
                                              '$$$slb_id$$$': None,
                                              '$$$allocation_id$$$': None,
                                          })

            task_eip_send_feishu_done = TaskModel(
                name='TaskSendFeishu',
                args={
                    'state': feishu_client.STATE.DONE,
                }
            )

            task_1_graph = {
                task_eip_send_feishu_init: [],
                task_create_eip: [task_allocate_eip],
                task_create_slb: [task_allocate_eip],
                task_allocate_eip: [task_eip_send_feishu_done],
                task_eip_send_feishu_done: [],
            }
            await save_task_graph(job_1_id, task_1_graph)

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    async def delete_cluster_protection(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        set_protect = kwargs.get('termination_protected', 1)

        if not cluster_id:
            raise Exception(f'Please specify cluster_id.')

        if set_protect:
            raise Exception(f'Please specify correct param, ask administrator...')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster {cluster_id} not found.')

        if cluster.status not in [ClusterModel.STATUS.ACTIVE, ClusterModel.STATUS.FREEZE]:
            raise Exception(f'Cannot do operation, cluster status is {cluster.status}.')

        if cluster.charge_type == 'Monthly':
            raise Exception(f'Cluster charge type is Monthly, could not be deleted.')

        await cluster.save({'is_terminate_protection': set_protect})
        return {'cluster_id': cluster_id}

    async def delete_cluster(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        # JIRA问题 @PMUED-7361
        need_disassociate_eip = False

        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')
        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if cluster.status == ClusterModel.STATUS.DELETED:
            return True

        if cluster.status not in [ClusterModel.STATUS.ACTIVE, ClusterModel.STATUS.FREEZE]:
            raise Exception(f'Cannot do operation, cluster status is {cluster.status}.')

        if cluster.is_terminate_protection:
            raise Exception(f'Cluster is under protection, id {cluster_id}')

        if cluster.charge_type == 'Monthly':
            raise Exception(f'Cluster charge type is Monthly, could not be deleted.')

        # JIRA问题 @PMUED-7361
        eip_info_query = model_query(EIPModel).filter(EIPModel.cluster_id == cluster_id,
                                                      EIPModel.status != EIPModel.STATUS.DELETED)
        eip_infos = await eip_info_query.query_all()

        if eip_infos:
            need_disassociate_eip = True

        eip_client = getattr(sdk, f'eip_client_{cluster.cluster_type.lower()}')
        slb_client = getattr(sdk, f'slb_client_{cluster.cluster_type.lower()}')

        for eip_info in eip_infos:
            eip_id = eip_info.allocate_address_id
            slb_id = eip_info.load_balancer_id
            listener_ids = [listener_id for listener_id in eip_info.listener_id.values()]

            if eip_info.status == EIPModel.STATUS.BINDED:
                real_eip_info = await eip_client.describe_address(eip_id, account_id=account_id)
                real_state = real_eip_info.get('State', 'unknown')
                real_instance_type = real_eip_info.get('InstanceType', 'unknown')
                real_instance_id = real_eip_info.get('InstanceId', 'unknown')

                if real_state != 'associate':
                    logger.info(self, f'Unbind EIP failed, EIP {eip_id} status not [associate].')
                    need_disassociate_eip = False

                if real_instance_type != 'Slb' or real_instance_id != slb_id:
                    logger.info(self, f'Unbind EIP failed, EIP {eip_id} binding info was wrong, '
                                      f'please contact administrator.')
                    need_disassociate_eip = False

                real_listener_ids = await slb_client.describe_listeners(
                    slb_id, account_id=account_id
                )

                for real_listener_id in real_listener_ids:
                    if real_listener_id not in listener_ids:
                        logger.info(f'listener id {real_listener_id} of slb id {slb_id} not in OASIS db, '
                                    f'please contact administrator...')
                        need_disassociate_eip = False

        job = JobModel(name='delete_cluster', status=JobModel.STATUS.Init, cluster_id=cluster_id)
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
        }

        await set_job_context(job_id, self.context)

        is_expire = kwargs.get('instance_recycle_status', 0)

        task_send_feishu_init = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.INIT,
            }
        )

        task_milestone_1 = TaskModel(name='TaskUpdateCluster',
                                     args={
                                         'status': ClusterModel.STATUS.DELETING,
                                     })

        task_remove_instance_monitor = TaskModel(name='TaskRemoveInstanceMonitor')

        task_remove_cluster_monitor = TaskModel(name='TaskRemoveClusterMonitor')

        task_gringotts_delete_cluster = TaskModel(name='TaskGringottsDeleteCluster')

        task_delete_service_instance = TaskModel(name='TaskDeleteServiceInstance')

        task_release_internal_eip = TaskModel(name='TaskReleaseInternalEIP')

        task_delete_inner_lb = TaskModel(name='TaskDeleteInnerLB')

        task_delete_ebs = TaskModel(name='TaskDeleteEbs')

        task_delete_instance = TaskModel(name='TaskDeleteInstance')

        task_delete_dataguard = TaskModel(name='TaskDeleteDataguard')

        task_delete_epc_key = TaskModel(name='TaskDeleteEpcKey')

        task_milestone_2 = TaskModel(name='TaskUpdateCluster',
                                     args={
                                         'status': ClusterModel.STATUS.DELETED,
                                     })

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_milestone_1: [task_gringotts_delete_cluster],
            task_remove_instance_monitor: [task_remove_cluster_monitor],
            task_remove_cluster_monitor: [task_gringotts_delete_cluster],
        }

        if not is_expire:  # 如果是主动退订，需要调用统一退订的订单接口退订所有的服务实例，然后调用各业务线的api退订实例
            task_graph.update({
                task_gringotts_delete_cluster: [task_delete_service_instance],
                task_delete_service_instance: [task_release_internal_eip],
                task_release_internal_eip: [task_delete_inner_lb],
                task_delete_inner_lb: [task_delete_ebs],
                task_delete_ebs: [task_delete_instance],
                task_delete_instance: [task_delete_dataguard],
                task_delete_dataguard: [task_delete_epc_key],
                task_delete_epc_key: [task_milestone_2],
            })
        else:  # 如果是到期退订，订单系统发送mq给各个业务线，kes和khbase由于是虚拟产品，因此只需要把集群更改为Deleted
            task_update_instance_status = TaskModel(name='TaskUpdateInstanceStatus',
                                                    args={
                                                        'status': InstanceModel.STATUS.DELETED
                                                    })

            task_graph.setdefault(task_gringotts_delete_cluster, [task_update_instance_status])
            task_graph.setdefault(task_update_instance_status, [task_milestone_2])

        task_graph.setdefault(task_milestone_2, [task_send_feishu_done])
        task_graph.setdefault(task_send_feishu_done, [])

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        if need_disassociate_eip:
            # Move Unbind EIP into individual job
            job_1 = JobModel(name='unbind_eip', status=JobModel.STATUS.Init, cluster_id=cluster_id,
                             parent_job=job_id)
            await job_1.save()
            job_1_id = job_1.id

            task_eip_send_feishu_init = TaskModel(
                name='TaskSendFeishu',
                args={
                    'state': feishu_client.STATE.INIT,
                }
            )

            # Disassociate EIP
            task_disassociate_eip = TaskModel(name='TaskDisassociateEIP')

            # Delete EIP
            task_delete_slb = TaskModel(name='TaskDeleteSLB', args={
                '$$$slb_ids$$$': None,
            })

            task_eip_send_feishu_done = TaskModel(
                name='TaskSendFeishu',
                args={
                    'state': feishu_client.STATE.DONE,
                }
            )

            task_1_graph = {
                task_eip_send_feishu_init: [],
                task_disassociate_eip: [task_delete_slb],
                task_delete_slb: [task_eip_send_feishu_done],
                task_eip_send_feishu_done: [],
            }
            await save_task_graph(job_1_id, task_1_graph)

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    async def freeze_cluster(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if cluster.status != ClusterModel.STATUS.ACTIVE:
            raise Exception(f'Cannot freeze cluster, status {cluster.status}')

        logger.info(self, f'===freeze_cluster {cluster_id}')

        job = JobModel(name='freeze_cluster', status=JobModel.STATUS.Init, cluster_id=cluster_id)
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
        }

        await set_job_context(job_id, self.context)

        task_send_feishu_init = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.INIT,
            }
        )

        task_milestone_1 = TaskModel(name='TaskUpdateCluster',
                                     args={
                                         'status': ClusterModel.STATUS.FREEZING,
                                     })

        task_gringotts_freeze_cluster = TaskModel(name='TaskGringottsFreezeCluster')

        task_stop_instance = TaskModel(name='TaskStopInstance')

        task_detach_ebs = TaskModel(name='TaskDetachEbs')

        task_milestone_2 = TaskModel(name='TaskUpdateCluster',
                                     args={
                                         'status': ClusterModel.STATUS.FREEZE,
                                     })

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_milestone_1: [task_gringotts_freeze_cluster],
            task_gringotts_freeze_cluster: [task_stop_instance],
            task_stop_instance: [task_detach_ebs],
            task_detach_ebs: [task_milestone_2],
            task_milestone_2: [task_send_feishu_done],
            task_send_feishu_done: [],
        }

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    async def unfreeze_cluster(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if cluster.status != ClusterModel.STATUS.FREEZE:
            raise Exception(f'Cannot unfreeze cluster, cluster status: {cluster.status}')

        logger.info(self, f'===unfreeze_cluster {cluster_id}')

        job = JobModel(name='unfreeze_cluster', status=JobModel.STATUS.Init, cluster_id=cluster_id)
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
        }

        await set_job_context(job_id, self.context)

        task_send_feishu_init = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.INIT,
            }
        )

        task_milestone_1 = TaskModel(name='TaskUpdateCluster',
                                     args={
                                         'status': ClusterModel.STATUS.UNFREEZING,
                                     })

        task_reattach_ebs = TaskModel(name='TaskReattachEbs')

        task_start_instance = TaskModel(name='TaskStartInstance')

        task_gringotts_unfreeze_cluster = TaskModel(name='TaskGringottsUnfreezeCluster')

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
            task_milestone_1: [task_reattach_ebs],
            task_reattach_ebs: [task_start_instance],
            task_start_instance: [task_gringotts_unfreeze_cluster],
            task_gringotts_unfreeze_cluster: [task_milestone_2],
            task_milestone_2: [task_send_feishu_done],
            task_send_feishu_done: [],
        }

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }
