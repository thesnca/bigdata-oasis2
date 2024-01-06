from sqlalchemy.sql.elements import and_

from conf.charge_conf import PRODUCT_GROUP_MAP
from conf.charge_conf import product_code_map
from oasis.api import BaseView
from oasis.api.base.methods import get_bind_eip
from oasis.api.base.methods import get_unbind_eip
from oasis.api.base.methods import op_list_clusters_from_db
from oasis.api.base.methods import op_list_jobs_from_db
from oasis.api.base.methods import op_list_users_from_db
from oasis.api.base.results.operation_summary import OpClusterSummary
from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.instance import InstanceModel
from oasis.db.models.instance_group import InstanceGroupModel
from oasis.db.models.job import JobModel
from oasis.db.models.user import UserModel
from oasis.utils import sdk
from oasis.utils.chaos import OP_CLUSTER_STATUS_CONVERT_MAP
from oasis.utils.convert import str2datetime
from oasis.utils.convert import translate_marker_str
from oasis.utils.logger import logger
from oasis.utils.sdk import charge_client
from oasis.utils.sdk.iam import get_user_ak_sk_by_id
from oasis.utils.sdk.iam import get_user_by_id


class OperationView(BaseView):
    """
        routes.append(('/OpCreateUser', OperationView))
        routes.append(('/OpDeleteUser', OperationView))
        routes.append(('/OpUpdateUser', OperationView))
        routes.append(('/OpDescribeUser', OperationView))
        routes.append(('/OpListUsers', OperationView))
        routes.append(('/OpVerifyUserPermissions', OperationView))
        routes.append(('/OpListClusters', OperationView))
        routes.append(('/OpListClusterJobs', OperationView))
        routes.append(('/OpDescribeCluster', OperationView))
        routes.append(('/OpListClusterNodes', OperationView))
        routes.append(('/OpQueryOrder', OperationView))
        routes.append(('/OpNotifyOrder', OperationView))
        routes.append(('/OpControlHttpReferer', OperationView))
        routes.append(('/OpModifyCluster', OperationView))
        routes.append(('/OpStartInstance', OperationView))
    """

    async def op_verify_user_permissions(self, *args, **kwargs):
        kuser_id = kwargs.get('kuser_id', None)
        if not kuser_id:
            raise Exception(f'Please specify KUserId')
        if await get_user_ak_sk_by_id(kuser_id, self.product) == None:
            return {
                'is_verify': False,
            }
        return {
            'is_verify': True,
        }

    async def op_create_user(self, *args, **kwargs):
        kuser_id = kwargs.get('kuser_id', None)
        if not kuser_id:
            raise Exception(f'Please specify KUserId')
        user_model = await get_model_by_id(UserModel, kuser_id, account_id=self.account_id)
        if user_model:
            raise Exception(f'User has created, id {kuser_id}')
        user = UserModel()
        if kuser_id:
            user.id = kuser_id
        product = kwargs.get('product', 'kes')
        res = await get_user_by_id(kuser_id, product)
        tenant_id = res.get('tenant_id', None)
        if tenant_id:
            user.tenant_id = tenant_id
        else:
            raise Exception(f'Add user failed, User {kuser_id} does not have tenant id.')
        company_alias = kwargs.get('company_alias', None)
        if company_alias:
            user.company_alias = company_alias

        user_level = kwargs.get('user_level', None)
        if user_level:
            user.user_level = user_level

        total_virtual_cpu = kwargs.get('total_virtual_cpu', None)
        if total_virtual_cpu:
            user.total_virtual_cpu = total_virtual_cpu

        total_mem_mb = kwargs.get('total_mem_mb', None)
        if total_mem_mb:
            user.total_mem_mb = total_mem_mb

        total_disk_gb = kwargs.get('total_disk_gb', None)
        if total_disk_gb:
            user.total_disk_gb = total_disk_gb

        logger.info(f'Create User info: {user}')
        await user.save()
        return user

    async def op_delete_user(self, *args, **kwargs):
        kuser_id = kwargs.get('kuser_id', [])
        if not kuser_id:
            raise Exception(f'Please specify KUserId')
        for k_id in kuser_id:
            query = model_query(UserModel)
            query = query.filter(UserModel.id == k_id)
            user_model = await query.query_one()
            if not user_model:
                raise Exception(f'User not found, id {k_id}')
            query_cluster = model_query(ClusterModel)
            query_cluster = query_cluster.filter(
                and_(ClusterModel.ksc_user_id == k_id, ClusterModel.status != 'Deleted'))
            count, cluster_model = await query_cluster.query_all(count=True)
            if count != 0:
                raise Exception(f'User {k_id} has {count} clusters')
            await user_model.delete()
        return {
            'kuser_id': kuser_id,
        }

    async def op_update_user(self, *args, **kwargs):

        kuser_id = kwargs.get('kuser_id', None)
        account_id = self.account_id
        user = await get_model_by_id(UserModel, kuser_id, account_id=account_id)
        user_dict = {}
        if not user:
            raise Exception(f'Please specify user Exited, got {kuser_id}')
        company_alias = kwargs.get('company_alias', None)
        if company_alias:
            user_dict.setdefault('company_alias', company_alias)

        user_level = kwargs.get('user_level', None)
        if user_level:
            user_dict.setdefault('user_level', user_level)

        total_virtual_cpu = kwargs.get('total_virtual_cpu', None)
        if total_virtual_cpu:
            user_dict.setdefault('total_virtual_cpu', total_virtual_cpu)

        total_mem_mb = kwargs.get('total_mem_mb', None)
        if total_mem_mb:
            user_dict.setdefault('total_mem_mb', total_mem_mb)

        total_disk_gb = kwargs.get('total_disk_gb', None)
        if total_disk_gb:
            user_dict.setdefault('total_disk_gb', total_disk_gb)

        user = await user.save(user_dict)
        return user

    async def op_describe_user(self, *args, **kwargs):
        kuser_id = kwargs.get('kuser_id', None)
        if not kuser_id:
            raise Exception(f'Please specify KUserId')
        query = model_query(UserModel)
        query = query.filter(UserModel.id == kuser_id)
        user_model = await query.query_one()
        if not user_model:
            raise Exception(f'User not found, id {kuser_id}')
        return user_model

    async def op_list_users(self, *args, **kwargs):
        account_id = self.account_id
        # reformat kwargs
        created_before = str2datetime(kwargs.pop('created_before', None))
        created_after = str2datetime(kwargs.pop('created_after', None))
        company_alias = kwargs.pop('company_alias', None)

        marker_str = kwargs.pop('marker', 'offset=0 & limit=10')
        marker = translate_marker_str(marker_str)
        offset = marker.get('offset', 0)
        limit = marker.get('limit', 10)

        # Fuzzy/Name/Id
        filters = kwargs.pop('filters', [])

        count, f_users = await op_list_users_from_db(filters, offset, limit,
                                                     created_after=created_after,
                                                     created_before=created_before,
                                                     company_alias=company_alias,
                                                     account_id=account_id)

        marker_str = None
        if count > (marker['limit'] + marker['offset']):
            marker_str = 'offset=%d & limit=%d' % (marker['offset'] + marker['limit'], marker['limit'])

        result = {
            'Users': f_users,
            'Total': count,
            'Marker': marker_str,
        }
        return result

    async def op_list_clusters(self, *args, **kwargs):
        account_id = self.account_id
        company_alias = kwargs.get('company_alias', None)
        # reformat kwargs
        created_before = str2datetime(kwargs.pop('created_before', None))
        created_after = str2datetime(kwargs.pop('created_after', None))
        expired_before = str2datetime(kwargs.pop('expired_before', None))
        expired_after = str2datetime(kwargs.pop('expired_after', None))

        marker_str = kwargs.pop('marker', 'offset=0 & limit=10')
        marker = translate_marker_str(marker_str)
        offset = marker.get('offset', 0)
        limit = marker.get('limit', 10)

        cluster_status = [sta for status in kwargs.pop('cluster_status', [])
                          for sta in OP_CLUSTER_STATUS_CONVERT_MAP.get(status, ['NonDeleted'])]
        cluster_type = kwargs.pop('cluster_type', [])
        charge_type = kwargs.pop('charge_type', [])

        # Fuzzy/Name/Id
        filters = kwargs.pop('filters', [])

        count, f_clusters = await op_list_clusters_from_db(filters, offset, limit,
                                                           cluster_status=cluster_status,
                                                           cluster_type=cluster_type,
                                                           charge_type=charge_type,
                                                           created_after=created_after,
                                                           created_before=created_before,
                                                           expired_after=expired_after,
                                                           expired_before=expired_before,
                                                           company_alias=company_alias,
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

    async def op_describe_cluster(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')
        query = model_query(ClusterModel)
        query = query.filter(ClusterModel.id == cluster_id)
        cluster_model = await query.query_one()
        company_alias = None
        tmp = await get_model_by_id(UserModel, ClusterModel.ksc_user_id)
        if tmp:
            company_alias = tmp.company_alias
        if not cluster_model:
            raise Exception(f'Cluster not found, id {cluster_id}')

        summary_cluster = OpClusterSummary(cluster_model).__dict__
        if summary_cluster['EnableEip']:
            eip_info = await get_bind_eip(summary_cluster['ClusterId'])
            if eip_info:
                summary_cluster['Eip'] = eip_info.eip_address
                summary_cluster['SlbId'] = eip_info.load_balancer_id
            else:
                slb_info = await get_unbind_eip(summary_cluster['ClusterId'])
                if slb_info:
                    summary_cluster['SlbId'] = slb_info.load_balancer_id
        summary_cluster["company_alias"] = company_alias

        return summary_cluster

    async def op_list_cluster_jobs(self, *args, **kwargs):
        account_id = self.account_id
        cluster_id = kwargs.get('cluster_id', None)
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')
        # reformat kwargs
        created_before = str2datetime(kwargs.pop('created_before', None))
        created_after = str2datetime(kwargs.pop('created_after', None))

        marker_str = kwargs.pop('marker', 'offset=0 & limit=10')
        marker = translate_marker_str(marker_str)
        offset = marker.get('offset', 0)
        limit = marker.get('limit', 10)

        # Fuzzy/Name/Id
        filters = kwargs.pop('filters', [])

        count, f_jobs = await op_list_jobs_from_db(filters, offset, limit,
                                                   created_after=created_after,
                                                   created_before=created_before,
                                                   cluster_id=cluster_id,
                                                   account_id=account_id)

        marker_str = None
        if count > (marker['limit'] + marker['offset']):
            marker_str = 'offset=%d & limit=%d' % (marker['offset'] + marker['limit'], marker['limit'])

        result = {
            'Jobs': f_jobs,
            'Total': count,
            'Marker': marker_str,
        }
        return result

    async def op_list_cluster_nodes(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        cluster_model = await get_model_by_id(ClusterModel, cluster_id)
        query = model_query(JobModel)
        query = query.filter(JobModel.cluster_id == cluster_id).order_by(JobModel.updated_at.desc())
        last_operation = None
        _, job_models = await query.query_all(count=True, limit=1)
        for job_model in job_models:
            last_operation = job_model.name
            break

        main_instance_id = cluster_model.extra.get('main_instance_id', '') if cluster_model.extra else ''
        if not cluster_model:
            raise Exception(f'Cluster not found, id {cluster_id}')

        instance_dec_list = []
        for instance_group in cluster_model.instance_groups:
            instance_list = instance_group.instances
            for i in instance_list:
                i = i.to_dict()
                i['main_instance_id'] = main_instance_id
                i['resource_type'] = instance_group.resource_type
                i['instance_type_code'] = instance_group.instance_type_code
                i['volume_type'] = instance_group.volume_type
                i['volume_count'] = instance_group.volume_count
                i['volume_size'] = instance_group.volume_size
                i['charge_type'] = cluster_model.charge_type
                i['last_operation'] = last_operation
                instance_dec_list.append(i)
        return instance_dec_list

    async def op_query_order(self, *args, **kwargs):
        order_id = kwargs.get('order_id', None)
        sub_order_id = kwargs.get('sub_order_id', None)
        instance_id = kwargs.get('instance_id', None)

        if not order_id:
            if sub_order_id:
                subs = await charge_client.query_sub_orders(sub_order_id)
                for sub in subs:
                    order_id = sub.get('orderId', None)
                    if order_id:
                        break
            elif instance_id:
                ins_dict = await charge_client.query_sub_orders_by_instance_ids([instance_id])
                order_id = ins_dict.get(instance_id, {}).get('orderId', None)
            if not order_id:
                raise Exception(f'Cannot find suborder {sub_order_id}')

        sub_orders = await charge_client.query_sub_orders_by_order_id(order_id)
        res = []
        for so in sub_orders:
            info = await charge_client.get_instance_info(so.get('instanceId'))
            product_group = int(so.get('productGroup', 0))
            product_name = product_code_map.get(product_group, product_group)
            res.append({
                'sub_order_id': so.get('subOrderId'),
                'instance_id': so.get('instanceId'),
                'product_group': product_name,
                'order_status': so.get('status'),
                'instance_status': info.get('status'),
                'start_time': info.get('billingBeginTime', None),
                'end_time': info.get('billingEndTime', None),
            })

        return {
            'order_id': order_id,
            'sub_orders': res,
        }

    async def op_notify_order(self, *args, **kwargs):
        sub_order_id = kwargs.get('sub_order_id', None)
        status = kwargs.get('status', None)
        instance_id = kwargs.get('instance_id', None)
        cluster_type = kwargs.get('cluster_type', None)

        if cluster_type not in ['kes', 'khbase', 'kmr']:
            raise Exception(f'ClusterType invalid, got {cluster_type}')

        if not sub_order_id:
            raise Exception(f'SubOrderId invalid, got {sub_order_id}')

        if status not in [1, 2]:
            raise Exception(f'Status invalid, got {status}')

        subs = await charge_client.query_sub_orders(sub_order_id)
        sub_order = None
        for sub in subs:
            if sub_order_id == sub.get('subOrderId'):
                sub_order = sub
                break
        if not sub_order:
            raise Exception(f'Cannot find sub order {sub_order_id}')

        order_status = sub_order.get('status')
        product_group = sub_order.get('productGroup')
        order_instance_id = sub_order.get('instanceId', None)
        user_id = sub_order.get('userId', None)

        if order_status != 1:
            raise Exception(f'Sub order already in status {order_status}, cannot notify again.')

        if order_instance_id:
            if instance_id and order_instance_id != instance_id:
                raise Exception(f'Order instance id {order_instance_id} does not match instance_id {instance_id}')

            if product_group == PRODUCT_GROUP_MAP['KEC'] and instance_id:
                kec_client = getattr(sdk, f'kec_client_{cluster_type.lower()}')
                await kec_client.notify_suborder_status(instance_id, sub_order_id, status, account_id=user_id)
            elif product_group in [PRODUCT_GROUP_MAP['EPC'], PRODUCT_GROUP_MAP['GEPC']] and instance_id:
                epc_client = getattr(sdk, f'epc_client_{cluster_type.lower()}')
                await epc_client.notify_suborder_status(instance_id, sub_order_id, status, account_id=user_id)
            elif product_group == PRODUCT_GROUP_MAP['EBS'] and instance_id:
                ebs_client = getattr(sdk, f'ebs_client_{cluster_type.lower()}')
                await ebs_client.notify_suborder_status_ebs(instance_id, sub_order_id, status, account_id=user_id)
            elif not instance_id:
                await charge_client.notify_suborder_status(sub_order_id, status)
        else:
            await charge_client.notify_suborder_status(sub_order_id, status)

        return {
            'sub_order_id': sub_order_id,
            'status': status,
            'instance_id': instance_id,
        }

    async def op_control_http_referer(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        status = kwargs.get('status', None)
        if not cluster_id:
            raise Exception(f'Cluster id invalid, got id {cluster_id}')

        if status is None:
            raise Exception(f'Status invalid, got status {status}')

        if isinstance(status, str) and status.lower() in ['0', 'false']:
            status = False
        status = bool(status)

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        for ig in cluster.instance_groups:
            for ins in ig.instances:
                remote = await ins.remote()
                async with remote as conn:
                    _, start_line = await conn.execute(
                        'grep -n "secure_link \\$arg_token,\\$arg_exp;" /opt/nginx/conf/nginx.conf',
                        raise_when_error=False)
                    if not start_line:
                        continue

                    start_num = int(start_line.split(':')[0])
                    end_num = start_num + 7
                    # 开启防盗链
                    if not status:
                        if '#' not in start_line:
                            continue
                        await conn.execute(f'sed -i "{start_num},{end_num} s/# //g" /opt/nginx/conf/nginx.conf')
                    # 关闭防盗链
                    else:
                        if '#' in start_line:
                            continue
                        await conn.execute(f'sed -i "{start_num},{end_num} s/^/# /g" /opt/nginx/conf/nginx.conf')

                    await conn.execute('/opt/nginx/sbin/nginx -s reload', raise_when_error=False)

        return {
            'cluster_id': cluster_id,
            'status': status,
        }

    async def op_modify_cluster(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        name = kwargs.get('cluster_name', None)
        status = kwargs.get('cluster_status', None)
        charge_type = kwargs.get('charge_type', None)
        purchase_time = kwargs.get('purchase_time', None)
        expire_time = kwargs.get('expire_time', None)

        if not cluster_id:
            raise Exception(f'Cluster id invalid, got id {cluster_id}')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        update_dict = {}
        if name:
            update_dict.setdefault('name', name)
        if status:
            if status not in ['Active', 'Deleted']:
                raise Exception(f'Status invalid, got {status}')
            update_dict.setdefault('status', status)
        if charge_type:
            if charge_type == 'Monthly' and not purchase_time:
                raise Exception(f'Charge type Monthly need purchase time, got {purchase_time}')
            update_dict.setdefault('charge_type', charge_type)
            update_dict.setdefault('purchase_time', purchase_time)
        if expire_time:
            if expire_time == 'NULL':
                expire_time = None
            update_dict.setdefault('expire_time', expire_time)

        if not update_dict:
            raise Exception(f'Nothing to modify, got {kwargs}')

        res = await cluster.save(update_dict)

        return res

    async def op_start_instance(self, *args, **kwargs):
        instance_id = kwargs.get('instance_id', None)
        if not instance_id:
            raise Exception(f'Instance id invalid, got id {instance_id}')

        query = model_query(InstanceModel)
        query = query.filter(InstanceModel.instance_id == instance_id)
        instance = await query.query_one()
        if not instance:
            raise Exception(f'Instance not found, id {instance_id}')

        instance_group_id = instance.instance_group_id
        instance_group = await get_model_by_id(InstanceGroupModel, instance_group_id)
        cluster_id = instance_group.cluster_id
        cluster = await get_model_by_id(ClusterModel, cluster_id)

        kec_client = getattr(sdk, f'kec_client_{cluster.cluster_type.lower()}')
        account_id = cluster.ksc_user_id
        res = await kec_client.start_instances(instance_ids=[instance_id], account_id=account_id)

        return res
