from oasis.api import BaseView
from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.eip import EIPModel
from oasis.db.models.instance import InstanceModel
from oasis.db.models.instance_group import InstanceGroupModel
from oasis.db.models.user import UserModel
from oasis.utils import sdk
from oasis.utils.chaos import CLUSTER_STATUS_CONVERT_MAP
from oasis.utils.convert import convert_status
from oasis.utils.sdk import neutron_client
from oasis.worker.tasks import get_rolled_instance


class QueryView(BaseView):
    """
        routes.append(('/VerifyUserPermissions', QueryView))
        routes.append(('/DescribeUser', QueryView))
        routes.append(('/DescribeVpcs', QueryView))
        routes.append(('/DescribeSubnets', QueryView))
        routes.append(('/DescribeEips', QueryView))
        routes.append(('/DescribeEipAddresses', QueryView))
        routes.append(('/DescribeSecurityGroups', QueryView))
        routes.append(('/ListClusterOrderInstances', QueryView))
        routes.append(('/GetEsAddress', QueryView))
        routes.append(('/FetchClusterByExtraInstance', QueryView))
        routes.append(('/CheckInstanceItems', QueryView))
    """

    async def verify_user_permissions(self, *args, **kwargs):
        return {
            'is_verify': True,
            'cluster_types': [],
        }

    async def describe_user(self, *args, **kwargs):
        kuser_id = kwargs.get('kuser_id', None)
        if not kuser_id:
            raise Exception(f'Please specify KUserId')
        query = model_query(UserModel)
        query = query.filter(UserModel.id == kuser_id)
        user_model = await query.query_one()
        if not user_model:
            raise Exception(f'User not found, id {kuser_id}')
        return user_model

    async def describe_vpcs(self, *args, **kwargs):
        vpc_ids = kwargs.get('vpc_ids', [])
        # if not vpc_ids:
        #     raise Exception('Please specify vpc ids')
        vpc_client = getattr(sdk, f'vpc_client_{self.product}')
        vpcs = await vpc_client.describe_vpcs(vpc_ids, account_id=self.account_id)
        return {'vpcs': vpcs}

    async def describe_subnets(self, *args, **kwargs):
        vpc_ids = kwargs.get('vpc_ids', [])
        subnets_ids = kwargs.get('subnet_ids', [])
        availability_zone = kwargs.get('availability_zone', None)
        # if not vpc_ids:
        #     raise Exception('Please specify vpc ids')
        vpc_client = getattr(sdk, f'vpc_client_{self.product}')
        subnets = await vpc_client.describe_subnets(vpc_ids, subnets_ids,
                                                    availability_zone, account_id=self.account_id)
        return {'subnets': subnets}

    async def describe_eips(self, *args, **kwargs):
        project_id = kwargs.get('project_id', None)
        eip_client = getattr(sdk, f'eip_client_{self.product}')
        eips = await eip_client.list_addresses(project_id, account_id=self.account_id)
        return {'eips': eips}

    async def describe_eip_addresses(self, *args, **kwargs):
        project_id = kwargs.get('project_id', None)
        eip_client = getattr(sdk, f'eip_client_{self.product}')
        account_id = self.account_id

        addresses_lists = await eip_client.list_addresses(project_id, account_id=account_id)
        lines_lists = await eip_client.list_getlines(account_id=account_id)
        ret = []
        if not lines_lists:
            return ret

        line_dict = {
            line.get('LineId', None): {
                'LineName': line.get('LineName', None),
                'LineType': line.get('LineType', None),
            }
            for line in lines_lists
        }

        for addresses_list in addresses_lists:
            line_id = addresses_list.get('LineId', None)
            line_name = None
            line_type = None
            if line_id in line_dict:
                line_name = line_dict.get(line_id).get('LineName')
                line_type = line_dict.get(line_id).get('LineType')

            ret.append({
                'LineName': line_name,
                'LineId': line_id,
                'LineType': line_type,
                'PublicIp': addresses_list.get('PublicIp', None),
                'BandWidth': addresses_list.get('BandWidth', None),
                'ProjectId': addresses_list.get('ProjectId', None),
                'State': addresses_list.get('State', None),
                'AllocationId': addresses_list.get('AllocationId', None),
                'IpVersion': addresses_list.get('IpVersion', None),
                'InstanceType': addresses_list.get('InstanceType', None),
                'InstanceId': addresses_list.get('InstanceId', None)
            })

        return {
            'getlines': ret,
        }

    async def describe_security_groups(self, *args, **kwargs):
        vpc_ids = kwargs.get('vpc_ids', [])

        vpc_client = getattr(sdk, f'vpc_client_{self.product}')
        security_groups = await vpc_client.describe_security_groups(vpc_ids, account_id=self.account_id)

        available_security_groups = []
        for security_group in security_groups:
            available = await neutron_client.check_security_group_rules(
                security_group.get('SecurityGroupId'),
                self.product.upper(),
                # add_kmr_tag=True,
                tenant_id=self.tenant_id)
            if not available:
                continue
            available_security_groups.append(security_group)

        return {
            'security_groups': available_security_groups,
        }

    async def list_cluster_order_instances(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        order_ids = []
        kec_ids = []
        epc_ids = []
        ebs_ids = []
        service_ids = []

        for ig in cluster.instance_groups:
            resource_type = ig.resource_type
            for instance in ig.instances:
                service_ids.append(instance.service_instance_id)
                if resource_type == 'KEC':
                    kec_ids.append(instance.instance_id)
                elif resource_type == 'EPC':
                    epc_ids.append(instance.instance_id)

                volumes = instance.volumes
                if volumes:
                    ebs_ids.extend(volumes)

        if kec_ids:
            order_ids.append({
                'Type': 'KEC',
                'Ids': kec_ids,
            })
        if epc_ids:
            order_ids.append({
                'Type': 'EPC',
                'Ids': epc_ids,
            })
        if ebs_ids:
            order_ids.append({
                'Type': 'EBS',
                'Ids': ebs_ids,
            })
        if service_ids:
            order_ids.append({
                'Type': 'SERVICE',
                'Ids': service_ids,
            })

        if cluster.enable_eip or cluster.enable_private_slb:
            eip_query = model_query(EIPModel).filter(
                EIPModel.cluster_id == cluster_id,
                EIPModel.status != EIPModel.STATUS.DELETED)
            eips = await eip_query.query_all()
            for eip in eips:
                if eip.status == 'Bind' and eip.allocate_address_id:
                    order_ids.append({
                        'Type': 'EIP',
                        'Ids': eip.allocate_address_id
                    })
                order_ids.append({
                    'Type': 'SLB',
                    'Ids': eip.load_balancer_id,
                })

        return {'order_instances': order_ids}

    async def get_es_address(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        inner_user = kwargs.get('inner_user', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        vpc_list = []
        bind_list = []
        bind_status = None

        for ig in cluster.instance_groups:
            for ins in ig.instances:
                ins_id = ins.instance_id
                name = ins.instance_name
                internal_ip = ins.internal_ip
                inner_manager_ip = ins.inner_manager_ip
                vpc_list.append({
                    'id': ins_id,
                    'name': name,
                    'vpc_path': f'{internal_ip}:9200',
                })
                if inner_manager_ip:
                    bind_list.append({
                        'id': ins_id,
                        'name': name,
                        'bind_path': f'{inner_manager_ip}',
                    })
        if cluster.extra:
            bind_status = cluster.extra.get('bind_eip_status', None)

        res = {
            'vpc_list': vpc_list,
        }
        if inner_user:
            res.update({
                'bind_list': bind_list,
                'bind_eip_status': bind_status,
            })
        return res

    async def fetch_cluster_by_extra_instance(self, *args, **kwargs):
        extra_instance_id = kwargs.get('extra_instance_id', None)

        query = model_query(InstanceModel)
        query = query.filter(InstanceModel.service_instance_id == extra_instance_id)
        instance = await query.query_one()

        if not instance:
            instance_status = 'NOT_FOUND'
            cluster_status = 'NOT_FOUND'
            # Get rolled instance from redis
            cluster_id = await get_rolled_instance(extra_instance_id)

            if cluster_id:
                instance_status = 'ROLLED'
                cluster = await get_model_by_id(ClusterModel, cluster_id)
                if cluster:
                    cluster_status = convert_status(CLUSTER_STATUS_CONVERT_MAP, cluster.status)
                else:
                    cluster_status = 'ROLLED'

            return {
                'cluster_id': None,
                'cluster_name': None,
                'instance_status': instance_status,
                'extra_instance_id': extra_instance_id,
                'cluster_status': cluster_status,
            }

        instance_group_id = instance.instance_group_id
        instance_group = await get_model_by_id(InstanceGroupModel, instance_group_id)

        cluster = await get_model_by_id(ClusterModel, instance_group.cluster_id)

        return {
            'cluster_id': instance_group.cluster_id,
            'cluster_name': cluster.name,
            'instance_status': instance.status,
            'extra_instance_id': extra_instance_id,
            'status': convert_status(CLUSTER_STATUS_CONVERT_MAP, cluster.status),
        }

    async def check_instance_items(self, *args, **kwargs):
        extra_instance_id = kwargs.get('instance_id', None)

        query = model_query(InstanceModel)
        query = query.filter(InstanceModel.service_instance_id == extra_instance_id)
        instance = await query.query_one()

        if not instance:
            return None

        instance_group_id = instance.instance_group_id
        instance_group = await get_model_by_id(InstanceGroupModel, instance_group_id)

        return {
            'node_group_type': instance_group.instance_group_type,
            # for EPC instance_type_code, CAL-II-ES.normal.4C4G ==> CAL-II
            'flavor_name': '-'.join(instance_group.instance_type_code.split('-')[:-1]),
        }
