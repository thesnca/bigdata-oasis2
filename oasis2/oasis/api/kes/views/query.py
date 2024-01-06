from oasis.api import BaseView
from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.utils.chaos import CLUSTER_STATUS_CONVERT_MAP
from oasis.utils.chaos import DISTRIBUTION_SCHEMAS
from oasis.utils.convert import convert_status
from oasis.utils.sdk import gringotts_client
from oasis.utils.sdk import neutron_client


class QueryView(BaseView):
    """
        routes.append(('/ListDistributions', QueryView))
        routes.append(('/ListComprehensiveStatus', QueryView))
        routes.append(('/CheckSecurityGroup', QueryView))
    """

    async def list_distributions(self, *args, **kwargs):
        cluster_type = kwargs.get('cluster_type', None)
        if not cluster_type:
            raise Exception(f'Please specify cluster type, got {cluster_type}')
        service_dict = DISTRIBUTION_SCHEMAS.get(cluster_type.upper(), {})
        res = []
        for service_version in service_dict:
            template = {
                'distribution': service_version,
                'product_version': service_dict[service_version]['version'],
                'main_version': service_dict[service_version]['main_version'],
                'plugins': service_dict[service_version]['plugins'],
            }
            res.append(template)
        return {'distributions': res}

    async def list_comprehensive_status(self, *args, **kwargs):
        cluster_ids = kwargs.get('cluster_ids', None)
        account_id = self.account_id

        if not cluster_ids:
            raise Exception(f'Please specify cluster ids, got {cluster_ids}')

        res = {}
        gg_cluster_list = []
        for cluster_id in cluster_ids:
            cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
            if not cluster:
                raise Exception(f'Cluster {cluster_id} not found.')

            if cluster.status not in [ClusterModel.STATUS.FREEZE,
                                      ClusterModel.STATUS.DELETED,
                                      ClusterModel.STATUS.ERROR]:
                gg_cluster_list.append(cluster.id)

            res.setdefault(cluster.id, cluster.status)

        gg_results = await gringotts_client.list_services_idle(gg_cluster_list, token=self.user_token)
        for gg_result in gg_results:
            cid = gg_result.get('ClusterId', '')
            if cid and not gg_result.get('IsIdle', True):
                res.update({cid: ClusterModel.STATUS.PROGRESSING})

        comprehensive = [{
            'cluster_id': cluster_id,
            'comprehensive_status': convert_status(CLUSTER_STATUS_CONVERT_MAP, status)}
            for cluster_id, status in res.items()]
        return {
            'comprehensive': comprehensive
        }

    async def check_security_group(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        sg_id = cluster.security_group_id
        vpc_domain_id = cluster.vpc_domain_id
        cluster_type = cluster.cluster_type

        # 老集群
        if not sg_id:
            sec_group_name = f'KSC{cluster_type}'
            sec_group_description = f'{cluster_type}-{cluster.id}'
            sg = await neutron_client.get_or_create_sec_group(sec_group_name,
                                                              vpc_domain_id,
                                                              sec_group_description,
                                                              cluster_type,
                                                              tenant_id=self.tenant_id
                                                              )
            sg_id = sg.get('id', None)

        else:
            sg = await neutron_client.get_security_group_by_id(
                sg_id,
                # add_kmr_tag=True,
                tenant_id=self.tenant_id)
            if not sg:
                return {
                    'security_group_id': sg_id,
                    'security_group_name': None,
                    'description': None,
                    'is_available': False,
                    'msg': f'Security group does not exist, id: {sg_id}',
                }
        sg_name = sg.get('name', None)
        description = sg.get('description', None)
        msg = None

        # 判断是否可用
        available = await neutron_client.check_security_group_rules(
            sg_id, cluster.cluster_type,
            # add_kmr_tag=True,
            tenant_id=self.tenant_id)
        if not available:
            msg = f'Security group rules is incorrect, id: {sg_id}'

        return {
            'verification': {
                'security_group_id': sg_id,
                'security_group_name': sg_name,
                'description': description,
                'is_available': available,
                'msg': msg,
            },
        }
