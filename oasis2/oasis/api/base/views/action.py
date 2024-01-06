from oasis.api import BaseView
from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.utils.logger import logger


class ActionView(BaseView):
    """
        routes.append(('/ModifyClusterBillingInfo', IamView))
    """

    async def modify_cluster_billing_info(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        modify_dict = {}

        expire_time = kwargs.get('expire_time', None)
        if expire_time is None:
            raise Exception(f'Please send expire time.')

        if not expire_time:  # expire_time: ''
            expire_time = None

        modify_dict.setdefault('expire_time', expire_time)

        charge_type = kwargs.get('charge_type', None)

        if charge_type:
            modify_dict.setdefault('charge_type', charge_type)

        logger.info(f'Modify cluster billing info: {modify_dict}')
        await cluster.save(modify_dict)

        return {
            'cluster_id': cluster_id,
            'expire_time': expire_time
        }
