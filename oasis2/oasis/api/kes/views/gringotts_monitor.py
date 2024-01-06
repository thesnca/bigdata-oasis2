from oasis.api import BaseView
from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.utils.convert import dict_snake2camel
from oasis.utils.convert import snake2camel
from oasis.utils.logger import logger
from oasis.utils.sdk import gringotts_monitor_client


class GringottsMonitorView(BaseView):
    """
        routes.append(('/ListMetrics', GringottsMonitorView))
        routes.append(('/ListClusterStatus', GringottsMonitorView))
        routes.append(('/GetClusterStatus', GringottsMonitorView))
    """

    def __getattr__(self, http_method):
        def __inner(*args, **kwargs):
            async def __pre_check():
                final_res = await getattr(BaseView, '__getattr__')(self, http_method)(*args, **kwargs)

                if http_method in ['get', 'post', 'put', 'delete']:
                    return final_res

                # To avoid exception
                kwargs.pop('token', None)

                cluster_id = kwargs.get('cluster_id', None)
                account_id = self.account_id
                if not cluster_id:
                    raise Exception(f'Please specify cluster id, got {cluster_id}')

                cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
                if not cluster:
                    raise Exception(f'Cluster not found, id {cluster_id}')

                gg_http_method = snake2camel(http_method)
                data = dict_snake2camel(kwargs)

                logger.info(self, f'Passthrough gringotts monitor api {gg_http_method}, params {kwargs}')
                res = await getattr(
                    gringotts_monitor_client,
                    gg_http_method,
                )(token=self.user_token, **data)

                return res

            return __pre_check()

        return __inner
