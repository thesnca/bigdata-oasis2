from oasis.api import BaseView
from oasis.api.base.methods import disable_cluster_scale_notification
from oasis.api.base.methods import enable_cluster_scale_notification
from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.notification import NotificationModel
from oasis.worker.tasks.remote import TaskAddIptablesRules
from oasis.worker.tasks.remote import TaskConfigNic


class CubeView(BaseView):
    """
        routes.append(('/ScaleNotification', CubeView))
        routes.append(('/ConfigNic', CubeView))
        routes.append(('/AddIptablesRules', CubeView))
    """

    async def scale_notification(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        enabled = kwargs.get('enabled', None)
        url = kwargs.get('url', None)
        token = kwargs.get('token', None)
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')
        if enabled is None:
            raise Exception(f'Please specify enabled, got {enabled}')
        if not url:
            raise Exception(f'Please specify url, got {url}')
        if not token:
            raise Exception(f'Please specify token, got {token}')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster {cluster_id} not found.')

        query = model_query(NotificationModel)
        query.filter(NotificationModel.cluster_id == cluster_id)
        notifications = await query.query_all()

        if notifications:
            for noti in notifications:
                if noti.url != url:
                    continue
                if enabled:
                    raise Exception(f'Cluster {cluster_id} already have notification with url: {url}.')
                # Disable notification
                await disable_cluster_scale_notification(noti)

        # Enable notification
        if enabled:
            await enable_cluster_scale_notification(cluster_id, url, token)

        return {
            'cluster_id': cluster_id,
            'enabled': enabled,
            'url': url,
            'token': token,
        }

    async def config_nic(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        nic = kwargs.get('nic', None)
        routes = kwargs.get('routes', [])

        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')
        if not nic:
            raise Exception(f'Please specify nic, got {nic}')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster {cluster_id} not found.')

        await TaskConfigNic(args=kwargs).run()
        return kwargs

    async def add_iptables_rules(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        instances = kwargs.get('instances', None)

        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')
        # TODO validate rule
        if not instances:
            raise Exception(f'Please specify instances, got {instances}')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster {cluster_id} not found.')

        await TaskAddIptablesRules(args=kwargs).run()
        return kwargs
