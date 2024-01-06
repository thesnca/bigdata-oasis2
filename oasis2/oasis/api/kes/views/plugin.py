import os
import re

from oasis.api import BaseView
from oasis.api import openapi
from oasis.api.kes.methods import add_plugin
from oasis.api.kes.methods import get_plugin
from oasis.api.kes.methods import list_plugins_from_db
from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.es_plugin import EsPluginModel
from oasis.db.models.job import JobModel
from oasis.db.models.task import TaskModel
from oasis.utils.config import config
from oasis.utils.convert import str2datetime
from oasis.utils.convert import translate_marker_str
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger
from oasis.utils.redlock import lock_cluster
from oasis.utils.sdk import feishu_client
from oasis.utils.sdk import ks3_client
from oasis.worker.planner import save_task_graph
from oasis.worker.tasks import set_job_context


class PluginView(BaseView):
    """
        routes.append(('/GetKs3PresignedUrl', PluginView))
        routes.append(('/ListPlugins', PluginView))  # system default + user define
        routes.append(('/AddUserPlugin', PluginView))
        routes.append(('/InstallUserPlugin', PluginView))
        routes.append(('/UninstallUserPlugin', PluginView))
        routes.append(('/DeleteUserPlugin', PluginView))
    """

    async def get_ks3_presigned_url(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        account_id = self.account_id
        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        obj_key = kwargs.pop('key_name', None)
        if not obj_key:
            raise Exception(f'object key not found, key: {obj_key}')

        public_endpoint = config.get('kes_user_plugin', 'ks3_public_endpoint')
        bucket_name = config.get('kes_user_plugin', 'bucket_name')
        url = f'/{public_endpoint}/{bucket_name}/{obj_key}'

        # No longer need this url
        # presigned_url = await ks3_client.get_presigned_url
        headers = self.headers
        content_type = headers.get('content-type', None)
        _, _, headers = await ks3_client.get_signature_headers(endpoint=public_endpoint, url=url, method='PUT',
                                                               content_type=content_type)

        result = {
            # No longer need this url
            # 'Url': presigned_url,
            'Headers': headers,
        }

        logger.info(self, f'===Headers: {headers}')

        return result

    @openapi
    async def list_plugins(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        # reformat kwargs
        created_before = str2datetime(kwargs.pop('created_before', None))
        created_after = str2datetime(kwargs.pop('created_after', None))

        # for page
        marker_str = kwargs.pop('marker', 'offset=0 & limit=10')
        marker = translate_marker_str(marker_str)
        offset = marker.get('offset', 0)
        limit = marker.get('limit', 10)

        # Name
        filters = kwargs.pop('filters', [])

        count, f_plugins = await list_plugins_from_db(cluster_id, filters, offset, limit,
                                                      created_after=created_after,
                                                      created_before=created_before)

        marker_str = None
        if count > (marker['limit'] + marker['offset']):
            marker_str = 'offset=%d & limit=%d' % (marker['offset'] + marker['limit'], marker['limit'])

        result = {
            'Plugins': f_plugins,
            'Total': count,
            'Marker': marker_str,
        }

        return result

    @openapi
    async def add_user_plugin(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        plugins = kwargs.pop('plugins', [])
        plugin_map = {}

        for plugin in plugins:
            ks3_address = plugin.pop('ks3_address', None)
            plugin_name = plugin.pop('name', None)
            upload_type = plugin.pop('upload_type', None)
            description = plugin.pop('description', None)

            if not ks3_address:
                raise Exception(f'ks3 address is None')

            if not plugin_name:
                raise Exception(f'plugin name is None')

            if plugin_name in plugin_map:
                raise Exception(f'duplicate plugin name {plugin_name}')

            if upload_type is None:
                raise Exception(f'upload type is None')

            if upload_type not in [EsPluginModel.TYPE.DEFAULT_UPLOAD_TYPE,
                                   EsPluginModel.TYPE.KS3_UPLOAD_TYPE]:
                raise Exception(f'upload type is error: {upload_type}')

            if EsPluginModel.TYPE.DEFAULT_UPLOAD_TYPE == upload_type:
                end_point = config.get('infra', 'ks3_endpoint')
                bucket_name = config.get('kes_user_plugin', 'bucket_name')
                plugin_path = config.get('kes_user_plugin', 'plugin_path')
                ks3_address = f'{end_point}/{bucket_name}/{plugin_path}/{cluster_id}/{ks3_address}'
            elif EsPluginModel.TYPE.KS3_UPLOAD_TYPE == upload_type:
                ks3_address = ks3_address.split('//')[1] if '//' in ks3_address else ks3_address

            file_name, file_suffix = os.path.splitext(ks3_address.split('/')[-1])
            if not re.match(r'^[A-Za-z0-9.-]{8,128}$', file_name):
                raise Exception(f'file must contain characters or number or . or -, length is 8-128: {file_name}')
            if file_name.lower().find('sql') != -1:
                raise Exception(u'目前不支持安装自定义的SQL插件')
            if file_suffix not in ['.zip']:
                raise Exception(f'file suffix must be .zip: {file_suffix}')

            plugin_obj = await get_plugin(cluster_id, plugin_name, file_name)
            if plugin_obj:
                raise Exception(f'Plugin already exist, name {plugin_name}')

            plugin_map.setdefault(plugin_name, {
                'name': plugin_name,
                'type': upload_type,
                'ks3_address': ks3_address,
                'description': description,
            })

        for plugin_name, values in plugin_map.items():
            plugin_id = await add_plugin(cluster_id, plugin_name, values.get('type'),
                                         values.get('ks3_address'), values.get('description'))
            values.setdefault('id', plugin_id)

        return {
            'cluster_id': cluster_id,
            'plugins': [p.get('id', '') for p in plugin_map.values()]
        }

    @openapi
    async def install_user_plugin(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if cluster.status != ClusterModel.STATUS.ACTIVE:
            raise Exception(f'Cannot install user plugin, cluster status: {cluster.status}')

        plugins = kwargs.get('plugins', None)
        if not plugins:
            raise Exception(f'Please specify plugins, got {plugins}')

        plugin_objs = []
        for plugin_id in plugins:
            if not plugin_id:
                raise Exception(f'Please specify plugin id, got {plugin_id}')

            plugin_obj = await get_model_by_id(EsPluginModel, plugin_id)
            if not plugin_obj:
                raise Exception(f'Plugin not found, id {plugin_id}')

            if plugin_obj.plugin_type == EsPluginModel.SOURCE.SYSTEM_DEFAULT_PLUGIN:
                raise Exception(f'Cannot install system default plugin')

            if plugin_obj.status != EsPluginModel.STATUS.UNINSTALL_STATUS:
                raise Exception(f'Cannot install plugin, plugin status: {plugin_obj.status}')

            plugin_objs.append(plugin_obj)

        job = JobModel(name='install_user_plugin', status=JobModel.STATUS.Init, cluster_id=cluster_id)
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
            'cluster_type': 'KES',
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

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_send_feishu_done: []
        }

        # 按插件顺序，组装task，方便下面顺序执行task
        plugin_tasks = []
        for plugin_obj in plugin_objs:
            task_update_user_plugin_status2installing = TaskModel(
                name='TaskUpdateUserPlugin',
                rollback_on_fail=True,
                args={
                    'result': True,
                    'plugin_id': plugin_obj.id,
                    'status': EsPluginModel.STATUS.INSTALLING_STATUS,
                }
            )

            task_gringotts_install_user_plugin = TaskModel(
                name='TaskGringottsInstallUserPlugin',
                args={
                    'plugin_id': plugin_obj.id,
                    'ks3_plugin_address': plugin_obj.ks3_address
                }
            )

            task_update_user_plugin_status2install = TaskModel(
                name='TaskUpdateUserPlugin',
                rollback_on_fail=True,
                args={
                    'plugin_id': plugin_obj.id,
                    'status': EsPluginModel.STATUS.INSTALL_STATUS,
                }
            )

            # TODO: 安装完插件后，先不删除临时ks3插件，因为脚本还会去下载使用，如扩容等...等用户删除插件操作后，再删除...
            # task_delete_ks3_user_plugin = TaskModel(name='TaskDeleteKs3UserPlugin',
            #     rollback_on_fail=True,
            #     args={
            #         'plugin_id': plugin_obj.id
            #     }
            # )

            plugin_tasks.append(
                (
                    task_update_user_plugin_status2installing,
                    task_gringotts_install_user_plugin,
                    task_update_user_plugin_status2install,
                    # task_delete_ks3_user_plugin,
                )
            )

        # 批量操作按顺序执行，即操作完一个插件，再操作另一个插件，gg只能同时执行一个task
        for index, task_tuple in enumerate(plugin_tasks):
            task_graph.setdefault(task_tuple[0], [task_tuple[1]])
            task_graph.setdefault(task_tuple[1], [task_tuple[2]])
            # task_graph.setdefault(task_tuple[2], [task_tuple[3]])
            if index < len(plugin_tasks) - 1:
                # task_graph.setdefault(task_tuple[3], [plugin_tasks[index+1][0]])
                task_graph.setdefault(task_tuple[2], [plugin_tasks[index + 1][0]])
            else:
                # task_graph.setdefault(task_tuple[3], [task_send_feishu_done])
                task_graph.setdefault(task_tuple[2], [task_send_feishu_done])

        logger.info(self, f'====install_user_plugin: {task_graph}')

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    @openapi
    async def uninstall_user_plugin(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if cluster.status != ClusterModel.STATUS.ACTIVE:
            raise Exception(f'Cannot restart cluster, cluster status: {cluster.status}')

        plugins = kwargs.get('plugins', None)
        if not plugins:
            raise Exception(f'Please specify plugins, got {plugins}')

        plugin_objs = []
        for plugin_id in plugins:
            if not plugin_id:
                raise Exception(f'Please specify plugin id, got {plugin_id}')

            plugin_obj = await get_model_by_id(EsPluginModel, plugin_id)
            if not plugin_obj:
                raise Exception(f'Plugin not found, id {plugin_id}')

            if plugin_obj.plugin_type == EsPluginModel.SOURCE.SYSTEM_DEFAULT_PLUGIN:
                raise Exception(f'Cannot delete system default plugin')

            if plugin_obj.status != EsPluginModel.STATUS.INSTALL_STATUS:
                raise Exception(f'Cannot uninstall plugin, plugin status: {plugin_obj.status}')

            plugin_objs.append(plugin_obj)

        job = JobModel(name='uninstall_user_plugin', status=JobModel.STATUS.Init, cluster_id=cluster_id)
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
            'cluster_type': 'KES',
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

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_send_feishu_done: []
        }

        # 按插件顺序，组装task，方便下面顺序执行task
        plugin_tasks = []
        for plugin_obj in plugin_objs:
            task_update_user_plugin_status2uninstalling = TaskModel(
                name='TaskUpdateUserPlugin',
                rollback_on_fail=True,
                args={
                    'result': True,
                    'plugin_id': plugin_obj.id,
                    'status': EsPluginModel.STATUS.UNINSTALLING_STATUS,
                }
            )

            task_gringotts_uninstall_user_plugin = TaskModel(
                name='TaskGringottsUninstallUserPlugin',
                args={
                    'plugin_id': plugin_obj.id,
                    'ks3_plugin_address': plugin_obj.ks3_address
                }
            )

            task_update_user_plugin_status2uninstall = TaskModel(
                name='TaskUpdateUserPlugin',
                rollback_on_fail=True,
                args={
                    'plugin_id': plugin_obj.id,
                    'status': EsPluginModel.STATUS.UNINSTALL_STATUS,
                }
            )

            plugin_tasks.append(
                (
                    task_update_user_plugin_status2uninstalling,
                    task_gringotts_uninstall_user_plugin,
                    task_update_user_plugin_status2uninstall,
                )
            )

        # 批量操作按顺序执行，即操作完一个插件，再操作另一个插件，gg只能同时执行一个task
        for index, task_tuple in enumerate(plugin_tasks):
            task_graph.setdefault(task_tuple[0], [task_tuple[1]])
            task_graph.setdefault(task_tuple[1], [task_tuple[2]])
            if index < len(plugin_tasks) - 1:
                task_graph.setdefault(task_tuple[2], [plugin_tasks[index + 1][0]])
            else:
                task_graph.setdefault(task_tuple[2], [task_send_feishu_done])

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }

    @openapi
    async def delete_user_plugin(self, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)
        account_id = self.account_id
        if not cluster_id:
            raise Exception(f'Please specify cluster id, got {cluster_id}')

        cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if cluster.status != ClusterModel.STATUS.ACTIVE:
            raise Exception(f'Cannot restart cluster, cluster status: {cluster.status}')

        plugins = kwargs.get('plugins', None)
        if not plugins:
            raise Exception(f'Please specify plugins, got {plugins}')

        plugin_objs = []
        for plugin_id in plugins:
            if not plugin_id:
                raise Exception(f'Please specify plugin id, got {plugin_id}')

            plugin_obj = await get_model_by_id(EsPluginModel, plugin_id)
            if not plugin_obj:
                raise Exception(f'Plugin not found, id {plugin_id}')

            if plugin_obj.plugin_type == EsPluginModel.SOURCE.SYSTEM_DEFAULT_PLUGIN:
                raise Exception(f'Cannot delete system default plugin')

            if plugin_obj.status != EsPluginModel.STATUS.UNINSTALL_STATUS:
                raise Exception(f'Cannot delete plugin, plugin status: {plugin_obj.status}')

            plugin_objs.append(plugin_obj)

        job = JobModel(name='delete_user_plugin', status=JobModel.STATUS.Init, cluster_id=cluster_id)
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
            'cluster_type': 'KES',
            'account_id': self.account_id,
            'tenant_id': self.tenant_id,
            'auth_token': self.user_token,
            'plugins': plugins
        }

        await set_job_context(job_id, self.context)

        task_send_feishu_init = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.INIT,
            }
        )

        task_send_feishu_done = TaskModel(
            name='TaskSendFeishu',
            args={
                'state': feishu_client.STATE.DONE,
            }
        )

        task_graph = {
            task_send_feishu_init: [],
            task_send_feishu_done: []
        }

        # 按插件顺序，组装task，方便下面顺序执行task
        plugin_tasks = []
        for plugin_obj in plugin_objs:
            task_update_user_plugin_status2deleting = TaskModel(
                name='TaskUpdateUserPlugin',
                rollback_on_fail=True,
                args={
                    'result': True,
                    'plugin_id': plugin_obj.id,
                    'status': EsPluginModel.STATUS.DELETING_STATUS,
                }
            )

            task_gringotts_delete_user_plugin = TaskModel(
                name='TaskGringottsDeleteUserPlugin',
                args={
                    'plugin_id': plugin_obj.id,
                    'ks3_plugin_address': plugin_obj.ks3_address
                }
            )

            task_update_user_plugin_status2delete = TaskModel(
                name='TaskUpdateUserPlugin',
                rollback_on_fail=True,
                args={
                    'plugin_id': plugin_obj.id,
                    'status': EsPluginModel.STATUS.DELETE_STATUS,
                }
            )

            task_delete_ks3_user_plugin = TaskModel(
                name='TaskDeleteKs3UserPlugin',
                rollback_on_fail=True,
                args={
                    'plugin_id': plugin_obj.id
                }
            )

            plugin_tasks.append(
                (
                    task_update_user_plugin_status2deleting,
                    task_gringotts_delete_user_plugin,
                    task_update_user_plugin_status2delete,
                    task_delete_ks3_user_plugin,
                )
            )

        # 批量操作按顺序执行，即操作完一个插件，再操作另一个插件，gg只能同时执行一个task
        for index, task_tuple in enumerate(plugin_tasks):
            task_graph.setdefault(task_tuple[0], [task_tuple[1]])
            task_graph.setdefault(task_tuple[1], [task_tuple[2]])
            task_graph.setdefault(task_tuple[2], [task_tuple[3]])
            if index < len(plugin_tasks) - 1:
                task_graph.setdefault(task_tuple[3], [plugin_tasks[index + 1][0]])
                # task_graph.setdefault(task_tuple[2], [plugin_tasks[index+1][0]])
            else:
                task_graph.setdefault(task_tuple[3], [task_send_feishu_done])
                # task_graph.setdefault(task_tuple[2], [task_send_feishu_done])

        await save_task_graph(job_id, task_graph)
        await job.save({'status': JobModel.STATUS.Doing})

        return {
            'cluster_id': cluster_id,
            'job_id': job_id,
        }
