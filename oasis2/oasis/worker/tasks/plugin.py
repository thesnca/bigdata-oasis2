from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.es_plugin import EsPluginModel
from oasis.utils.config import config
from oasis.utils.logger import logger
from oasis.utils.sdk import gringotts_client
from oasis.utils.sdk import ks3_client
from oasis.worker.tasks import BaseTask
from oasis.worker.tasks import check_rollback
from oasis.worker.tasks import check_task


class TaskUpdateUserPlugin(BaseTask):
    @check_task
    async def run(self):
        cluster_id = self.args.pop('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        plugin_id = self.args.pop('plugin_id', None)
        if not plugin_id:
            raise Exception(f'Please specify plugin id, got {plugin_id}')

        # Sometimes args send error params
        self.args.pop('request_id', None)
        self.args.pop('cluster_id', None)
        self.args.pop('plugin_id', None)

        plugin = await get_model_by_id(EsPluginModel, plugin_id)
        if not plugin:
            raise Exception(f'Plugin not found, id {plugin_id}')

        result = self.args.pop('result', None)
        if not result:  # 上一个任务操作失败，未安装成功，即未安装状态
            self.args['status'] = EsPluginModel.STATUS.UNINSTALL_STATUS
        await plugin.save(self.args)

        return {'plugin_id': plugin_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskDeleteKs3UserPlugin(BaseTask):
    @check_task
    async def run(self):
        cluster_id = self.args.pop('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        plugin_id = self.args.pop('plugin_id', None)
        if not plugin_id:
            raise Exception(f'Please specify plugin id, got {plugin_id}')

        # Sometimes args send error params
        self.args.pop('request_id', None)
        self.args.pop('cluster_id', None)
        self.args.pop('plugin_id', None)

        plugin = await get_model_by_id(EsPluginModel, plugin_id)
        if not plugin:
            raise Exception(f'Plugin not found, id {plugin_id}')

        # 插件安装完成后：oasis删除临时ks3的zip包
        if (EsPluginModel.TYPE.DEFAULT_UPLOAD_TYPE == plugin.upload_type and
                EsPluginModel.SOURCE.USER_DEFINE_PLUGIN == plugin.plugin_type):
            try:
                plugin_path = config.get('kes_user_plugin', 'plugin_path')
                plugin_name = plugin.ks3_address.split('/')[-1]
                key = f'{plugin_path}/{cluster_id}/{plugin_name}'
                await ks3_client.delete_object_sdk(key)
            except Exception as e:
                logger.error(self, f'===TaskDeleteKs3UserPlugin plugin id: {plugin_id}... Error: {e}')

        return {'plugin_id': plugin_id, 'result': True}

    @check_rollback
    async def rollback(self):
        return True


class TaskDeleteKs3UserPlugins(BaseTask):
    @check_task
    async def run(self):
        cluster_id = self.args.pop('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        plugins = self.args.pop('plugins', [])

        self.args.pop('request_id', None)

        for plugin_id in plugins:
            plugin = await get_model_by_id(EsPluginModel, plugin_id)
            if not plugin:
                logger.error(self, f'Plugin not found, id {plugin_id}')
                continue

            # 插件安装完成后：oasis删除临时ks3的zip包
            if (EsPluginModel.TYPE.DEFAULT_UPLOAD_TYPE == plugin.upload_type and
                    EsPluginModel.SOURCE.USER_DEFINE_PLUGIN == plugin.plugin_type):
                try:
                    plugin_path = config.get('kes_user_plugin', 'plugin_path')
                    plugin_name = plugin.ks3_address.split('/')[-1]
                    key = f'{plugin_path}/{cluster_id}/{plugin_name}'
                    await ks3_client.delete_object_sdk(key)
                except Exception as e:
                    logger.error(self, f'===TaskDeleteKs3UserPlugins plugin id: {plugin_id}... Error: {e}')

        return {'plugins': plugins, 'result': True}

    @check_rollback
    async def rollback(self):
        return True


class TaskGringottsInstallUserPlugin(BaseTask):
    @check_task
    async def run(self):
        token = self.args.pop('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        plugin_id = self.args.pop('plugin_id', None)
        if not plugin_id:
            raise Exception(f'Please specify plugin id, got {plugin_id}')

        plugin = await get_model_by_id(EsPluginModel, plugin_id)
        if not plugin:
            raise Exception(f'Plugin not found, id {plugin_id}')

        ks3_plugin_address = self.args.pop('ks3_plugin_address', None)
        if not ks3_plugin_address:
            raise Exception(f'ks3 plugin address not found, id {ks3_plugin_address}')

        install_args = {
            'cluster_id': cluster_id,
            'plugin_id': plugin_id,
            'ks3_plugin_address': ks3_plugin_address
        }

        try:
            _op_id = await gringotts_client.install_user_plugin(token=token, **install_args)
            result = await gringotts_client.wait_gg_op_active(_op_id, token=token)
        except Exception as e:
            logger.warn(self, f'Gringotts install user plugin failed, error: {e}')
            result = False

        return {'plugin_id': plugin_id, 'result': result}

    @check_rollback
    async def rollback(self):
        return True


class TaskGringottsUninstallUserPlugin(BaseTask):
    @check_task
    async def run(self):
        token = self.args.pop('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        plugin_id = self.args.get('plugin_id', None)
        if not plugin_id:
            raise Exception(f'Please specify plugin id, got {plugin_id}')

        plugin = await get_model_by_id(EsPluginModel, plugin_id)
        if not plugin:
            raise Exception(f'Plugin not found, id {plugin_id}')

        ks3_plugin_address = self.args.pop('ks3_plugin_address', None)
        if not ks3_plugin_address:
            raise Exception(f'ks3 plugin address not found, id {ks3_plugin_address}')

        uninstall_args = {
            'cluster_id': cluster_id,
            'plugin_id': plugin_id,
            'ks3_plugin_address': ks3_plugin_address
        }

        try:
            _op_id = await gringotts_client.uninstall_user_plugin(token=token, **uninstall_args)
            result = await gringotts_client.wait_gg_op_active(_op_id, token=token)
        except Exception as e:
            logger.warn(self, f'Gringotts uninstall user plugin failed, error: {e}')
            result = False

        return {'plugin_id': plugin_id, 'result': result}

    @check_rollback
    async def rollback(self):
        return True


class TaskGringottsDeleteUserPlugin(BaseTask):
    @check_task
    async def run(self):
        token = self.args.pop('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        plugin_id = self.args.get('plugin_id', None)
        if not plugin_id:
            raise Exception(f'Please specify plugin id, got {plugin_id}')

        plugin = await get_model_by_id(EsPluginModel, plugin_id)
        if not plugin:
            raise Exception(f'Plugin not found, id {plugin_id}')

        ks3_plugin_address = self.args.pop('ks3_plugin_address', None)
        if not ks3_plugin_address:
            raise Exception(f'ks3 plugin address not found, id {ks3_plugin_address}')

        delete_args = {
            'cluster_id': cluster_id,
            'plugin_id': plugin_id,
            'ks3_plugin_address': ks3_plugin_address
        }

        try:
            _op_id = await gringotts_client.delete_user_plugin(token=token, **delete_args)
            result = await gringotts_client.wait_gg_op_active(_op_id, token=token)
        except Exception as e:
            logger.warn(self, f'Gringotts delete user plugin failed, error: {e}')
            result = False

        return {'plugin_id': plugin_id, 'result': result}

    @check_rollback
    async def rollback(self):
        return True
