import os
import re

from oasis.api.kes.methods import add_plugin
from oasis.api.kes.methods import get_plugin
from oasis.db.models.es_plugin import EsPluginModel
from oasis.utils.config import config
from oasis.utils.logger import logger


async def add_user_plugin(cluster_id, plugins):
    res_plugins = []

    for plugin in plugins:
        ks3_address = plugin.pop('ks3_address', None)
        if not ks3_address:
            raise Exception(f'ks3 address is None')

        upload_type = plugin.pop('upload_type', None)
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

        plugin_name = plugin.pop('name', None)
        if not plugin_name:
            raise Exception(f'plugin name is None')

        plugin_obj = await get_plugin(cluster_id, plugin_name, file_name)
        if plugin_obj:
            raise Exception(f'Plugin already exist, name {plugin_name}')

        description = plugin.pop('description', None)

        plugin_id = await add_plugin(cluster_id, plugin_name, upload_type, ks3_address, description)
        res_plugins.append(plugin_id)
        logger.info('util:::add_user_plugin:::', f'===add plugin id: {plugin_id}')

    return res_plugins
