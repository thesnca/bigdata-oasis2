# -*- coding: utf-8 -*-
from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.logger import logger
from oasis.utils.convert import dict_snake2camel
from oasis.utils.generator import gen_uuid4
from oasis.utils.poll_util import wait_until_complete


def _prepare(func):
    async def __inner(self, *args, **kwargs):
        request_id = kwargs.pop('request_id', gen_uuid4())
        token = kwargs.pop('token', None)
        if not token:
            raise Exception(f'Please specify token, got {token}')

        headers = {
            'X-Ksc-Request-Id': request_id,
            'x_auth_token': token,
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

        return await func(self, headers=headers, *args, **kwargs)

    return __inner


class GringottsClient:
    def __init__(self):
        self.endpoint = config.get('gringotts', 'gringotts_url')

    def __getattr__(self, api):
        def __inner(*args, **kwargs):
            @_prepare
            async def __request_gg(self, token=None, headers=None):
                code, ret = await http.post(f'{self.endpoint}{api}', data=kwargs, headers=headers, timeout=600)
                # TODO convert gg return
                return ret

            token = kwargs.pop('token', None)
            return __request_gg(self, token=token)

        return __inner

    @_prepare
    async def upgrade_instance_groups(self, data, token=None, headers=None):
        api = 'UpgradeInstanceGroups'

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts upgrade cluster failed, return: {ret}')

    @_prepare
    async def launch_cluster(self, data, token=None, headers=None):
        api = 'LaunchCluster'

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts launch cluster failed, return: {ret}')

    @_prepare
    async def scale_out_cluster(self, data, token=None, headers=None):
        api = 'ScaleOutInstanceGroups'

        code, ret = await http.post(f'{self.endpoint}{api}', data=data)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts scale out cluster failed, return: {ret}')

    @_prepare
    async def get_es_free_nodes(self, data, token=None, headers=None):
        api = 'GetEsFreeNodes'

        code, ret = await http.post(f'{self.endpoint}{api}', data=data)
        logger.info(self, f'gringotts get es free nodes... code: {code}, ret:{ret}')

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return ret
        raise Exception(f'Gringotts get es free nodes failed, return: {ret}')

    @_prepare
    async def scale_in_cluster(self, data, token=None, headers=None):
        api = 'ScaleInInstanceGroups'

        code, ret = await http.post(f'{self.endpoint}{api}', data=data)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts scale in cluster failed, return: {ret}')

    @_prepare
    async def delete_cluster(self, cluster_id, token=None, headers=None, is_validate_request=False):
        api = 'DeleteCluster'
        data = {
            'ClusterId': cluster_id,
        }

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts delete cluster failed, return: {ret}')

    @_prepare
    async def freeze_cluster(self, cluster_id, token=None, headers=None):
        api = 'FreezeCluster'
        data = {
            'ClusterId': cluster_id,
        }

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts freeze cluster failed, return: {ret}')

    @_prepare
    async def un_freeze_cluster(self, cluster_id, token=None, headers=None):
        api = 'UnfreezeCluster'
        data = {
            'ClusterId': cluster_id,
        }

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts unfreeze cluster failed, return: {ret}')

    @_prepare
    async def service_control(self, token=None, headers=None, **kwargs):
        api = 'ServiceControl'
        data = dict_snake2camel(kwargs)

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts service_control failed, return: {ret}')

    @_prepare
    async def component_control(self, token=None, headers=None, **kwargs):
        api = 'ComponentControl'
        data = dict_snake2camel(kwargs)

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts component_control failed, return: {ret}')

    @_prepare
    async def enable_xpack(self, token=None, headers=None, **kwargs):
        api = 'EnableXpack'
        data = dict_snake2camel(kwargs)

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts enable_xpack failed, return: {ret}')

    @_prepare
    async def disable_xpack(self, token=None, headers=None, **kwargs):
        api = 'DisableXpack'
        data = dict_snake2camel(kwargs)

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts disable_xpack failed, return: {ret}')

    @_prepare
    async def list_services_idle(self, cluster_ids, token=None, headers=None):
        api = 'ListServicesIdle'
        data = {
            'ClusterIds': cluster_ids,
        }

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            return ret.get('Operations', [])
        raise Exception(f'Gringotts list_services_idle failed, return: {ret}')

    @_prepare
    async def describe_operation(self, operation_id, token=None, headers=None):
        api = 'DescribeOperation'

        data = {
            'OperationId': operation_id,
        }

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            return ret
        raise Exception(f'Gringotts describe_operation failed, return: {ret}')

    @_prepare
    async def describe_cluster_force(self, cluster_id, token=None, headers=None):
        api = 'DescribeClusterForce'

        data = {
            'ClusterId': cluster_id,
        }

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            return ret
        raise Exception(f'Gringotts describe_cluster_force failed, return: {ret}')

    @_prepare
    async def install_user_plugin(self, token=None, headers=None, **kwargs):
        api = 'InstallUserPlugin'
        data = {
            'ClusterId': kwargs.get('cluster_id', None),
            'PluginId': kwargs.get('plugin_id', None),
            'Ks3PluginAddress': kwargs.get('ks3_plugin_address', None)
        }

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts install_user_plugin failed, return: {ret}')

    @_prepare
    async def uninstall_user_plugin(self, token=None, headers=None, **kwargs):
        api = 'UninstallUserPlugin'
        data = {
            'ClusterId': kwargs.get('cluster_id', None),
            'PluginId': kwargs.get('plugin_id', None),
            'Ks3PluginAddress': kwargs.get('ks3_plugin_address', None)
        }

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts uninstall_user_plugin failed, return: {ret}')

    @_prepare
    async def delete_user_plugin(self, token=None, headers=None, **kwargs):
        api = 'DeleteUserPlugin'
        data = {
            'ClusterId': kwargs.get('cluster_id', None),
            'PluginId': kwargs.get('plugin_id', None),
            'Ks3PluginAddress': kwargs.get('ks3_plugin_address', None)
        }

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts delete_user_plugin failed, return: {ret}')

    @_prepare
    async def snapshot_on(self, token=None, headers=None, **kwargs):
        api = 'SnapshotOn'
        data = dict_snake2camel(kwargs)

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            op_id = ret.get('OperationId', None)
            if op_id:
                return op_id
        raise Exception(f'Gringotts snapshot on failed, return: {ret}')

    @wait_until_complete(timeout=1800, interval=30)
    async def wait_gg_op_active(self, operation_id, token=None):
        ret = await self.describe_operation(operation_id, token=token)
        logger.info(self, f'wait gg op active... ret: {ret}')
        
        if ret:
            # Pending - begin
            # Running - execute
            # Succeed - ok
            # Faild - error
            # Status: 'Faild,Succeed,Running,Pending',

            status = ret.get('Operation', {}).get('Status', 'Unknown')
            if 'SUCCEEDED' == status:
                return True
            elif 'FAILED' == status:
                raise Exception(f'gg run task failed msg: {ret}')
        return False
