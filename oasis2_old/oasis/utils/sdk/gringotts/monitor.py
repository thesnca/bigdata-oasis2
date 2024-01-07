# -*- coding: utf-8 -*-
from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.generator import gen_uuid4


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


class GringottsMonitorClient(object):
    def __init__(self):
        self.endpoint = config.get('gringotts', 'gringotts_monitor_url')

    def __getattr__(self, api):
        def __inner(*args, **kwargs):
            @_prepare
            async def __request_gg(self, token=None, headers=None):
                code, ret = await http.post(f'{self.endpoint}{api}', data=kwargs, headers=headers)
                # TODO convert gg return
                return ret

            token = kwargs.pop('token', None)
            return __request_gg(self, token=token)

        return __inner

    @_prepare
    async def list_cluster_status(self, cluster_id, cluster_type, token=None, headers=None):
        api = 'ListClusterStatus'

        data = {
            'ClusterId': cluster_id,
            'ClusterType': cluster_type,
            'RequestId': headers.get('X-Ksc-Request-Id', gen_uuid4()),
        }

        code, ret = await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

        if 199 < code < 300:
            return ret
        raise Exception(f'Gringotts Monitor list_cluster_status failed, return: {ret}')
