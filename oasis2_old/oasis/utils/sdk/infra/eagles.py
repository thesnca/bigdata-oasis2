# http://wiki.op.ksyun.com/display/cloudmonitor/11.+KHBase
from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger

SERVICEPRODUCT = {
    'KES': {
        'product_user_type': 72,
        'product_cluster_type': 73,
        'product_instance_type': 75
    },
    'KHBASE': {
        'product_user_type': 83,
        'product_cluster_type': 84,
        'product_master_type': 85,
        'product_core_type': 86
    }
}


def _prepare(func):
    async def __inner(self, *args, **kwargs):
        enable_eagles = config.get('eagles', 'enable')
        if not enable_eagles == 'true':
            logger.warn('Eagles is disabled, return .')
            return

        account_id = kwargs.pop('account_id', None)
        if not account_id:
            raise Exception(f'Please specify account_id, got {account_id}')
        request_id = kwargs.pop('request_id', gen_uuid4())

        headers = {
            'Content-Type': 'application/json',
            'X-KSC-REQUEST-ID': request_id,
            'X-KSC-ACCOUNT-ID': account_id,
            'X-KSC-REGION': self.region,
            'Accept': 'application/json',
        }
        data = {
            'regionKey': self.region,
            'productType': self.product_type,
        }

        return await func(self, data=data, headers=headers, *args, **kwargs)

    return __inner


class EaglesClient(object):
    def __init__(self):
        self.endpoint = config.get('eagles', 'eagles_uri')
        self.region = config.get('eagles', 'region')
        self.product_type = config.get('eagles', 'product_type')

    @_prepare
    async def add_monitor(self, cluster_id, monitor_instance_name, monitor_instance_ip,
                          account_id=None, data: dict = None, headers: dict = None):
        api = '/monitorapi/host/create'
        data.setdefault('instanceId', cluster_id)
        data.setdefault('hostName', monitor_instance_name)
        data.setdefault('guestIp', monitor_instance_ip)
        data.setdefault('template', [])

        await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

    @_prepare
    async def remove_monitor(self, instance_id,
                             account_id=None, data: dict = None, headers: dict = None):
        api = '/monitorapi/host/delete'
        data.setdefault('instanceIdList', [instance_id])

        await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

    @_prepare
    async def service_add_cluster_monitor(self, cluster_id, cluster_name, cluster_type,
                                          account_id=None, data: dict = None, headers: dict = None):
        api = '/monitorapi/host/create'

        # not exists? raise it!
        eagle_user_type = SERVICEPRODUCT[cluster_type]['product_user_type']
        eagle_cluster_type = SERVICEPRODUCT[cluster_type]['product_cluster_type']
        values = {
            'instanceId': cluster_id,
            'productType': eagle_user_type,
            'hostName': cluster_name,
            'guestIp': f'{cluster_type}_cluster_monitor',
            'template': [],
            'extProperty': {
                'parentInstanceId': '',
                'property': [
                    {
                        'field': 'childtype',
                        'value': eagle_cluster_type,
                        'name': cluster_name
                    }
                ]
            }
        }

        data.update(values)
        await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

    @_prepare
    async def service_remove_cluster_monitor(self, cluster_id, cluster_type,
                                             account_id=None, data: dict = None, headers: dict = None):
        api = '/monitorapi/host/delete'

        values = {
            'instanceIdList': [cluster_id],
            'productType': SERVICEPRODUCT[cluster_type]['product_cluster_type']
        }
        data.update(values)
        await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

    @_prepare
    async def service_add_instances_monitor(self, cluster_id, cluster_name,
                                            instance_id, instance_name, instance_internal_ip,
                                            cluster_type, instance_group_type,
                                            account_id=None, data: dict = None, headers: dict = None):
        api = '/monitorapi/host/create'

        instance_key = ''
        if cluster_type == 'KES':
            instance_key = 'product_instance_type'
        elif cluster_type == 'KHBASE':
            instance_key = f'product_{instance_group_type.lower()}_type'

        eagle_instance_type = SERVICEPRODUCT[cluster_type][instance_key]

        values = {
            'instanceId': instance_id,
            'hostName': instance_name,
            'guestIp': instance_internal_ip,
            'template': [],
            'extProperty': {
                'parentInstanceId': cluster_id,
                'property': [
                    {
                        'field': 'clustername',
                        'value': cluster_name,
                        'name': cluster_name,
                    },
                    {
                        'field': 'childtype',
                        'value': eagle_instance_type,
                        'name': instance_name,
                    }
                ]
            },
            'productType': SERVICEPRODUCT[cluster_type]['product_user_type']
        }
        data.update(values)
        await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

    @_prepare
    async def service_remove_instance_monitor(self, instance_ids, cluster_type, instance_group_type,
                                              account_id=None, data: dict = None, headers: dict = None):
        api = '/monitorapi/host/delete'

        instance_key = ''
        if cluster_type == 'KES':
            instance_key = 'product_instance_type'
        elif cluster_type == 'KHBASE':
            instance_key = f'product_{instance_group_type.lower()}_type'

        eagle_instance_type = SERVICEPRODUCT[cluster_type][instance_key]

        if not isinstance(instance_ids, list):
            instance_ids = [instance_ids]

        values = {
            'instanceIdList': instance_ids,
            'productType': eagle_instance_type,
        }
        data.update(values)
        await http.post(f'{self.endpoint}{api}', data=data, headers=headers)

    @_prepare
    async def update_monitor(self, instance_id, instance_name,
                             account_id=None, data: dict = None, headers: dict = None):
        api = '/monitorapi/host/updateHostInstance'

        values = {
            'instanceId': instance_id,
            'instanceName': instance_name,
            'ip': '',
        }
        data.update(values)
        await http.put(f'{self.endpoint}{api}', data=data, headers=headers)
