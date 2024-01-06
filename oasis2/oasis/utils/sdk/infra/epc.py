from conf.infra_conf import EPC_API_MAP
from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.convert import list2dict
from oasis.utils.exceptions import EpcRequestException
from oasis.utils.generator import gen_uuid4
from oasis.utils.poll_util import wait_until_complete


def _prepare(func):
    async def __inner(self, *args, **kwargs):
        account_id = kwargs.pop('account_id', None)
        if not account_id:
            raise Exception(f'Please specify account_id, got {account_id}')
        request_id = kwargs.pop('request_id', gen_uuid4())

        headers = {
            'X-Ksc-Region': self.region,
            'X-Ksc-Request-Id': request_id,
            'X-Ksc-Account-Id': account_id,
            'X-KSC-SOURCE': self.product,
            'X-KSC-SK': self.ksc_sk,
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        params = {
            'Version': self.version,
        }

        return await func(self, params=params, headers=headers, *args, **kwargs)

    return __inner


class EpcClient:
    def __init__(self, product):
        self.product = product
        self.endpoint = config.get('infra', 'epc_endpoint', fallback=None)
        self.ksc_sk = config.get('ksc', product)
        self.region = config.get('infra', 'region')
        self.version = config.get('infra', 'epc_version', fallback=None)

    @_prepare
    async def notify_suborder_status(self, instance_id, epc_sub_order_id,
                                     status, owner_product_group=None,
                                     owner_instance_id=None, *,
                                     account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', EPC_API_MAP['notify'])

        data = {
            "instanceId": instance_id,
            "subOrderId": epc_sub_order_id,
            "status": status,
        }
        if owner_product_group:
            data.setdefault('ownerProductGroup', owner_product_group)
        if owner_instance_id:
            data.setdefault('ownerInstanceId', owner_instance_id)

        code, ret = await http.post(self.endpoint, params=params, data=data, headers=headers)
        if 199 < code < 300:
            return True
        raise Exception(f'epc_notify_suborder_status failed, return {ret}')

    @_prepare
    async def create_instance(self, epc_param, account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', EPC_API_MAP['create_instance'])
        epc_param.update(params)

        # todo not here
        # if data['ChargeType'] == "FreeTrial":
        #     data['ChargeType'] = "Daily"
        # if data['ChargeType'] == "Monthly":
        #     data['ChargeType'] = "Daily"
        code, ret = await http.get(self.endpoint, params=epc_param, headers=headers)

        if 199 < code < 300:
            return ret.get('Host', None)
        raise EpcRequestException(f'Create epc failed, return {ret}')

    @_prepare
    async def describe_instances(self, instance_ids, account_id=None, params: dict = None, headers: dict = None):
        instance_dict = list2dict('HostId', instance_ids)
        params.setdefault('Action', EPC_API_MAP['describe_instances'])
        if len(instance_ids) > 5:
            params.setdefault('MaxResults', len(instance_ids))
        params.update(instance_dict)

        code, ret = await http.get(self.endpoint, params=params, headers=headers)
        if 199 < code < 300:
            res = []
            host_set = ret.get('HostSet', None)
            if host_set:
                for h in host_set:
                    result = dict()
                    result['HostType'] = h.get('HostType', None)
                    result['Raid'] = h.get('Raid', None)
                    result['HostName'] = h.get('HostName', None)
                    result['HostStatus'] = h.get('HostStatus', None)
                    result['HostId'] = h.get('HostId', None)
                    nias = h.get('NetworkInterfaceAttributeSet', None)
                    if nias:
                        result['PrivateIpAddress'] = nias[0].get('PrivateIpAddress', None)
                    result['NetworkInterfaceMode'] = h.get('NetworkInterfaceMode', None)
                    res.append(result)
            return res
        raise EpcRequestException(f'epc_describe_instances search failed, return: {ret}')

    @_prepare
    async def delete_instance(self, host_id, account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', EPC_API_MAP['delete_instances'])
        params.setdefault('HostId', host_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers)
        if 199 < code < 300:
            return ret.get('Return', False) is not False
        raise EpcRequestException(f'EPC delete_instance failed, epc id: {host_id}, return: {ret}')

    @_prepare
    async def start_instance(self, host_id, account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', EPC_API_MAP['start_instance'])
        params.setdefault('HostId', host_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers)
        if 199 < code < 300:
            return ret.get('Return', False) is not False
        raise EpcRequestException(f'EPC start_instance failed, epc id: {host_id}, return: {ret}')

    @_prepare
    async def stop_instance(self, host_id, account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', EPC_API_MAP['stop_instance'])
        params.setdefault('HostId', host_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers)
        if 199 < code < 300:
            return ret.get('Return', False) is not False
        raise EpcRequestException(f'EPC stop_instance failed, epc id: {host_id}, return: {ret}')

    @_prepare
    async def reboot_instance(self, host_id, account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', EPC_API_MAP['reboot_instance'])
        params.setdefault('HostId', host_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers)
        if 199 < code < 300:
            return ret.get('Return', False) is not False
        raise EpcRequestException(f'EPC reboot_instance failed, epc id: {host_id}, return: {ret}')

    async def get_private_ip_address(self, instance_id):
        instance_ids = [instance_id]
        ret = await self.describe_instances(instance_ids)
        if ret:
            return ret[0].get('PrivateIpAddress', None)
        return None

    async def check_create_active(self, instance_ids, *, account_id=None, flag_state='Running'):
        _pinstances = await self.describe_instances(instance_ids, account_id=account_id)

        _ac_lst = []
        for _in in _pinstances:
            _istate = _in.get("HostStatus", "unknown")
            if flag_state == _istate:
                _ac_lst.append(_in)
            elif "InstallFailed" == _istate:
                _errmsg = "Node %s has error status, epc HostId: %s" % (_in.get("HostName", ""), _in.get("HostId"))
                raise EpcRequestException(_errmsg)

        return _ac_lst

    @wait_until_complete(timeout=1800, interval=60)
    async def wait_create_active(self, instance_ids, account_id=None, flag_state='Running'):
        if not instance_ids:
            return True
        ac_lst = await self.check_create_active(instance_ids, flag_state=flag_state, account_id=account_id)
        return len(instance_ids) == len(ac_lst)

    @wait_until_complete(timeout=1800, interval=60)
    async def wait_instances_delete(self, instance_ids, account_id=None):
        if not instance_ids:
            return True
        _pinstances = await self.describe_instances(instance_ids, account_id=account_id)
        return len(_pinstances) == 0
