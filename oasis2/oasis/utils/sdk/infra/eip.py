from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger
from oasis.utils.sdk.infra.mock import mock_request_fail


def _prepare(func):
    async def __inner(self, *args, **kwargs):
        account_id = kwargs.pop('account_id', None)
        if not account_id:
            raise Exception(f'Please specify account_id, got {account_id}')
        request_id = kwargs.pop('request_id', gen_uuid4())

        headers = {
            'Accept': 'application/json',
            'X-Ksc-Region': self.region,
            'X-Ksc-Request-Id': request_id,
            # 'X-Ksc-Account-Id': account_id,
            'X-KSC-SOURCE': self.product,
            'X-KSC-SK': self.ksc_sk,
        }
        aws_headers = {
            'ak': self.ak,
            'sk': self.sk,
            'region': self.region,
            'host': self.endpoint.split('/')[2],
            'service': 'eip',
        }
        params = {
            'Version': self.version,
            'AccountId': account_id,
        }

        return await func(self, params=params, headers=headers, aws_headers=aws_headers, *args, **kwargs)

    return __inner


class EipClient(object):
    def __init__(self, product):
        self.product = product
        self.endpoint = config.get('infra', 'eip_endpoint')
        self.ksc_sk = config.get('ksc', product)
        self.region = config.get('infra', 'region')
        self.version = config.get('infra', 'eip_version')
        self.ak = config.get('iam', f'{product}_ak')
        self.sk = config.get('iam', f'{product}_sk')

    # http://eip.api.ksyun.com/?Action=GetLines&Version=2016-03-04&AccountId=73403574
    @_prepare
    async def get_line_id(self, eip_provider, account_id=None, aws_headers=None,
                          params: dict = None, headers: dict = None):
        params.setdefault('Action', 'GetLines')

        code, ret = await http.post(self.endpoint, params=params, headers=headers,
                                    aws_headers=aws_headers)
        if 199 < code < 300:
            lines = ret.get('LineSet', [])
            return lines
        else:
            return []

    @_prepare
    async def list_getlines(self, account_id=None, aws_headers=None,
                            params: dict = None, headers: dict = None):
        params.setdefault('Action', 'GetLines')

        code, ret = await http.post(self.endpoint, params=params, headers=headers,
                                    aws_headers=aws_headers)
        if 199 < code < 300:
            lines = ret.get('LineSet', [])
            return lines
        else:
            return []

    @_prepare
    async def describe_address(self, allocation_id=None, account_id=None, aws_headers=None,
                               params: dict = None, headers: dict = None):
        '''
        {
            "RequestId": "5375d486-d75a-4ebe-ac55-74da107d47fc",
            "AddressesSet": [
                {
                    "PublicIp": "198.18.0.164",
                    "AllocationId": "52325b11-e2ee-452d-b921-d90f3313e1db",
                    "State": "associate",
                    "LineId": "0cecf7e5-6c7b-4252-bf8c-bf44f9460d5c",
                    "BandWidth": 1,
                    "InstanceType": "Slb",
                    "InstanceId": "bf8c51b6-8963-4397-b905-1ac1f4073330",
                    "UserTag": "console",
                    "IpVersion": "ipv4",
                    "ProjectId": "0",
                    "CreateTime": "2021-12-13 15:27:53",
                    "Mode": "normal"
                }
            ]
        }
        '''
        params.setdefault('Action', 'DescribeAddresses')
        params.setdefault('AllocationId.1', allocation_id)

        code, ret = await http.post(self.endpoint, params=params, headers=headers,
                                    aws_headers=aws_headers)
        logger.debug('==describe_addresses code: %s, result: %s' % (code, ret))
        if 199 < code < 300:
            lines = ret.get('AddressesSet', [])
            if len(lines) > 0:
                return lines[0]
        return {}

    @_prepare
    async def list_addresses(self, project_id=None, account_id=None, aws_headers=None,
                             params: dict = None, headers: dict = None):
        params.setdefault('Action', 'DescribeAddresses')
        if project_id:
            params.setdefault('ProjectId', str(project_id))

        code, ret = await http.post(self.endpoint, params=params, headers=headers,
                                    aws_headers=aws_headers)
        logger.debug('==describe_addresses code: %s, result: %s' % (code, ret))
        if 199 < code < 300:
            lines = ret.get('AddressesSet', [])
            return lines
        else:
            return []

    # http://eip.api.ksyun.com/?Action=AllocateAddress&Version=2016-03-04&AccountId=73403574&ChargeType=RegionPeak&SubOrderId=EIP2S190827155612014227044&LineId=947ebe90-70b0-4479-b2c3-f26cac3802ef
    @_prepare
    async def get_allocate_address_id(self, charge_type, eip_order_id, line_id, project_id, eip_purchase_time, *,
                                      account_id=None, aws_headers=None,
                                      band_width=None,
                                      params: dict = None, headers: dict = None):
        params.setdefault('Action', 'AllocateAddress')
        params.setdefault('ChargeType', charge_type)
        params.setdefault('SubOrderId', eip_order_id)
        params.setdefault('LineId', line_id)

        if project_id:
            params.setdefault('ProjectId', str(project_id))

        if charge_type == 'PrePaidByMonth':
            params.setdefault('PurchaseTime', str(eip_purchase_time))

        if band_width:
            params.setdefault('BandWidth', str(band_width))
        else:
            params.setdefault('BandWidth', '1')

        params.setdefault('Service', 'eip')
        code, ret = await http.post(self.endpoint, params=params, headers=headers,
                                    aws_headers=aws_headers, retry_times=3)
        # code, ret = await mock_request_fail('http://10.69.72.79:28257/eip', {'SubOrderId': eip_order_id})
        logger.debug(f'==get_allocate_address_id code: {code}, result: {ret}')
        if 199 < code < 300:
            return ret.get('AllocationId', None), ret.get('PublicIp', None)
        raise Exception(f'get_allocate_address_id failed, error: {ret}')

    # http://eip.api.ksyun.com/?Action=AssociateAddress&Version=2016-03-04&AccountId=73403574&AllocationId=3f1d9d29-1d9d-466a-8266-fa23dea62a5a&InstanceId=92e27187-932f-4a89-ae70-357bc940f8b8&NetworkInterfaceId=63c5271a-f1dc-436a-87d6-b3f46f67e9ff&InstanceType=Ipfwd
    @_prepare
    async def associate_ipfwd_eip(self, instance_id, network_interface_id, allocation_id, *,
                                  account_id=None, aws_headers=None,
                                  params: dict = None, headers: dict = None):
        params.setdefault('Action', 'AssociateAddress')
        params.setdefault('AllocationId', allocation_id)
        params.setdefault('InstanceId', instance_id)
        params.setdefault('NetworkInterfaceId', network_interface_id)
        params.setdefault('InstanceType', 'Ipfwd')

        code, ret = await http.post(self.endpoint, params=params, headers=headers,
                                    aws_headers=aws_headers)
        if 199 < code < 300:
            return ret
        raise Exception(f'associate_ipfwd_eip failed, error: {ret}')

    @_prepare
    async def associate_slb_eip(self, instance_id, allocation_id, account_id=None,
                                aws_headers=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', 'AssociateAddress')
        params.setdefault('AllocationId', allocation_id)
        params.setdefault('InstanceId', instance_id)
        params.setdefault('InstanceType', 'Slb')

        code, ret = await http.post(self.endpoint, params=params, headers=headers,
                                    aws_headers=aws_headers)
        if 199 < code < 300:
            return ret
        raise Exception(f'associate_slb_eip failed, error: {ret}')

    # http://eip.api.ksyun.com/?Action=DisassociateAddress&Version=2016-03-04&AllocationId=3f1d9d29-1d9d-466a-8266-fa23dea62a5a&AccountId=73403574
    @_prepare
    async def disassociate_address_eip(self, allocation_id, account_id=None,
                                       aws_headers=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', 'DisassociateAddress')
        params.setdefault('AllocationId', allocation_id)

        code, ret = await http.post(self.endpoint, params=params, headers=headers,
                                    aws_headers=aws_headers)
        if 199 < code < 300:
            return ret
        raise Exception(f'disassociate_address_eip failed, error: {ret}')

    # http://eip.api.ksyun.com/?Action=ReleaseAddress&Version=2016-03-04&AllocationId=cbfa8715-c66c-42fb-906e-1f20979c627f&AccountId=73403574
    @_prepare
    async def release_address_eip(self, allocation_id, account_id=None,
                                  aws_headers=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', 'ReleaseAddress')
        params.setdefault('AllocationId', allocation_id)

        code, ret = await http.post(self.endpoint, params=params, headers=headers,
                                    aws_headers=aws_headers)
        if 199 < code < 300:
            return ret
        raise Exception(f'release_address_eip failed, error: {ret}')
