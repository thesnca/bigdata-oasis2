from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.exceptions import VpcRequestException
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger


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
            'service': 'vpc',
        }
        params = {
            'Version': self.version,
            'AccountId': account_id,
        }

        return await func(self, params=params, headers=headers, aws_headers=aws_headers, *args, **kwargs)

    return __inner


class VpcClient(object):
    def __init__(self, product):
        self.product = product
        self.endpoint = config.get('infra', 'vpc_endpoint')
        self.ksc_sk = config.get('ksc', product)
        self.region = config.get('infra', 'region')
        self.version = config.get('infra', 'vpc_version')
        self.ak = config.get('iam', f'{product}_ak')
        self.sk = config.get('iam', f'{product}_sk')

    @_prepare
    async def describe_vpcs(self, vpc_ids, account_id=None, params: dict = None, headers: dict = None,
                            aws_headers: dict = None):
        params.setdefault('Action', 'DescribeVpcs')

        if vpc_ids:
            for idx, vpc_id in enumerate(vpc_ids):
                params.setdefault(f'VpcId.{idx + 1}', vpc_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers,
                                   aws_headers=aws_headers, res_type='text')

        if 199 < code < 300:
            vpcs = ret.get('VpcSet', [])
            return vpcs
        else:
            logger.warn("describe vpcs failed, api: %s, code: %s, response: %s" % (params, code, ret))
            return None
            # raise ex.VpcRequestException("describe vpcs failed, api: %s, code: %s, response: %s" % (api, code, ret))

    @_prepare
    async def describe_subnets(self, vpc_ids, subnet_ids, availability_zone, *,
                               account_id=None, params: dict = None, headers: dict = None, aws_headers: dict = None):
        params.setdefault('Action', 'DescribeSubnets')
        if vpc_ids:
            params.setdefault('Filter.1.Name', 'vpc-id')
            for idx, vpc_id in enumerate(vpc_ids):
                params.setdefault(f'Filter.1.Value.{idx + 1}', vpc_id)
        if subnet_ids:
            for idx, subnet_id in enumerate(subnet_ids):
                params.setdefault(f'SubnetId.{idx + 1}', subnet_id)
        if availability_zone:
            params.setdefault('Filter.2.Name', 'availability-zone-name')
            params.setdefault('Filter.2.Value.1', availability_zone)

        code, ret = await http.get(self.endpoint, params=params, headers=headers,
                                   aws_headers=aws_headers, res_type='text')
        if 199 < code < 300:
            if ret:
                if not subnet_ids:
                    subnets = ret.get('SubnetSet', [])
                else:
                    subnets = ret.get('SubnetSet', [])
                return subnets
        raise VpcRequestException("describe subnets failed, api: %s" % params)

    @_prepare
    async def vpc_create(self, account_id, vpc_name, cidr_block, is_default,
                         params: dict = None, headers: dict = None, aws_headers: dict = None):
        params.setdefault('Action', 'CreateVpc')
        if vpc_name:
            params.setdefault('VpcName', vpc_name)

        params.setdefault('CidrBlock', cidr_block)
        if is_default:
            params.setdefault('IsDefault', 'true')

        code, ret = await http.get(self.endpoint, params=params, headers=headers,
                                   aws_headers=aws_headers, res_type='text')

        if 199 < code < 300:
            vpcs = ret.get('VpcSet', [])
            if vpcs:
                return vpcs[0]
            return None
        raise VpcRequestException("create vpc failed, api: %s, code: %s, response: %s" % (params, code, ret))

    @_prepare
    async def subnet_create(self, account_id, subnet_name, vpc_id, cidr_block, subnet_type, dhcpip_from=None,
                            dhcpip_to=None, gateway_ip=None, dns1=None, dns2=None,
                            params: dict = None, headers: dict = None, aws_headers: dict = None):
        params.setdefault('Action', 'CreateSubnet')
        params.setdefault('VpcId', vpc_id)
        params.setdefault('SubnetName', subnet_name)
        params.setdefault('CidrBlock', cidr_block)
        params.setdefault('SubnetType', subnet_type)

        if dhcpip_from:
            params.setdefault('DhcpIpFrom', dhcpip_from)
        if dhcpip_to:
            params.setdefault('DhcpIpTo', dhcpip_to)
        if gateway_ip:
            params.setdefault('GatewayIp', gateway_ip)
        if dns1:
            params.setdefault('Dns1', dns1)
        if dns2:
            params.setdefault('Dns2', dns2)

        code, ret = await http.get(self.endpoint, params=params, headers=headers,
                                   aws_headers=aws_headers, res_type='text')

        if 199 < code < 300:
            return
        raise VpcRequestException("create subnet failed, api: %s, code: %s, response: %s" % (params, code, ret))

    @_prepare
    async def nat_create(self, account_id, vpc_id, nat_type, nat_mode, band_width,
                         nat_name=None, natip_num=None, charge_type=None, purchase_time=None,
                         params: dict = None, headers: dict = None, aws_headers: dict = None):
        params.setdefault('Action', 'CreateNat')
        params.setdefault('VpcId', vpc_id)
        params.setdefault('NatType', nat_type)
        params.setdefault('NatMode', nat_mode)
        params.setdefault('BandWidth', str(band_width))
        if nat_name:
            params.setdefault('NatName', nat_name)
        if natip_num:
            params.setdefault('NatIpNumber', str(natip_num))
        if charge_type:
            params.setdefault('ChargeType', charge_type)
        if purchase_time:
            params.setdefault('PurchaseTime', str(purchase_time))

        code, ret = await http.get(self.endpoint, params=params, headers=headers,
                                   aws_headers=aws_headers, res_type='text')

        if 199 < code < 300:
            return
        raise VpcRequestException("create vpc failed, api: %s, code: %s, response: %s" % (params, code, ret))

    # http://vpc.region.api.ksyun.com/?Action=DescribeNetworkInterfaces&Version=2016-03-04&Filter.1.Name=instance-id&Filter.1.Value.1=92e27187-932f-4a89-ae70-357bc940f8b8&AccountId=73403574
    @_prepare
    async def get_network_interface_id(self, instance_id, account_id=None,
                                       params: dict = None, headers: dict = None, aws_headers: dict = None):
        params.setdefault('Action', 'DescribeNetworkInterfaces')
        params.setdefault('Filter.1.Name', 'instance-id')
        params.setdefault('Filter.1.Value.1', instance_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers,
                                   aws_headers=aws_headers, res_type='text')

        network_interface_id = None
        if 199 < code < 300:
            interfaces = ret.get('NetworkInterfaceSet', [])
            if not interfaces:
                return None

            for ifs in interfaces:
                if 'primary' == ifs.get('NetworkInterfaceType', ''):
                    network_interface_id = ifs.get('NetworkInterfaceId', None)
                    break
        return network_interface_id

    @_prepare
    async def describe_security_groups(self, vpc_ids, account_id=None,
                                       params: dict = None, headers: dict = None, aws_headers: dict = None):
        params.setdefault('Action', 'DescribeSecurityGroups')
        if vpc_ids:
            params.setdefault('Filter.1.Name', 'vpc-id')
            for idx, vpc_id in enumerate(vpc_ids):
                params.setdefault(f'Filter.1.Value.{idx + 1}', vpc_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers,
                                   aws_headers=aws_headers, res_type='text')

        if 199 < code < 300:
            security_groups_data = ret.get('SecurityGroupSet', [])
            return security_groups_data
        logger.error(f"describe security groups failed, api: {params}, code: {code}, response: {ret}")
        return []
        # raise ex.VpcRequestException("describe subnets failed, api: %s" % api)
