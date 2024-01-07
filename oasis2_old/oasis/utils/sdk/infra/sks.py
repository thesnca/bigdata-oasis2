from conf.infra_conf import SKS_API_MAP
from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.exceptions import EpcRequestException
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger


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


class SksClient:
    def __init__(self, product):
        self.product = product
        self.endpoint = config.get('infra', 'sks_endpoint')
        self.ksc_sk = config.get('ksc', product)
        self.region = config.get('infra', 'region')
        self.version = config.get('infra', 'sks_version')

    @_prepare
    async def describe_keys(self, key_name='', account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', SKS_API_MAP['describe_keys'])

        if key_name:
            params.setdefault('KeyName', key_name)

        res_list = []
        next_token = 1
        while next_token:
            params.setdefault('NextToken', next_token)
            code, ret = await http.get(self.endpoint, params=params, headers=headers, res_type='json')
            if 199 < code < 300:
                total = ret.get('TotalCount')
                next_token = ret.get('NextToken')
                res = ret.get('KeySet')
                res_list.extend(res)
                if total <= len(res_list):
                    return res_list

    @_prepare
    async def create_key(self, key_name='', account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', SKS_API_MAP['create_key'])

        if key_name:
            params.setdefault('KeyName', key_name)

        code, ret = await http.get(self.endpoint, params=params, headers=headers, res_type='json')
        logger.debug(f'Create key ret {ret}')
        if 199 < code < 300:
            return {
                'management_private_key': ret['PrivateKey'],
                'management_keypair_id': ret['Key']['KeyId'],
                'management_public_key': ret['Key']['PublicKey']
            }
        raise EpcRequestException(f'EPC create key failed, return: {ret}')

    @_prepare
    async def delete_key(self, key_id, account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', SKS_API_MAP['delete_key'])
        params.setdefault('KeyId', key_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers, res_type='json')
        if 199 < code < 300:
            return ret
        logger.warn(f'EPC delete key failed, return: {ret}]')
        return None
