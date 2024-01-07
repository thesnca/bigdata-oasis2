from datetime import datetime
from hashlib import sha256
import hmac
import json

from oasis.db.service import redis_client
from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.generator import gen_uuid4

IAM_URI = config.get('iam', 'iam_uri')
IAM_PROXY_URI = config.get('iam', 'iam_proxy_uri')


def _prepare(func):
    async def _inner(account_id, product, *args, **kwargs):
        headers = {
            'X-KSC-REQUEST-ID': gen_uuid4(),
            'X-KSC-REGION': config.get('infra', 'region'),
            'X-KSC-ACCOUNT-ID': account_id,
            'X-KSC-TYPE': 'identity',
            'Content-Type': 'application/json',
        }
        params = {
        }
        return await func(account_id, product, headers=headers, params=params, *args, **kwargs)

    return _inner


def _cache(func):
    async def _inner(user_id, product, *args, **kwargs):
        cache_expire = int(config.get('oasis', 'cache_expire'))
        cache_key = f'/oasis/user/{product}/{user_id}/'
        user = await redis_client.get(cache_key)
        if user:
            await redis_client.expire(cache_key, cache_expire)
            return json.loads(user)

        res = await func(user_id, product, *args, **kwargs)
        if res:
            await redis_client.set(cache_key, res, expire=cache_expire)
        return res

    return _inner


@_prepare
async def get_user_ak_sk_by_id(account_id, product, headers=None, params=None):
    super_ak = config.get('iam', f'{product}_ak')
    super_sk = config.get('iam', f'{product}_sk')
    time = int(datetime.now().timestamp())
    params.setdefault('Accesskey', super_ak)
    params.setdefault('AccountId', account_id)
    params.setdefault('Timestamp', time)
    params_str = '&'.join([f'{k}={v}' for k, v in params.items()])
    signature = hmac.new(super_sk.encode('utf8'), params_str.encode('utf8'), sha256).hexdigest()
    params.setdefault('Signature', signature)
    _, res = await http.get(f'{IAM_URI}list_accesskey', params=params, headers=headers)
    ak_sk_list = res.get('Data', None)
    if not ak_sk_list:
        return None
    ak_sk = ak_sk_list[0]
    user_ak, user_sk = ak_sk.get('access_key'), ak_sk.get('secret_key')
    return user_ak, user_sk


@_cache
@_prepare
async def get_user_by_id(account_id, product, headers=None, params=None):
    params.setdefault('from', product)
    code, res = await http.get(f'{IAM_PROXY_URI}user', params=params, headers=headers)
    if 199 < code < 300:
        return res
    return None
