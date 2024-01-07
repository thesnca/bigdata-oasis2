from asyncio import sleep
import json

import aiohttp

from oasis.utils.generator import gen_aws_auth_header
from oasis.utils.logger import logger


def _get_session(func):
    async def _inner(*args, **kwargs):
        async with aiohttp.ClientSession() as session:
            return await func(session=session, *args, **kwargs)

    return _inner


def retry(func):
    async def _inner(*args, **kwargs):
        retry_times = kwargs.pop('retry_times', 3)
        if type(retry_times) is not int or retry_times < 0:
            raise Exception(f'Error retry_times, got {retry_times}')
        interval = kwargs.pop('interval', 30)

        headers = kwargs.get('headers', {})
        aws_headers = kwargs.get('aws_headers', {})
        params = kwargs.get('params', {})
        data = kwargs.get('data', {})
        method = func.__name__.upper()

        if aws_headers:
            aws_header = gen_aws_auth_header(method=method, params=params, **aws_headers)
            headers.update(aws_header)

        kwargs_str = ''
        if params:
            kwargs_str += '?' + '&'.join([f'{k}={v}' for k, v in params.items()])
        kwargs_str += ''.join([f' -H \'{k}:{v}\'' for k, v in headers.items()])
        if data:
            data_str = str(data).replace('\'', '"')
            kwargs_str += f' -d \'{data_str}\''

        url = args[0]
        for i in range(retry_times):
            try:
                logger.info(f'Request send. {method} {url}{kwargs_str}.')
                ret = await func(*args, **kwargs)
                logger.info(f'Request back. {method} {url}{kwargs_str}. Return {ret}.')
                return ret
            except Exception as e:
                logger.error(f'Request failed, retry {i + 1}/{retry_times}. {method} {url}{kwargs_str}. Error: {e}')
                if i < retry_times - 1:
                    await sleep(interval)

        raise Exception(f'Request failed {retry_times} times. {method} {url}{kwargs_str}.')

    return _inner


@_get_session
@retry
async def get(url, *, session, params=None, headers=None,
              res_type=None, aws_headers=None, **kwargs):
    # if aws_headers:
    #     aws_header = gen_aws_auth_header(method='GET', params=params, **aws_headers)
    #     headers.update(aws_header)
    # logger.info(f'GET {url}, headers : {headers}, params: {params}')
    async with session.get(url, params=params, headers=headers, **kwargs) as res:
        try:
            return res.status, await res.json()
        except:
            return res.status, await res.text()


@_get_session
@retry
async def post(url, *, session, params=None, data=None, headers=None,
               res_type=None, aws_headers=None, **kwargs):
    if aws_headers:
        aws_header = gen_aws_auth_header(method='POST', params=params, **aws_headers)
        headers.update(aws_header)
    if type(data) == dict:
        data = json.dumps(data)
    # logger.info(f'POST {url}, headers : {headers}, data: {data}, params: {params}')
    async with session.post(url, params=params, data=data, headers=headers, **kwargs) as res:
        try:
            return res.status, await res.json(encoding='utf-8')
        except:
            return res.status, await res.text(encoding='utf-8')


@_get_session
@retry
async def put(url, *, session, params=None, data=None, headers=None,
              res_type=None, aws_headers=None, **kwargs):
    # if aws_headers:
    #     aws_header = gen_aws_auth_header(method='PUT', params=params, **aws_headers)
    #     headers.update(aws_header)
    if type(data) == dict:
        data = json.dumps(data)
    # logger.info(f'PUT {url}, headers : {headers}, data: {data}, params: {params}')
    async with session.put(url, params=params, data=data, headers=headers, **kwargs) as res:
        try:
            return res.status, await res.json()
        except:
            return res.status, await res.text()


@_get_session
@retry
async def delete(url, *, session, params=None, data=None, headers=None,
                 res_type=None, aws_headers=None, **kwargs):
    # if aws_headers:
    #     aws_header = gen_aws_auth_header(method='DELETE', params=params, **aws_headers)
    #     headers.update(aws_header)
    if type(data) == dict:
        data = json.dumps(data)
    # logger.info(f'DELETE {url}, headers : {headers}, data: {data}, params: {params}')
    async with session.delete(url, params=params, data=data, headers=headers, **kwargs) as res:
        try:
            return res.status, await res.json()
        except:
            return res.status, await res.text()
