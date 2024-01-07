import base64
from hashlib import sha1
import hmac
import time
from urllib import parse
from xml.dom import minidom

from ks3.connection import Connection
from ks3.prefix import Prefix

from ks3 import auth
from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.logger import logger


class Ks3Client:
    def __init__(self, ak=None, sk=None, endpoint='', bucket_name='', is_secure=False, init=False):
        self.ak = config.get('kes_user_plugin', 'ak') or ak
        self.sk = config.get('kes_user_plugin', 'sk') or sk
        self.endpoint = config.get('infra', 'ks3_endpoint') or endpoint
        self.region = config.get('infra', 'ks3_region')
        self.bucket_name = config.get('kes_user_plugin', 'bucket_name') or bucket_name

        self.is_secure = is_secure
        self.conn = None
        if init:
            import asyncio
            asyncio.run(self.init())

    async def init(self):
        if not self.conn:
            # 金山云主账号 AccessKey 拥有所有API的访问权限，风险很高。
            # 强烈建议您创建并使用子账号账号进行 API 访问或日常运维，请登录 https://uc.console.ksyun.com/pro/iam/#/user/list 创建子账号。
            # 通过指定 host(Endpoint)，您可以在指定的地域创建新的存储空间。
            self.conn = Connection(self.ak, self.sk, host=self.endpoint, is_secure=self.is_secure, domain_mode=False)

    async def _request_ks3(self, *, url=None, method='GET', data=None, ak=None, sk=None, ):
        headers = {}

        bucket = ''
        key = ''
        if url:
            urls = url.split('/')
            bucket = urls[2]
            key = '/'.join(urls[3:])

        path_str = f'/{bucket}'
        if key:
            path_str += f'/{parse.quote_plus(key.encode("utf-8"))}'

        if ak and sk:
            timestr = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
            value_dict = {
                'content-md5': '',
                'content-type': '',
                'date': timestr,
            }
            values = [method]
            values.extend([value_dict[k] for k in sorted(value_dict.keys())])

            values.append(path_str)
            buf = '\n'.join(values)

            b64_hmac = base64.b64encode(hmac.new(sk.encode('utf8'), buf.encode('utf8'), sha1).digest()).strip()
            headers.setdefault('Date', timestr)
            headers.setdefault('Authorization', 'KSS %s:%s' % (ak, b64_hmac.decode('utf8')))

        finalurl = f'http://{self.endpoint}/{bucket}'
        return await getattr(http, method.lower())(finalurl, headers=headers, data=data)

    async def get_signature_headers(self, *, endpoint=None, url=None, method='GET', ak=None, sk=None, content_type=''):
        headers = {}
        endpoint = endpoint or self.endpoint
        ak = ak or self.ak
        sk = sk or self.sk
        bucket = ''
        key = ''
        if url:
            urls = url.split('/')
            bucket = urls[2]
            key = '/'.join(urls[3:])

        path_str = f'/{bucket}'
        if key:
            if method in ['DELETE', 'PUT']:
                path_str += f'/{key}'
            else:
                path_str += f'/{parse.quote_plus(key.encode("utf-8"))}'

        if ak and sk:
            timestr = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
            values = [
                method,  # HTTP-Verb 表示请求的方法，如：GET\PUT\POST\DELETE等
                '',
                # Content-MD5 表示请求内容数据的MD5值, 使用Base64编码。当请求头中包含Content-MD5时，需要在StringToSign中包含Content-MD5，否则用("")替代。
                # content_type,  # Content-Type 表示请求体的类型
                timestr,
                # Date 表示此次请求操作的时间，必须为 HTTP1.1 中支持的 GMT 格式，例如：Tue, 30 Nov 2021 06:29:38 GMT。若Date时间与KS3服务端时间相差15分钟以上，则KS3将返回403错误。
                'x-kss-acl:public-read',
                f'x-kss-date:{timestr}',
                # CanonicalizedKssHeaders：有的客户端不支持发送Date请求头。这种情况下，计算签名时需要保持Date字段的同时，在CanonicalizedKssHeaders中加入x-kss-date，格式与Date一致。当请求中包含x-kss-date头时，KS3在计算签名时会忽略Date头。
                path_str,  # CanonicalizedResource，表示用户访问的资源
            ]
            buf = '\n'.join(values)
            logger.info(self, f'===StringToSign: {buf}')

            b64_hmac = base64.b64encode(hmac.new(sk.encode('utf8'), buf.encode('utf8'), sha1).digest()).strip()
            headers.setdefault('Authorization', 'KSS %s:%s' % (ak, b64_hmac.decode('utf8')))

            if method == 'DELETE':
                headers.setdefault('HOST', f'{bucket}.{endpoint}')
            elif method == 'PUT':
                headers.setdefault('x-kss-date', timestr)
                headers.setdefault('bucket', bucket)
                headers.setdefault('endpoint', endpoint)

        return key, bucket, headers

    async def list_buckets(self, ak=None, sk=None):
        results = []
        _, res = await self._request_ks3(ak=ak, sk=sk)
        res = minidom.parseString(res)
        res = res.getElementsByTagName('Bucket')
        for r in res:
            name = r.getElementsByTagName('Name')[0].childNodes[0].data
            region = r.getElementsByTagName('Region')[0].childNodes[0].data
            if region == self.region:
                results.append(name)
        return results

    async def get_bucket(self, bucket_name=None):
        """获取存储空间实例
        """
        bucket_name = bucket_name if bucket_name else self.bucket_name
        return self.conn.get_bucket(bucket_name)

    async def get_presigned_url(self, key_name):
        """
        生成上传文件的签名URL

        :param key_name: 填写Object完整路径。Object完整路径中不能包含Bucket名称。
                         KS3文件，包含文件后缀在内的完整路径。如 user-plugin/cluster-id/filename.zip
        :return:
        """
        url = None
        bucket_name = await self.get_bucket()

        #
        k = bucket_name.new_key(key_name)
        if k:
            # 生成上传文件的签名URL，有效时间为1天。
            url = k.get_presigned_url(60 * 60 * 24)

        return url

    async def generate_signed_headers(self, method, bucket='', key='', headers=None, query_args=None):
        headers = headers or {}
        auth.add_auth_header(self.ak, self.sk, headers, method, bucket, key, query_args)
        return headers

    async def get_presigned_headers(self, key_name):
        headers = {
            'content-type': 'application/zip'
        }
        headers = await self.generate_signed_headers('PUT', self.bucket_name, key_name, headers)
        return headers

    async def list_object(self, prefix):
        bucket = await self.get_bucket()
        if not prefix.endswith('/'):
            prefix += '/'
        stack = [prefix]
        while stack:
            p = stack.pop()
            for obj in bucket.list(prefix=p, delimiter='/'):
                if isinstance(obj, Prefix):
                    stack.append(obj.name)
                else:
                    yield obj.name

    # async def delete_prefix(self, prefix):
    #     for i in range(10):
    #         try:
    #             object_list = await self.list_object(prefix)
    #             for key in object_list:
    #                 await self.delete_object(key)
    #             break
    #         except Exception as e:
    #             pass

    async def delete_object_sdk(self, key):
        """
        删除文件

        :param key: key表示删除KS3文件时需要指定包含文件后缀在内的完整路径。如 images/test.jpg
        :return:
        """
        bucket = await self.get_bucket()
        bucket.delete_key(key)

    # async def delete_object(self, url=None):
    #     await self._request_ks3(url=url, method='DELETE', ak=self.ak, sk=self.sk)
