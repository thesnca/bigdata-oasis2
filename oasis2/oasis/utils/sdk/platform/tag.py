from copy import deepcopy

from oasis.utils import http
from oasis.utils.config import config
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
            'X-Ksc-Account-Id': account_id,
            'X-KSC-SOURCE': self.product,
            'X-KSC-SK': self.ksc_sk,
        }
        aws_headers = {
            'ak': self.ak,
            'sk': self.sk,
            'region': self.region,
            'host': self.endpoint.split('/')[2],
            'service': 'tagv2',
        }
        params = {
            'Version': self.version,
            'AccountId': account_id,
        }

        return await func(self, params=params, headers=headers, aws_headers=aws_headers, *args, **kwargs)

    return __inner


class TagResource:
    class EXEC:
        ALL = 'all'
        NEW = 'new'
        SCALE = 'scale'
        BIND = 'bind'

    class SYS_CLUSTER_TAG:
        CLUSTER_TYPE = 'Ksyun_ClusterType'
        CLUSTER_ID = 'Ksyun_ClusterId'


class TagClient(object):
    def __init__(self, product):
        self.product = product
        self.endpoint = config.get('infra', 'tagv2_endpoint')
        self.ksc_sk = config.get('ksc', product)
        self.region = config.get('infra', 'region')
        self.version = config.get('infra', 'tagv2_version')
        self.ak = config.get('iam', f'{product}_ak')
        self.sk = config.get('iam', f'{product}_sk')

    @_prepare
    async def replace_resources_tags(self, resource_type, replace_instance_ids: list, replace_tag_ids: list,
                                     account_id=None, params: dict = None, headers: dict = None, aws_headers=None):
        '''
        https://docs.官网.com/documents/39824
        '''
        params.setdefault('Action', 'ReplaceResourcesTags')
        params.setdefault('ResourceType', resource_type)
        # [{“ResourceUuids”:“a,b”,“TagIds”:“1,2”}]
        params.setdefault('ReplaceTags[0][ResourceUuids]', ','.join(replace_instance_ids))
        params.setdefault('ReplaceTags[0][TagIds]', ','.join(replace_tag_ids))

        code, ret = await http.post(self.endpoint, params=params, headers=headers, aws_headers=aws_headers)

        if 199 < code < 300 and ret.get('Result', False):
            return ret.get('Result', False)

        raise Exception(f'replace_resources_tags faild. code:{code} ret:{ret}')

    @_prepare
    async def list_tags_id_by_resource_ids(self, resource_type, resource_uuids: list,
                                           account_id=None, params: dict = None, headers: dict = None, aws_headers=None):
        '''
        {
            "Tags": [
                {
                    "ResourceUuid": "08fe5ce7-839d-40b0-bab3-d2dd2ff81dc7",
                    "TagId": 28747,
                    "TagKey": "dylan",
                    "TagValue": "a"
                },
                {
                    "ResourceUuid": "4e8d15a6-ee52-44c8-a7c3-1efcb34c0851",
                    "TagId": 28747,
                    "TagKey": "dylan",
                    "TagValue": "a"
                },
                {
                    "ResourceUuid": "5c8257ff-90a6-4f59-92b3-f84711c13174",
                    "TagId": 28748,
                    "TagKey": "nalyd",
                    "TagValue": "1"
                },
                {
                    "ResourceUuid": "5c8257ff-90a6-4f59-92b3-f84711c13174",
                    "TagId": 28749,
                    "TagKey": "dylan",
                    "TagValue": "b"
                }
            ],
            "RequestId": "Meow!"
        }
        '''
        params.setdefault('Action', 'ListTagsByResourceIds')
        params.setdefault('ResourceType', resource_type)
        params.setdefault('ResourceUuids', ','.join(resource_uuids))

        code, ret = await http.post(self.endpoint, params=params, headers=headers, aws_headers=aws_headers)
        result = {}
        if 199 < code < 300:
            for tag in ret.get('Tags', []):
                tag_uuid = tag.get('ResourceUuid', '')
                if tag_uuid in result:
                    result[tag_uuid].append(tag.get('TagId', 0))
                else:
                    result[tag_uuid] = [tag.get('TagId', 0)]
            return result

        raise Exception(f'replace_resources_tags faild. code:{code} ret:{ret}')

    @_prepare
    async def list_tag_keys(self, account_id=None, params: dict = None, headers: dict = None, aws_headers=None):
        '''
        {
            "TagKeys": [
                "dylan",
                "kmr",
                "使用方",
                "是否长期",
                "华为大数据平台"
            ],
            "Page": 1,
            "PageSize": 0,
            "Total": 5,
            "RequestId": "Meow!"
        }
        '''
        params.setdefault('Action', 'ListTagKeys')

        code, ret = await http.post(self.endpoint, params=params, headers=headers, aws_headers=aws_headers)
        result = {}
        if 199 < code < 300:
            return ret

        raise Exception(f'list_tag_keys faild. code:{code} ret:{ret}')

    @_prepare
    async def list_tag_values(self, tag_keys, account_id=None, params: dict = None, headers: dict = None, aws_headers=None):
        '''

        '''
        params.setdefault('Action', 'ListTagValues')
        params.setdefault('TagKeys', tag_keys)

        code, ret = await http.post(self.endpoint, params=params, headers=headers, aws_headers=aws_headers)
        result = {}
        if 199 < code < 300:
            return ret

        raise Exception(f'list_tag_values faild. code:{code} ret:{ret}')

    @_prepare
    async def create_tag(self, key, value, account_id=None, params: dict = None, headers: dict = None, aws_headers=None):
        '''
        {
            "Result": true,
            "TagId": 16137,
            "RequestId": "Meow!"
        }
        '''
        params.setdefault('Action', 'CreateTag')
        params.setdefault('Key', key)
        params.setdefault('Value', value)

        code, ret = await http.post(self.endpoint, params=params, headers=headers, aws_headers=aws_headers)
        result = {}
        if 199 < code < 300:
            return ret

        raise Exception(f'list_tag_values faild. code:{code} ret:{ret}')

    @_prepare
    async def list_tag(self, tag_key=None, tag_value=None, account_id=None, params: dict = None, headers: dict = None, aws_headers=None):
        params.setdefault('Action', 'ListTags')
        if tag_key:
            params.setdefault('Key', tag_key)
        if tag_value:
            params.setdefault('Value', tag_value)

        code, ret = await http.post(self.endpoint, params=params, headers=headers, aws_headers=aws_headers)
        result = {}
        if 199 < code < 300:
            return ret

        raise Exception(f'list_tag_values faild. code:{code} ret:{ret}')

    @_prepare
    async def delete_tag(self, tags: list, account_id=None, params: dict = None, headers: dict = None, aws_headers=None):
        params.setdefault('Action', 'DeleteTag')
        for i, t in enumerate(tags):
            params.setdefault(f'Tags[{i}][Key]', t.get('Key', ''))
            params.setdefault(f'Tags[{i}][Value]', t.get('Value', ''))

        code, ret = await http.post(self.endpoint, params=params, headers=headers, aws_headers=aws_headers)

        if 199 < code < 300:
            if ret and "Result" in ret:
                return ret.get('Result', False)

        raise Exception(f'delete_tag faild. code:{code} ret:{ret}')

    async def validate_tag_ids(self, tags: list, account_id=None):
        '''
        重新检查，确认资源在标签平台存在
        这里用来确保绑定成功，与集群标签是否存在于标签系统无关。
        '''
        tag_ids = []
        for tag in tags:
            tag_key = tag.get('tag_key', '')
            tag_value = tag.get('tag_value', '')
            if tag_key and tag_value:
                real_tag_one = await self.list_tag(tag_key, tag_value, account_id=account_id)
                if real_tag_one:
                    for rtag in real_tag_one.get('Tags', []):
                        real_tag_id = rtag.get('Id', None)
                        real_tag_key = rtag.get('Key', None)
                        real_tag_value = rtag.get('Value', None)
                        # list_tag 是模糊匹配，需要进行完全匹配
                        if real_tag_key == tag_key and real_tag_value == tag_value and real_tag_id:
                            tag_ids.append(str(real_tag_id))

        return tag_ids

    async def get_cluster_default_key(self, cluster_id, cluster_type, account_id=None):
        '''
        方法没有对二次绑定进行系统tag校验，所以只提供给创建集群使用。
        '''

        result_tags = []
        result_tag_ids = []
        # 托管必填tag #平台约定 #模拟系统tag
        tag_params = {
            TagResource.SYS_CLUSTER_TAG.CLUSTER_TYPE: cluster_type,
            TagResource.SYS_CLUSTER_TAG.CLUSTER_ID: cluster_id,
        }

        for k, v in tag_params.items():
            temp_tag_id = None

            system_tags = await self.list_tag(k, v, account_id=account_id)

            if system_tags:
                for stag in system_tags.get('Tags', []):
                    sys_tag_id = stag.get('Id', None)
                    sys_tag_key = stag.get('Key', None)
                    sys_tag_value = stag.get('Value', None)
                    if sys_tag_key == k and sys_tag_value == v and sys_tag_id:
                        temp_tag_id = sys_tag_id

            if not temp_tag_id:
                new_tag = await self.create_tag(k, v, account_id=account_id)
                if new_tag and new_tag.get('TagId', ''):
                    temp_tag_id = new_tag.get('TagId', '')

            if temp_tag_id:
                result_tag_ids.append(str(temp_tag_id))
                result_tags.append({'tag_key': k, 'tag_value': v, 'tag_id': str(temp_tag_id)})

        return result_tags, result_tag_ids

    @_prepare
    async def detach_resource_tags(self, tagids: str, resource_type, resource_id, account_id=None, params: dict = None, headers: dict = None, aws_headers=None):
        params.setdefault('Action', 'DetachResourceTags')
        params.setdefault('ResourceType', resource_type)
        params.setdefault('ResourceUuid', resource_id)
        params.setdefault('TagIds', tagids)

        code, ret = await http.post(self.endpoint, params=params, headers=headers, aws_headers=aws_headers)
        result = {}
        if 199 < code < 300:
            if ret and "Result" in ret:
                return ret.get('Result', False)

        raise Exception(f'delete_tag faild. code:{code} ret:{ret}')
