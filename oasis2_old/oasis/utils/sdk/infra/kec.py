from conf.infra_conf import KEC_API_MAP
from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.convert import list2dict
from oasis.utils.exceptions import KecRequestException
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger
from oasis.utils.poll_util import wait_until_complete


def _prepare(func):
    async def __inner(self, *args, **kwargs):
        account_id = kwargs.pop('account_id', None)
        if not account_id:
            raise Exception(f'Please specify account_id, got {account_id}')
        request_id = kwargs.pop('request_id', gen_uuid4())

        # aws_auth = gen_aws_auth_header(self.ak, self.sk, self.product)
        headers = {
            'X-Ksc-Region': self.region,
            'X-Ksc-Request-Id': request_id,
            'X-Ksc-Account-Id': account_id,
            'X-KSC-SOURCE': self.product,
            'X-KSC-SK': self.ksc_sk,
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            # 'Authorization': aws_auth,
        }
        params = {
            'Version': self.version,
            'AccountId': account_id,
        }

        return await func(self, params=params, headers=headers, *args, **kwargs)

    return __inner


class KecClient:
    def __init__(self, product):
        self.product = product
        self.endpoint = config.get('infra', 'kec_endpoint')
        self.ksc_sk = config.get('ksc', product)
        self.region = config.get('infra', 'region')
        self.version = config.get('infra', 'kec_version')
        self.ak = config.get('iam', f'{product}_ak')
        self.sk = config.get('iam', f'{product}_sk')

    @_prepare
    async def notify_suborder_status(self, instance_id, kec_sub_order_id,
                                     status, owner_product_group=None, owner_instance_id=None, *,
                                     params: dict = None, headers: dict = None, account_id=None):
        params.setdefault('Action', KEC_API_MAP['notify'])
        data = {
            'instanceId': instance_id,
            'subOrderId': kec_sub_order_id,
            'status': status,
        }

        if owner_product_group:
            data.setdefault('ownerProductGroup', owner_product_group)
        if owner_instance_id:
            data.setdefault('ownerInstanceId', owner_instance_id)

        code, ret = await http.post(self.endpoint, params=params, data=data, headers=headers)

        if 199 < code < 300:
            return True
        else:
            return False

    @_prepare
    async def create_instances(self, instances, order_id, *, account_id=None,
                               params: dict = None, headers: dict = None):
        params.setdefault('Action', KEC_API_MAP['create_instance'])
        instances_lst = sorted(instances, key=lambda x: int(x['ProductBatchNo']))
        for i in instances_lst:
            i.pop('ProductBatchNo', None)
            #i['LiveUpgradeSupport'] = "true"  # 兼容银河支持热升配（主要还是版本不一样，现在主机业务 和公有云代码存在差不多2年的差别）
            if i['ChargeType'] != 'Monthly':
                i['PurchaseTime'] = 0
            if i['ChargeType'] == 'Minutely':
                i['ChargeType'] = 'HourlyInstantSettlement'
            if i['ChargeType'] == 'FreeTrial':
                i['ChargeType'] = 'Daily'

        data = {
            'InstanceList': instances_lst,
            'InstanceTrade': {'OrderId': order_id},
        }

        code, ret = await http.post(self.endpoint, params=params, data=data, headers=headers)
        # code, ret = await mock_request_fail('http://10.69.72.79:28257/kec')

        if 199 < code < 300:
            instances_set = ret.get('InstancesSet', None)
            if instances_set:
                return instances_set
            return []
        raise KecRequestException(f'create kec failed, params: {params}, ret: {ret}')

    @_prepare
    async def describe_instances(self, instance_ids, params: dict = None,
                                 headers: dict = None, account_id=None, ):
        '''
        ("building", "创建中"),
        ("active", "运行中"),
        ("inactive", "健康检查失败"),
        ("stopping", "关闭中"),
        ("stopped", "已关闭"),
        ("starting", "启动中"),
        ("restarting", "重启中"),
        ("backuping", "镜像中"),
        ("resizing", "升级中"),
        ("csresize-error", "升级失败"),
        ("invalid", "已停用"),
        ("deleting", "删除中"),
        ("deleted", "已删除"),
        ("error", "创建失败"),
        ("overriding", "重装中"),
        ("pre_migrating", "预迁移中"),
        ("pre_migrating_error", "预迁移失败"),
        ("migrating", "在线升配中"),
        ("migrating_op", "OP迁移中"),
        ("migrate", "迁移中"),
        ("migrating_error", "在线升配失败"),
        ("  ", "离线升配失败"),
        ("resize_error_local", "本地升配失败"),
        ("migrating_success", "在线升配成功"),
        ("migrating_success_off_line", "离线升配成功"),
        ("resize_success_local", "本地升配成功"),
        ("recycling", "回收中"),
        ("rebooting", "重启中"),
        ("rebooting_hard", "强制重启中"),
        ("drg_migrating","云服务器迁入容灾组中"),
        ("drg_migrating_error","云服务器迁入容灾组失败"),
        ("power_on_error", "开机失败"),
        ("resize_prep","调整配置"),//离线迁移初始状态
        ("resize_migrating","离线迁移中"),
        ("resize_migrated","已离线迁移"),
        ("resize_finish","离线迁移完成"),
        ("cross_queue","跨实例迁移排队中"),
        ("cross_migrating","跨实例迁移中"),
        ("cross_finish","跨实例迁移完成"),
        ("detach_key_pair_failed","解绑秘钥失败"),
        ("attach_key_pair_failed","绑定秘钥失败"),
        ("cross_error","跨实例迁移失败"),
        ("waiting_volume_attached", "预创建状态"),
        ("net_card_loading","网卡挂载中"),
        ("net_card_unloading","网卡卸载中"),
        ("extending_volume", "扩容中")
        '''
        params.setdefault('Action', KEC_API_MAP['describe_instance'])

        if len(instance_ids) > 5:
            params.setdefault('MaxResults', len(instance_ids))
        params.update(list2dict('InstanceId', instance_ids))

        code, ret = await http.get(self.endpoint, params=params, headers=headers)

        res = []
        if 199 < code < 300:
            instances_set = ret.get('InstancesSet', [])
            for h in instances_set:
                result = {
                    'InstanceState': h.get('InstanceState', {}).get('Name', None),
                    'InstanceId': h.get('InstanceId', None),
                    'InstanceConfigure': h.get('InstanceConfigure', None),
                    'InstanceName': h.get('InstanceName', None),
                    'InstanceType': h.get('InstanceType', None),
                    'PrivateIpAddress': h.get('NetworkInterfaceSet', [{}])[0].get('PrivateIpAddress', None),
                    'SecurityGroupId': h.get('NetworkInterfaceSet', [{}])[0].get('SecurityGroupSet', [{}])[0].get(
                        'SecurityGroupId', None),
                }
                res.append(result)
            return res
        raise KecRequestException(f'describe kec failed, instance_ids: {instance_ids}, return: {ret}')

    @_prepare
    async def delete_instances(self, instance_ids, account_id=None,
                               params: dict = None, headers: dict = None):
        if not instance_ids:
            return True
        params.setdefault('Action', KEC_API_MAP['delete_instance'])
        params.setdefault('ForceDelete', 'true')  # https://wiki.op.ksyun.com/pages/viewpage.action?pageId=124369403

        params.update(list2dict('InstanceId', instance_ids))
        code, ret = await http.post(self.endpoint, params=params, headers=headers)

        if 199 < code < 300:
            instances_set = ret.get('InstancesSet', [])
            result = [{
                'InstanceId': h.get('InstanceId', None),
                'Return': h.get('Return', None),
            } for h in instances_set]
            return result
        raise KecRequestException(f'delete kec failed, instance_ids: {instance_ids}, return: {ret}')

    @_prepare
    async def start_instances(self, instance_ids, account_id=None,
                              params: dict = None, headers: dict = None):
        params.setdefault('Action', KEC_API_MAP['start_instance'])
        params.update(list2dict('InstanceId', instance_ids))

        code, ret = await http.get(self.endpoint, params=params, headers=headers)

        if 199 < code < 300:
            instances_set = ret.get('InstancesSet', [])
            result = [{
                'InstanceId': h.get('InstanceId', None),
                'Return': h.get('Return', None),
            } for h in instances_set]
            return result
        raise KecRequestException(f'Start kec failed, instance_ids: {instance_ids}, return: {ret}')

    @_prepare
    async def stop_instances(self, instance_ids, account_id=None,
                             params: dict = None, headers: dict = None):
        params.setdefault('Action', KEC_API_MAP['stop_instance'])
        params.update(list2dict('InstanceId', instance_ids))

        code, ret = await http.get(self.endpoint, params=params, headers=headers)

        if 199 < code < 300:
            instances_set = ret.get('InstancesSet', [])
            result = [{
                'InstanceId': h.get('InstanceId', None),
                'Return': h.get('Return', None),
            } for h in instances_set]
            return result
        raise KecRequestException(f'Stop kec failed, instance_ids: {instance_ids}, return: {ret}')

    @_prepare
    async def reboot_instances(self, instance_ids, account_id=None, force_reboot='false',
                               params: dict = None, headers: dict = None):
        params.setdefault('Action', KEC_API_MAP['reboot_instance'])
        params.update(list2dict('InstanceId', instance_ids))
        if force_reboot == 'true':
            params.setdefault('ForceReboot', force_reboot)

        code, ret = await http.get(self.endpoint, params=params, headers=headers)

        res = []
        if 199 < code < 300:
            instances_set = ret.get('InstancesSet', None)
            for h in instances_set:
                result = dict()
                result['InstanceId'] = h.get('InstanceId', None)
                result['Return'] = h.get('Return', None)
                res.append(result)
            return res
        else:
            logger.warn(f'kec reboot instances failed: code {code}, ret {res}')
            raise KecRequestException('reboot kec failed')

    async def check_create_active(self, instance_ids, account_id=None, flag_state='active', ):
        _pinstances = await self.describe_instances(instance_ids, account_id=account_id)
        _ac_lst = []
        for _in in _pinstances:
            _istate = _in.get('InstanceState', 'unknown')
            if flag_state == _istate:
                _ac_lst.append(_in)
            elif 'error' == _istate:
                _errmsg = 'Node %s has error status, kec instance_id: %s' % (
                    _in.get('InstanceName', ''), _in.get('InstanceId'))
                logger.warning(_errmsg)
                raise KecRequestException(_errmsg)
        return _ac_lst

    @wait_until_complete()
    async def wait_create_active(self, instance_ids, account_id=None, flag_state='active'):
        if not instance_ids:
            return True
        ac_lst = await self.check_create_active(instance_ids, account_id=account_id, flag_state=flag_state)
        return len(instance_ids) == len(ac_lst)

    @wait_until_complete()
    async def wait_instances_delete(self, instance_ids, account_id=None):
        if not instance_ids:
            return True
        _pinstances = await self.describe_instances(instance_ids, account_id=account_id)
        return len(_pinstances) == 0

    @wait_until_complete()
    async def wait_instance_upgrade(self, instance_id, upgrade_instance_type, account_id=None):
        '''
        respose的状态码返回值是否为200
        对于没有触发实例迁移的主机，检查主机状态是否为resize_success_local
        对于触发了实例迁移的主机，检查主机状态是否为migrating_success_off_line或migrating_success
        对于热升配的主机，检查升配后主机是否直接生效
        检查实例套餐是否为更配后的套餐
        '''
        _pinstances = await self.describe_instances([instance_id], account_id=account_id)
        instance_result = _pinstances[0]

        result_state = instance_result.get('InstanceState', 'unknown')
        result_instance_type = instance_result.get('InstanceType')

        if upgrade_instance_type == result_instance_type and result_state in [
                'migrating_success', 'migrating_success_off_line', 'resize_success_local', 'Active']:
            return True
        elif result_state in ['error', 'migrating_error', 'migrating_error_off_line']:
            _errmsg = 'Node %s has error status, kec instance_id: %s' % (
                instance_result.get('InstanceName', ''), instance_result.get('InstanceId'))
            logger.warning(_errmsg)
            raise KecRequestException(_errmsg)

        return False

    async def check_instance_upgrade(self, instance_id, upgrade_instance_type, account_id=None):

        _pinstances = await self.describe_instances([instance_id], account_id=account_id)
        instance_result = _pinstances[0]

        result_state = instance_result.get('InstanceState', 'unknown')
        result_instance_type = instance_result.get('InstanceType')

        if upgrade_instance_type != result_instance_type and result_state == 'active':
            return True
        elif result_state in ['error', 'migrating_error', 'migrating_error_off_line']:
            _errmsg = 'Node %s has error status, kec instance_id: %s' % (
                instance_result.get('InstanceName', ''), instance_result.get('InstanceId'))
            logger.warning(_errmsg)
            raise KecRequestException(_errmsg)
        # 说明不需要做升配
        return False

    @_prepare
    async def create_data_guard_group(self, prefix, account_id=None,
                                      params: dict = None, headers: dict = None):
        params.setdefault('DataGuardName', f'{prefix}-{gen_uuid4()[:8]}')
        params.setdefault('Action', KEC_API_MAP['create_data_guard_group'])

        code, ret = await http.get(self.endpoint, params=params, headers=headers)

        if 199 < code < 300:
            data_guard_id = ret['DataGuardId']
            return data_guard_id
        else:
            logger.error(f'kec create data guard group failed, return: {ret}')
            raise KecRequestException(f'create data guard group failed, return: {ret}')

    @_prepare
    async def delete_data_guard_group(self, guard_ids, account_id=None,
                                      params: dict = None, headers: dict = None):
        if not guard_ids:
            return True
        params.update(list2dict('DataGuardId', guard_ids))
        params.setdefault('Action', KEC_API_MAP['delete_data_guard_group'])

        code, ret = await http.get(self.endpoint, params=params, headers=headers)

        if 199 < code < 300:
            return ret['ReturnSet']
        else:
            logger.warn('kec delete data guard group failed')
            raise KecRequestException('delete data guard group failed')

    async def get_private_ip_address(self, instance_id, account_id=None):
        instance_ids = [instance_id]
        ret = await self.describe_instances(instance_ids, account_id)
        if len(ret) == 1 and 'PrivateIpAddress' in ret[0]:
            return ret[0]['PrivateIpAddress']
        else:
            return None

    @_prepare
    async def modify_network_interface(self, instance_id, network_interface_id,
                                       security_group_id, account_id=None,
                                       params: dict = None, headers: dict = None):
        params.setdefault('Action', 'ModifyNetworkInterfaceAttribute')
        params.setdefault('NetworkInterfaceId', network_interface_id)
        params.setdefault('InstanceId', instance_id)
        params.setdefault('SecurityGroupId', security_group_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers)

        if 199 < code < 300:
            return ret
        return None

    @_prepare
    async def modify_instance_type(self, instance_id, instance_type, account_id=None,
                                   params: dict = None, headers: dict = None,
                                   auto_notify=1, data_disk_gb=None):
        '''
        http://KEC业务线OpenAPI接口文档/kec-doc/markdown/kec/openApi/Instance/Instance/Instance.html#10309
        # TODO 目前升配时，订单管理从KEC业务线，移交给了KES/KMR(IsModifyInstanceAttribute)
        # 这并不是一个好的方案，但不违反'实例生命周期'
        '''
        params.setdefault('Action', KEC_API_MAP['modify_instance'])
        params.setdefault('InstanceId', instance_id)
        params.setdefault('InstanceType', instance_type)
        params.setdefault('AutoNotify', auto_notify)  # 1: KEC自动回写订单
        if int(auto_notify) == 0:
            # 是否真正执行升配,默认值是false，不开放参数，订单回调值应该设置为true
            params.setdefault('IsModifyInstanceAttribute', 'true')

        if data_disk_gb and int(data_disk_gb) > 0:
            params.setdefault('DataDiskGb', int(data_disk_gb))

        code, ret = await http.get(self.endpoint, params=params, headers=headers)

        if 199 < code < 300:
            return ret
        return None

    @wait_until_complete()
    async def wait_reboot_instances(self, instance_ids, account_id=None,
                                    force_reboot='false'):
        if not instance_ids:
            return True

        res = await self.reboot_instances(instance_ids,
                                          account_id=account_id,
                                          force_reboot=force_reboot)
        change_instance_state = res[0].get('Return', False)

        return change_instance_state == True
