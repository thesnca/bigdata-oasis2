from copy import deepcopy
from datetime import datetime

from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.convert import list2dict
from oasis.utils.exceptions import EbsRequestException
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger
from oasis.utils.poll_util import wait_until_complete
from oasis.utils.sdk.infra.mock import mock_request_fail


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
        }

        return await func(self, params=params, headers=headers, *args, **kwargs)

    return __inner


class EbsClient(object):

    class VolumeStatus:
        CREATING = 'creating'  # 创建中
        AVAILABLE = 'available'  # 未挂载
        ATTACHING = 'attaching'  # 挂载中
        INUSE = 'in-use'  # 使用中（即已挂载）
        DETACHING = 'detaching'  # 卸载中
        EXTENDING = 'extending'  # 扩容中
        DELETING = 'deleting'  # 删除中
        ERROR = 'error'  # 异常

    def __init__(self, product):
        self.product = product
        self.endpoint = config.get('infra', 'ebs_endpoint')
        self.ksc_sk = config.get('ksc', product)
        self.region = config.get('infra', 'region')
        self.ak = config.get('iam', f'{product}_ak')
        self.sk = config.get('iam', f'{product}_sk')

    @_prepare
    async def create_ebs(self, ebs_orders, charge_type: str, az, purchase_time=0,
                         account_id=None, params: dict = None, headers: dict = None):
        if len(ebs_orders) == 0:
            return {}
        charge = charge_type
        if "Minutely" == charge:
            charge = 'HourlyInstantSettlement'
        elif "FreeTrial" == charge:
            charge = 'Daily'

        params.setdefault('Action', 'CreateVolume')
        params.setdefault('ChargeType', charge)
        params.setdefault('AvailabilityZone', az)
        if charge_type == 'Monthly':
            params.setdefault('PurchaseTime', str(purchase_time))

        ebs_dict = {}
        for key in ebs_orders:
            ebs_instances = []
            volume_name = f'{self.product}_{datetime.now().strftime("%Y%m%d%H%M%S")}'
            volume_type = key.split("|")[0]
            volume_size = key.split("|")[1]
            for sub_order in ebs_orders[key]:
                n_params = deepcopy(params)
                n_params.setdefault('VolumeType', volume_type)
                n_params.setdefault('Size', volume_size)
                n_params.setdefault('SubOrderId', sub_order)
                n_params.setdefault('VolumeName', volume_name)
                code, ret = await http.get(self.endpoint, params=n_params, headers=headers)
                # code, ret = await mock_request_fail('http://10.69.72.79:28257/ebs')

                if 199 < code < 300:
                    if ret.get('VolumeId'):
                        ebs = ret.get('VolumeId')
                        ebs_instances.append(ebs)
                        continue
                raise EbsRequestException(f'EBS create failed, ebs_orders: {ebs_orders}, return: {ret}')
            ebs_dict[key] = ebs_instances
        return ebs_dict

    async def delete_ebs(self, volume_id, *,
                         account_id=None, params: dict = None,
                         headers: dict = None):
        volume_status = await self._get_ebs_status(volume_id, account_id=account_id)

        if not volume_status:
            logger.info(f'Volume {volume_id} already deleted.')
            return True

        if volume_status == 'in-use':
            await self.detach_ebs(volume_id, account_id=account_id)

            volume_status = await self.wait_ebs_status(
                volume_id, ['available', 'error', 'recycling'],
                account_id=account_id)

        if volume_status in ['available', 'error', 'recycling']:
            return await self._delete_volume(volume_id, account_id=account_id)

    @wait_until_complete(timeout=240, interval=5)
    async def wait_ebs_status(self, volume_id, expect_status: list,
                              unexpect_status: list = None,
                              account_id=None):
        volume_status = await self._get_ebs_status(volume_id, account_id=account_id)
        if volume_status in expect_status:
            logger.info(f'Volume {volume_id} status is {volume_status}.')
            return volume_status
        elif unexpect_status and volume_status in unexpect_status:
            raise Exception(f'Volume {volume_id} in unexpected status {volume_status}.')
        return False

    @wait_until_complete(timeout=600, interval=10)
    async def wait_ebs_upgrade(self, volume_id, upgrade_volume_size,
                               account_id=None):
        volume_info = await self._get_ebs_info(volume_id, account_id=account_id)
        volume_info_stats = volume_info.get('VolumeStatus', None)
        volume_info_size = int(volume_info.get('Size', 0))
        if volume_info_size == int(upgrade_volume_size) and volume_info_stats == 'in-use':
            return True
        elif volume_info_stats == 'error':
            raise Exception(f'Volume {volume_id} in unexpected status {volume_info_stats}.')

        logger.info(f'Volume {volume_id} status is {volume_info_stats}.')
        return False

    @_prepare
    async def _delete_volume(self, volume_id, *, account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', 'DeleteVolume')
        params.setdefault('ForceDelete', 'true')
        params.setdefault('VolumeId', volume_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers)
        if 199 < code < 300:
            return ret.get('Return', None)
        raise Exception(f'Delete volume failed, volume id : {volume_id}, '
                        f'return: {ret}')

    @_prepare
    async def recover_ebs(self, instance,
                          account_id=None, params: dict = None, headers: dict = None):
        for volume_id in instance.volumes:
            params.setdefault('Action', 'RecoveryVolume')
            params.setdefault('VolumeId', volume_id)

            code, ret = await http.get(self.endpoint, params=params, headers=headers)
            if 199 < code < 300:
                if ret.get('Return'):
                    ret.get('Return')
            else:
                logger.warn(f'EBS recover failed, order id :[{volume_id}],error message: {ret.get("Error")}')
                return False
        return True

    @_prepare
    async def attach_ebs(self, instance_id, volume_id,
                         account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', 'AttachVolume')
        n_params = deepcopy(params)
        n_params.setdefault('VolumeId', volume_id)
        n_params.setdefault('InstanceId', instance_id)
        code, ret = await http.get(self.endpoint, params=n_params, headers=headers)

        if 199 < code < 300:
            return ret.get('Return', None)
        err_msg = ret.get('Error', None)
        raise Exception(f'EBS attach error, volume id :[{volume_id}], error: {err_msg}')

    @_prepare
    async def get_mount_point(self, volumes,
                              account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', 'DescribeVolumes')
        params.update(list2dict('VolumeId', volumes))

        code, ret = await http.get(self.endpoint, params=params, headers=headers)

        mount_points = []
        if 199 < code < 300:
            ret_volumes = ret.get('Volumes', [])
            for ret_volume in ret_volumes:
                attachments = ret_volume.get('Attachment', [])
                for attachment in attachments:
                    mount_point = attachment.get('MountPoint', None)
                    if not mount_point:
                        raise Exception(f'EBS get mount point failed, return: {ret_volume}')
                    mount_points.append(mount_point)
            return mount_points
        raise Exception(f'EBS get mount point failed, return: {ret}')

    @_prepare
    async def _get_ebs_status(self, volume_id, *,
                              account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', 'DescribeVolumes')
        params.setdefault('VolumeId.1', volume_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers)

        volumes = ret.get('Volumes', None)
        if volumes:
            return volumes[0].get('VolumeStatus', None)
        return None

    @_prepare
    async def _get_ebs_info(self, volume_id, *,
                            account_id=None, params: dict = None, headers: dict = None):
        params.setdefault('Action', 'DescribeVolumes')
        params.setdefault('VolumeId.1', volume_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers)

        volumes = ret.get('Volumes', None)
        if volumes:
            return volumes[0]
        return None

    async def check_ebs_upgrade(self, volume_id, account_id=None):

        volume_status = await self._get_ebs_status(volume_id, account_id=account_id)

        if EbsClient.VolumeStatus.INUSE == volume_status:
            # 可以升配
            return True
        elif EbsClient.VolumeStatus.ERROR == volume_status:
            _errmsg = f'ebs has error status, volume id {volume_id}'
            logger.warning(_errmsg)
            raise EbsRequestException(_errmsg)

        # 不可以升配
        return False

    @_prepare
    async def detach_ebs(self, volume_id, account_id=None,
                         params: dict = None, headers: dict = None):
        params.setdefault('Action', 'DetachVolume')
        params.setdefault('VolumeId', volume_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers)

        if 199 < code < 300:
            return True
        raise Exception(f'EBS detach failed, volume id :{volume_id}, error message: {ret} ')

    @_prepare
    async def resize_ebs(self, volume_id, volume_size, account_id=None,
                         params: dict = None, headers: dict = None, sub_order_id=None):
        params.setdefault('Action', 'ResizeVolume')
        params.setdefault('VolumeId', volume_id)
        params.setdefault('Size', str(volume_size))
        params.setdefault('OnlineResize', 'true')
        if sub_order_id:
            params.setdefault('SubOrderId', sub_order_id)

        code, ret = await http.get(self.endpoint, params=params, headers=headers)

        if 199 < code < 300:
            return True
        else:
            logger.warn("EBS resize failed, volume id: [%s], error message: %s"
                        % (volume_id, ret.get('Error')))
            return False

    @_prepare
    async def notify_suborder_status_ebs(self, volume_id, ebs_sub_order_id, status,
                                         owner_product_group=None, owner_instance_id=None,
                                         account_id=None, params: dict = None, headers: dict = None):

        api = '/mq'
        params.setdefault('Action', 'NotifySubOrderStatus')
        data = {
            'subOrderId': str(ebs_sub_order_id),
            'status': status,
            'instanceId': str(volume_id),
        }

        if owner_product_group:
            data.setdefault('ownerProductGroup', owner_product_group)
        if owner_instance_id:
            data.setdefault('ownerInstanceId', owner_instance_id)

        code, ret = await http.post(f'{self.endpoint}{api}', params=params, data=data, headers=headers)

        if 199 < code < 300:
            return True
        else:
            logger.warn("EBS notify_suborder_status_ebs, volume id: [%s], error message: %s"
                        % (volume_id, ret.get('Error')))
            return False
