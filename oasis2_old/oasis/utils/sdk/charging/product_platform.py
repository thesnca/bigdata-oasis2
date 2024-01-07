# -*- coding: utf-8 -*-

import copy
from datetime import datetime
import json
from random import random
import re

from conf.charge_conf import CHARGE_BILL_MAP
from conf.charge_conf import CN_REGIONG_MAP
from conf.charge_conf import CN_ROOM_MAP
from conf.charge_conf import EIP_CHARGE_BILL_MAP
from conf.charge_conf import MACHINE_ROOM_MAP
from conf.charge_conf import PRODUCT_GROUP_MAP
from conf.charge_conf import PRODUCT_TYPE_MAP
from conf.charge_conf import PRODUCT_USE_MAP
from conf.charge_conf import PRODUCT_WHAT_MAP
from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.generator import gen_uuid4


def _generate_batch_number():
    dt = datetime.now()
    res = f'{dt.strftime("%Y%m%d%H%M%S")}' \
          f'{int((random() * 9 + 1) * 100000)}'
    return res


def form_basic_items(product_use, charge_type, source):
    """

    :param product_use: 1-buy, 2-renew
    :param charge_type: 1-formal, 2-freetrial
    :param source: 1-console, 2-operating platform, 3-openapi
    :return: dic base_items
    """
    basic_items = {
        'productUse': PRODUCT_USE_MAP[product_use],
        'productWhat': PRODUCT_WHAT_MAP[charge_type],
        'source': source
    }
    return basic_items


def form_basic_items_from_ieip(product_use, charge_type, source):
    """

    :param product_use: 1-buy, 2-renew
    :param charge_type: 1-formal, 2-freetrial
    :param source: 1-console, 2-operating platform, 3-openapi
    :return: dic base_items
    """
    basic_items = {
        'productUse': PRODUCT_USE_MAP[product_use],
        'productWhat': charge_type,
        'source': source
    }
    return basic_items


# KEC开机器需要 EBS、EIP、SLB 的批次号
def form_kick_kec_extend_items(cluster_id):
    product_batch_number = _generate_batch_number()
    extend_items = [
        {
            'itemName': '商品批次号',
            'itemNo': 'productBatch',
            'unit': None,
            'value': product_batch_number
        },
        {
            'itemNo': 'clusterUid',
            'itemName': '集群Uid',
            'unit': None,
            'value': cluster_id
        }
    ]
    return extend_items


def form_extend_items(instance_group_type, cluster_type, cluster_id):
    product_batch_number = _generate_batch_number()
    extend_items = [
        {
            'itemName': '实例组类型',
            'itemNo': 'instanceGroupType',
            'unit': None,
            'value': instance_group_type
        },
        {
            'itemName': '商品批次号',
            'itemNo': 'productBatch',
            'unit': None,
            'value': product_batch_number
        },
        {
            'itemNo': 'clusterUid',
            'itemName': '集群Uid',
            'unit': None,
            'value': cluster_id
        }
    ]
    if cluster_type == 'KES':
        extend_items.append({
            'itemName': '产品类型',
            'itemNo': 'ProductType',
            'unit': None,
            'value': '206'
        })
    elif cluster_type == 'KHBASE':
        extend_items.append({
            'itemName': '产品类型',
            'itemNo': 'ProductType',
            'unit': None,
            'value': '216'
        })

    return extend_items


def form_service_items(basic_items, instance_group_type, count, availability_zone, flavor_code, machine_type,
                       charge_type, cluster_type, service_type, user_id, cluster_id, purchase_time, duration_unit=2,
                       project_id=0, origin_instance_id=None, origin_product_id=None):
    service_items = copy.deepcopy(basic_items)
    service_items['tag'] = instance_group_type
    service_items['num'] = count
    service_items['productName'] = cluster_type
    service_items['productType'] = PRODUCT_TYPE_MAP[service_type]
    service_items['region'] = MACHINE_ROOM_MAP[availability_zone]
    service_items['availabilityZone'] = availability_zone
    service_items['billType'] = CHARGE_BILL_MAP[charge_type]
    service_items['userId'] = user_id
    if origin_instance_id:
        service_items['instanceId'] = origin_instance_id

    if charge_type in ['Monthly', 'FreeTrial']:
        service_items['duration'] = purchase_time
        service_items['durationUnitDic'] = duration_unit

    display = [
        {
            'key': '数据中心',
            'value': CN_REGIONG_MAP[availability_zone]
        },
        {
            'key': '可用区',
            'value': CN_ROOM_MAP[availability_zone]
        },
        {
            'key': '主机规格',
            'value': machine_type
        },
        {
            'key': '服务费类型',
            'value': flavor_code
        }]

    items = [
        {
            'itemNo': 'iamProjectId',
            'itemName': 'iamProjectId',
            'unit': None,
            'value': project_id
        },
        {
            'itemName': '套餐类型',
            'itemNo': 'instanceType',
            'unit': None,
            'value': flavor_code
        },
        {
            'itemNo': 'display',
            'itemName': 'display',
            'unit': None,
            'value': json.dumps(display)
        }
    ]
    if origin_product_id:
        upgrade_items = [
            {
                'itemNo': 'pkgCalculateType',
                'itemName': '',
                'unit': '',
                'value': 'excludePriceItem'
            },
            {
                'itemNo': 'pre_product_id',
                'itemName': '原始商品ID',
                'unit': None,
                'unitCount': None,
                'value': origin_product_id
            }
        ]
        items.extend(upgrade_items)

    extend_items = form_extend_items(instance_group_type, cluster_type, cluster_id)
    for item in extend_items:
        items.append(item)

    service_items['items'] = items
    return service_items


def form_slb_items(basic_items, availability_zone, bill_type, cluster_id, project_id=0):
    slb_items = copy.deepcopy(basic_items)
    slb_items['num'] = 1
    slb_items['productName'] = 'SLB'
    slb_items['productType'] = PRODUCT_TYPE_MAP['SLB']
    slb_items['region'] = MACHINE_ROOM_MAP[availability_zone]
    slb_items['billType'] = bill_type
    slb_items['productGroup'] = PRODUCT_GROUP_MAP['SLB']
    # K系列 slb 未按统一实例生命周期处理。所以需要SLB 自动通知订单
    slb_items['autoNotify'] = 1
    slb_items['instanceType'] = 'classsic'
    # slb 不支持试用商品，所以productWhat 都是1
    slb_items['productWhat'] = 1

    slb_items['baseItems'] = [
        {
            'itemNo': 'iamProjectId',
            'itemName': 'iamProjectId',
            'unit': None,
            'value': str(project_id),
        },
    ]

    slb_items['extendItems'] = form_kick_kec_extend_items(cluster_id)
    return slb_items


def form_eip_items(basic_items, availability_zone, line_id, eip_instance_type, band, charge_type, eip_purchase_time,
                   cluster_id, duration_unit=2, enable_ieip=False, num=1, project_id=0):
    eip_items = copy.deepcopy(basic_items)
    eip_items['num'] = num
    eip_items['productName'] = 'EIP'
    if enable_ieip:
        eip_items['productType'] = PRODUCT_TYPE_MAP['IEIP']
    else:
        eip_items['productType'] = PRODUCT_TYPE_MAP['EIP']
    eip_items['region'] = MACHINE_ROOM_MAP[availability_zone]
    eip_items['billType'] = EIP_CHARGE_BILL_MAP[charge_type]
    eip_items['productGroup'] = PRODUCT_GROUP_MAP['EIP']
    eip_items['instanceType'] = eip_instance_type
    # K系列 EIP 未按统一实例生命周期处理。所以需要EIP 自动通知订单
    eip_items['autoNotify'] = 1
    if charge_type in ['PrePaidByMonth', 'FreeTrial']:
        eip_items['duration'] = eip_purchase_time
        eip_items['durationUnitDic'] = duration_unit

    base_items = [
        {
            'itemNo': 'lineId',
            'itemName': '链路类型ID',
            'unit': None,
            'value': line_id
        },
        {
            'itemNo': 'net',
            'itemName': '带宽',
            'unit': 'Mbps',
            'value': band
        },
        {
            'itemNo': 'iamProjectId',
            'itemName': 'iamProjectId',
            'unit': None,
            'value': str(project_id),
        },
    ]
    eip_items['baseItems'] = base_items
    eip_items['extendItems'] = form_kick_kec_extend_items(cluster_id)

    return eip_items


def form_kec_items(
        basic_items, instance_group_type, count, availability_zone, flavor_code,
        ssd_size, product_type, charge_type, cluster_type, cluster_id, purchase_time,
        duration_unit=2, project_id=0, system_disk_type=None, system_disk_size=None,
        local_disk=None, origin_instance_id=None, origin_product_id=None):
    kec_items = copy.deepcopy(basic_items)
    kec_items['tag'] = instance_group_type
    kec_items['num'] = count
    kec_items['productName'] = 'KEC'
    kec_items['productType'] = product_type
    kec_items['region'] = MACHINE_ROOM_MAP[availability_zone]
    kec_items['availabilityZone'] = availability_zone
    kec_items['billType'] = CHARGE_BILL_MAP[charge_type]
    kec_items['productGroup'] = PRODUCT_GROUP_MAP['KEC']
    kec_items['instanceType'] = flavor_code

    if origin_instance_id:
        kec_items['instanceId'] = origin_instance_id

    if charge_type in ['Monthly', 'FreeTrial']:
        kec_items['duration'] = purchase_time
        kec_items['durationUnitDic'] = duration_unit

    base_items = [
        {
            'itemNo': 'ssd',
            'itemName': 'SSD',
            'unit': 'GB',
            'value': ssd_size
        },
        {
            'itemNo': 'iamProjectId',
            'itemName': 'iamProjectId',
            'unit': None,
            'value': project_id
        },
        {
            'itemNo': 'systemDiskType',
            'itemName': '系统盘类型',
            'unit': None,
            'value': system_disk_type
        },
        {
            'itemNo': 'systemDiskSize',
            'itemName': '系统盘大小',
            'unit': None,
            'value': system_disk_size
        },
        {
            'itemNo': 'opstype',
            'itemName': '操作系统',
            'unit': None,
            'value': 2
        }
    ]

    if origin_product_id:
        upgrade_items = [
            {
                'itemNo': 'pkgCalculateType',
                'itemName': '',
                'unit': '',
                'value': 'excludePriceItem'
            },
            {
                'itemNo': 'pre_product_id',
                'itemName': '原始商品ID',
                'unit': None,
                'unitCount': None,
                'value': origin_product_id
            }
        ]
        base_items.extend(upgrade_items)

    if local_disk:
        # 4*7400-SATA HDD
        local_disk_count, local_disk_size, local_disk_type, local_disk_item = re.split(r'[ *-]\s*', local_disk)
        base_items.append({
            'itemNo': local_disk_item.lower(),
            'itemName': f'直连{local_disk_type}盘',
            'unit': 'GB',
            'value': int(local_disk_count) * int(local_disk_size)
        })

    kec_items['baseItems'] = base_items
    kec_items['extendItems'] = form_extend_items(instance_group_type, cluster_type, cluster_id)
    return kec_items


def form_epc_items(basic_items, instance_group_type, count, availability_zone, flavor_code, charge_type, cluster_type,
                   cluster_id, purchase_time, duration_unit=2, project_id=0):
    epc_items = copy.deepcopy(basic_items)
    epc_items['tag'] = instance_group_type
    epc_items['num'] = count
    epc_items['productName'] = 'EPC'
    epc_items['productType'] = PRODUCT_TYPE_MAP['EPC']
    epc_items['region'] = MACHINE_ROOM_MAP[availability_zone]
    epc_items['availabilityZone'] = availability_zone
    epc_items['billType'] = CHARGE_BILL_MAP[charge_type]
    epc_items['productGroup'] = PRODUCT_GROUP_MAP['EPC']
    epc_items['instanceType'] = flavor_code

    if charge_type in ['Monthly', 'FreeTrial']:
        epc_items['duration'] = purchase_time
        epc_items['durationUnitDic'] = duration_unit

    base_items = [
        {
            'itemNo': 'networkInterfaceMode',
            'itemName': '是否bond标识',
            'unit': None,
            'value': 'bond4'
        },
        {
            'itemNo': 'iamProjectId',
            'itemName': 'iamProjectId',
            'unit': None,
            'value': str(project_id)
        },
    ]
    epc_items['baseItems'] = base_items
    epc_items['extendItems'] = form_extend_items(instance_group_type, cluster_type, cluster_id)
    return epc_items


def form_ebs_items(basic_items, instance_group_type, count, availability_zone, ebs_type, volume_size, charge_type,
                   cluster_type, cluster_id, purchase_time, duration_unit=2, project_id=0,
                   origin_instance_id=None, origin_product_id=None):
    ebs_items = copy.deepcopy(basic_items)
    ebs_items['tag'] = instance_group_type
    ebs_items['num'] = count
    ebs_items['productName'] = 'EBS'
    ebs_items['productType'] = PRODUCT_TYPE_MAP['EBS']
    ebs_items['region'] = MACHINE_ROOM_MAP[availability_zone]
    ebs_items['availabilityZone'] = availability_zone
    ebs_items['billType'] = CHARGE_BILL_MAP[charge_type]
    ebs_items['productGroup'] = PRODUCT_GROUP_MAP['EBS']
    ebs_items['instanceType'] = ebs_type
    if origin_instance_id:
        ebs_items['instanceId'] = origin_instance_id

    if charge_type in ['Monthly', 'FreeTrial']:
        ebs_items['duration'] = purchase_time
        ebs_items['durationUnitDic'] = duration_unit

    base_items = [
        {
            'itemNo': 'iamProjectId',
            'itemName': '项目组ID',
            'unit': None,
            'value': str(project_id)
        },
        {
            'itemNo': 'disk',
            'itemName': '容量',
            'unit': 'G',
            'value': str(volume_size)
        }
    ]

    if origin_product_id:
        upgrade_items = [
            {
                'itemNo': 'pkgCalculateType',
                'itemName': '',
                'unit': '',
                'value': 'excludePriceItem'
            },
            {
                'itemNo': 'pre_product_id',
                'itemName': '原始商品ID',
                'unit': None,
                'unitCount': None,
                'value': origin_product_id
            }
        ]
        base_items.extend(upgrade_items)

    ebs_items['baseItems'] = base_items
    ebs_items['extendItems'] = form_extend_items(instance_group_type, cluster_type, cluster_id)

    return ebs_items


def _prepare(func):
    async def __inner(*args, **kwargs):
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-KSC-REQUEST-ID': gen_uuid4(),
        }

        return await func(headers=headers, *args, **kwargs)

    return __inner


class ProductClient:
    def __init__(self):
        self.product_uri_map = {
            k.replace('_', ''): config.get('charge', f'{k}product_uri')
            for k in ['', 'eip_', 'ebs_', 'kec_', 'epc_']
        }

    @_prepare
    async def create_product(self, items, headers=None):

        code, ret = await http.post(f'{self.product_uri_map.get("")}/product/createProduct',
                                    data=items, headers=headers)

        if 199 < code < 300:
            data = ret.get('data', {})
            res = {
                'product_id': data['productId'],
                'price': data['price'],
                'product_group': data['productGroup']
            }
            return res
        return None

    async def find_item_by_product(self, product_id, item):
        params = {
            'productId': product_id
        }
        res = None
        code, ret = await http.get(f'{self.product_uri_map.get("")}/product/findProduct', params=params)

        if 199 < code < 300:
            data = ret.get('data', {})
            for ite in data.get('items', []):
                if ite['itemNo'] == item:
                    res = ite['value']
        return res

    async def find_items_by_product(self, product_id, item_keys):
        params = {
            'productId': product_id
        }
        res = dict()
        code, ret = await http.get(f'{self.product_uri_map.get("")}/product/findProduct', params=params)

        if 199 < code < 300:
            data = ret.get('data', {})
            res = {ite.get('itemNo', None): ite.get('value', None)
                   for ite in data.get('items', [])
                   if ite['itemNo'] in item_keys}
        return res

    @_prepare
    async def create_kec_product(self, items, account_id, region, headers=None):
        headers.update({
            'X-KSC-ACCOUNT-ID': account_id,
            'X-KSC-REGION': region,
        })
        code, ret = await http.post(f'{self.product_uri_map.get("kec")}/?Action=CreateProduct',
                                    data=items, headers=headers)

        if 199 < code < 300:
            res = {
                'product_id': ret['ProductId'],
                'price': ret['Price'],
                'product_group': PRODUCT_GROUP_MAP['KEC']
            }
            return res
        raise Exception(f'KEC create product failed, return: {ret}')

    @_prepare
    async def create_epc_product(self, items, account_id, region, headers=None):
        headers.update({
            'X-KSC-ACCOUNT-ID': account_id,
            'X-KSC-REGION': region,
        })
        code, ret = await http.post(f'{self.product_uri_map.get("epc")}/?Action=CreateProduct',
                                    data=items, headers=headers)

        if 199 < code < 300:
            res = {
                'product_id': ret['ProductId'],
                'price': ret['Price'],
                'product_group': PRODUCT_GROUP_MAP['KEC']
            }
            return res
        raise Exception(f'EPC create product failed, return: {ret}')

    @_prepare
    async def create_ebs_product(self, items, account_id, region, cluster_type, headers=None):
        headers.update({
            'X-KSC-ACCOUNT-ID': account_id,
            'X-KSC-REGION': region,
            'X-KSC-SOURCE': cluster_type.lower(),
        })
        code, ret = await http.post(f'{self.product_uri_map.get("ebs")}/mq?Action=CreateProduct',
                                    data=items, headers=headers)

        if 199 < code < 300:
            res = {
                'product_id': ret['ProductId'],
                'price': ret['Price'],
                'product_group': PRODUCT_GROUP_MAP['EBS']
            }
            return res
        raise Exception(f'EBS create product failed, return: {ret}')

    @_prepare
    async def create_eip_product(self, items, account_id, region, product_type='EIP', headers=None):
        headers.update({
            'X-KSC-ACCOUNT-ID': account_id,
            'X-KSC-REGION': region,
        })
        code, ret = await http.post(f'{self.product_uri_map.get("eip")}/?Action=CreateProduct',
                                    data=items, headers=headers)

        if 199 < code < 300:
            res = {
                'product_id': ret['ProductId'],
                'price': ret['Price'],
                'product_group': PRODUCT_GROUP_MAP[product_type]
            }
            return res
        raise Exception(f'EIP create product failed, return: {ret}')
