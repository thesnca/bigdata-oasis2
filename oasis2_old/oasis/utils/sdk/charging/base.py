# -*- coding: utf-8 -*-
import base64

from conf.infra_conf import VOLUME_TYPE_MAP
from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.utils.config import base_kec_userdata_conf
from oasis.utils.config import config
from oasis.utils.generator import gen_uuid4
from oasis.utils.generator import generate_instance_name, validate_instance_type_code
from oasis.utils.logger import logger
from oasis.utils.sdk import charge_client
from oasis.utils.sdk import price_client
from oasis.utils.sdk import product_client
from oasis.utils.sdk.charging import product_platform


def get_resource_instance_type(resource_type, instance_type, service_type, product_details):
    """
    :param product_details:
    :param resource_type: KEC|EPC
    :param instance_type: HBase.normal.4C8G
    :param service_type: KES_SSD|KES_EBS|KES_EPC|KHBase_SSD|KHBase_EBS|KHBase_EPC
    :return:
    """
    # logger.info('=========================product_details: %s', service_type)
    # logger.info('=========================product_details: %s', product_details)
    if resource_type == 'KEC':
        types = product_details.get(service_type, {}).get(instance_type, {})
        flavor_code = f'{types.get("kec_type")}.{types.get("kec_ebs")}'
        # logger.info('============================flavor_code: %s', flavor_code)
        return flavor_code
    return None


def replace_service_type(cluster_type, resource_type, volume_type):
    result = ''

    if cluster_type == 'KHBASE':
        cluster_type = 'KHBase'

    if resource_type == 'EPC':
        result = f'{cluster_type}_{resource_type}'
    # elif cluster_type == 'KES':
    #     result = f'{cluster_type}_EBS'
    else:
        if volume_type.startswith('CLOUD_'):
            result = f'{cluster_type}_EBS'
        elif volume_type.startswith('LOCAL_'):
            result = volume_type.replace('LOCAL_', f'{cluster_type}_')
        elif volume_type.startswith('local_'):
            result = volume_type.replace('local_', f'{cluster_type}_')


    return result


async def create_cluster_order(order_product_details, cluster_type, *, account_id=None):
    product_vals = []
    for key, val in order_product_details.items():
        if key in ['EIP', 'SLB']:
            product_vals.append(val)
        else:
            for instance_group_val in val.values():
                product_vals.append(instance_group_val)

    order_products = [{
        'productId': val.get('product_id', None),
        'num': val.get('num', None),
        'productGroup': val.get('product_group', None),
    } for val in product_vals]

    order_id = await charge_client.create_order(account_id, 'BUY', order_products, cluster_type)
    logger.info(f'create_cluster_order ==order_id: {order_id}')
    return order_id


def form_epc_param(cluster, instance_group, sub_order_id, sec_group_id=None,
                   idx=1, host_type='SSD', kes_agent=False):
    host_suffix = f'{cluster.cluster_type.lower()}-epc-'
    epc_image_id = f'epc-{cluster.distribution_version}'
    resource_attr = {x.get('Name'): x.get('Value') for x in instance_group.resource_attr}
    charge_type = cluster.charge_type
    # @JINWENPENG 试用EPC，订单侧传试用，EPC侧传按日月结
    if charge_type == 'FreeTrial':
        charge_type = 'Daily'

    _param_instance = {
        'AvailabilityZone': cluster.availability_zone,
        'HostName': generate_instance_name(cluster.id, instance_group.name, idx, host_suffix),
        'HostType': host_type,
        'Raid': resource_attr.get('raid_type'),
        'SubnetId': instance_group.vpc_subnet_id,
        'ChargeType': charge_type,
        'SecurityGroupId.1': sec_group_id,
        'KeyId': cluster.management_keypair_id,
        'Password': 'Xiaoying33',
        'SubOrderId': sub_order_id,
        'ImageId': config.get('image_id', epc_image_id),
        # bond4：双网卡做BOND，mode4模式
        # single：单网卡
        # dual：两个网卡分别加入不同的VPC
        'NetworkInterfaceMode': resource_attr.get('bond_type', 'bond4'),
    }
    if cluster.charge_type == 'Monthly':
        _param_instance['PurchaseTime'] = cluster.purchase_time

    _param_instance['CloudMonitorAgent'] = 'classic'

    # KES EPC预装列表: startinstances的时候，传 UserData 脚本与KMR5一致
    replace_key_dict = {'public_key': cluster.management_public_key}
    b64_userdata_str = base_kec_userdata_conf % replace_key_dict
    _param_instance["UserData"] = base64.b64encode(b64_userdata_str.encode("utf-8")).decode("utf-8")

    if kes_agent:
        _param_instance['KesAgent'] = 'support'

    return _param_instance


async def form_kec_param(count, cluster, instance_group,
                         sub_order_id=None, sec_group_id=None,
                         data_guard_id=None, idx="1", product_details=None):
    cluster_type = cluster.cluster_type

    # 保留批次号，为未知需求准备
    # 目前KEC开机，参数做排序，不传ProductBatchNo
    _productBatch = await get_item_value(sub_order_id, 'productBatch')
    service_type = replace_service_type(
        cluster_type, instance_group.resource_type, instance_group.volume_type)
    instance_type = get_resource_instance_type(
        instance_group.resource_type, instance_group.instance_type_code,
        service_type, product_details)

    _param_instance = {
        'InstanceType': instance_type,
        'ChargeType': cluster.charge_type,
        'MaxCount': count,
        'SecurityGroupId': sec_group_id,
        'ImageId': instance_group.image_id,
        'SubnetId': instance_group.vpc_subnet_id,
        'InstanceName': generate_instance_name(cluster.id, instance_group.name, None,
                                               f'{cluster.cluster_type.lower()}-'),
        'InstancePassword': 'Xiaoying33',
        'PurchaseTime': cluster.purchase_time,
        'InstanceNameSuffix': str(idx),
        'KeyIds': [cluster.management_keypair_id],
        'DataGuardId': data_guard_id,
        'ProductBatchNo': str(_productBatch),
    }

    # KES KEC预装列表: startinstances的时候，传 UserData 脚本与KMR5一致
    replace_key_dict = {'public_key': cluster.management_public_key}
    b64_userdata_str = base_kec_userdata_conf % replace_key_dict
    _param_instance["UserData"] = base64.b64encode(b64_userdata_str.encode("utf-8")).decode("utf-8")

    # region kec-ssd
    if instance_group.volume_type == "LOCAL_SSD":
        _ssdv = await get_item_value(sub_order_id, 'ssd')
        _param_instance['DataDiskGb'] = int(_ssdv)

    # _param_instance["SystemDisk"] = dict({
    #     "DiskSize": instance_group.system_volume_size
    #     })
    # # 这里不传，KEC 按商品类型自己取
    # if instance_group.system_volume_type:
    #     _param_instance["SystemDisk"]["DiskType"] = instance_group.system_volume_type
    # region DataGuard
    # TODO instance > 10 case

    logger.debug(f"debug ProductBatchNo : {_param_instance['ProductBatchNo']}, "
                 f"name: {_param_instance['InstanceName']}")

    # endregion
    return _param_instance


async def form_node_group_items(product_details, instance_group, basic_items, user_id, region,
                                availability_zone, charge_type, cluster_type, cluster_id,
                                purchase_time, duration_unit=2, project_id=0):
    '''
    !!! 有变化记得修改 form_node_group_items_for_upgrade !!!
    '''
    product_items = {}
    group_price = 0.0

    instance_group_type = instance_group['instance_group_type']
    resource_type = instance_group['resource_type']
    instance_type = instance_group['instance_type_code']
    instance_count = int(instance_group['instance_count'])

    volume_type = instance_group.get('volume_type', 'LOCAL_SSD')
    volume_size = instance_group.get('volume_size', '20')
    volume_count = int(instance_group.get('volume_count', None))
    system_disk_type = instance_group.get('system_disk_type', 'SSD3.0')
    system_disk_size = instance_group.get('system_disk_size', '20')

    if volume_type.startswith('CLOUD_'):
        is_cloud = True
        ebs_type = VOLUME_TYPE_MAP.get(volume_type)
    else:
        is_cloud = False
        ebs_type = None

    # 适配价格体系的配置问题
    if cluster_type == 'KHBASE':
        cluster_type = 'KHBase'

    service_type = replace_service_type(cluster_type, resource_type, volume_type)

    # region cloud ssd
    if is_cloud:
        volume_total_number = volume_count * instance_count
        ebs_items = product_platform.form_ebs_items(
            basic_items, instance_group_type, volume_total_number, availability_zone,
            ebs_type, volume_size, charge_type, cluster_type, cluster_id, purchase_time,
            duration_unit=duration_unit, project_id=project_id
        )
        # logger.info('========================ebs_items: %s', ebs_items)
        res = await product_client.create_ebs_product(ebs_items, user_id, region, cluster_type)
        res['num'] = volume_total_number
        product_items['EBS'] = res
        group_price += res['num'] * res['price']
    # end cloud ssd

    # region kec or epc
    machine_type = None
    if resource_type == 'KEC':
        ssd_size = 0
        local_disk = None
        '''
        {
            'KES_EBS': {'ES.basic.2C4G': {'kec_type': 'I1', 'kec_ebs': '2B', 'product_type': '41'},
                        'ES.basic.3C3G': {'product_type': '303', 'kec_type': 'E1', 'kec_ebs': '3A'},
                        'ES.basic.4C4G': {'kec_type': 'S3', 'kec_ebs': '4A', 'product_type': '152'},
                        'ES.test.4C8G': {'product_type': '151', 'kec_type': 'N3', 'kec_ebs': '4B'},
                        },
            'KES_SSD': {'ES.ssd.2C4G': {'kec_type': 'I1', 'kec_ebs': '2B', 'kec_ssd': 50.0, 'product_type': '41'},
                        'ES.ssd.3C3G': {'kec_ssd': 50.0, 'product_type': '303', 'kec_type': 'E1', 'kec_ebs': '3A'},
                        'ES.ssd.4C4G': {'kec_ssd': 50.0, 'product_type': '152', 'kec_type': 'S3', 'kec_ebs': '4A'},
                        'ES.D4.4C8G': {'product_type': '151', 'kec_type': 'N3', 'kec_ebs': '4B', 'local_disk': '4*7400-SATA HDD'},
                        'ES.ssd.8C16G': {'kec_ssd': 800.0, 'kec_type': 'S3', 'kec_ebs': '4B', 'product_type': '152'}},
            'KES_EPC': {'CAL': {}, 'SSD': {}}
        }
        '''
        kec_service_types = product_details.get(service_type, {})
        kec_instance_types = kec_service_types.get(instance_type, {})
        if not kec_instance_types:
            raise Exception(f'Can not find kec instance type, service {service_type}, instance {instance_type}')
        if not is_cloud:
            ssd_size = int(kec_instance_types.get('kec_ssd', 0))
            local_disk = kec_instance_types.get('local_disk', None)

        flavor_code = f'{kec_instance_types["kec_type"]}.{kec_instance_types["kec_ebs"]}'
        machine_type = flavor_code
        product_type = kec_instance_types['product_type']
        kec_items = product_platform.form_kec_items(
            basic_items, instance_group_type, instance_count, availability_zone,
            flavor_code, ssd_size, product_type, charge_type, cluster_type,
            cluster_id, purchase_time,
            duration_unit=duration_unit, project_id=project_id,
            system_disk_type=system_disk_type, system_disk_size=system_disk_size,
            local_disk=local_disk,
        )
        # logger.info(f'========================kec_items: {kec_items}')
        res = await product_client.create_kec_product(kec_items, user_id, region)
        res['num'] = instance_count
        # logger.info(f'========================kec_res: {res}')
        product_items['KEC'] = res
        group_price += res['num'] * res['price']
    elif resource_type == 'EPC':
        machine_type = instance_type
        epc_items = product_platform.form_epc_items(
            basic_items, instance_group_type, instance_count, availability_zone,
            instance_type, charge_type, cluster_type, cluster_id, purchase_time,
            duration_unit=duration_unit, project_id=project_id
        )
        # logger.info('========================epc_items: %s', epc_items)
        res = await product_client.create_epc_product(epc_items, user_id, region)
        res['num'] = instance_count
        product_items['EPC'] = res
        group_price += res['num'] * res['price']
    # end kec or epc

    # region kes or hbase
    service_items = product_platform.form_service_items(
        basic_items, instance_group_type, instance_count, availability_zone,
        instance_type, machine_type, charge_type, cluster_type, service_type, user_id,
        cluster_id,
        purchase_time,
        duration_unit=duration_unit,
        project_id=project_id,
    )
    # logger.info(f'===form_node_group_items-service_items: {service_items}')
    res = await product_client.create_product(service_items)
    res['num'] = instance_count
    product_items[service_type] = res
    group_price += res['num'] * res['price']
    # end kes or hbase
    # logger.info(f'===form_node_group_items-product_items : {product_items}')

    return product_items, group_price


async def form_node_group_items_for_upgrade(product_details, instance_group, basic_items, user_id, region,
                                            availability_zone, charge_type, cluster_type, cluster_id,
                                            purchase_time, duration_unit=2, project_id=0,
                                            origin_instance_group=None, origin_instance=None):
    '''
    与form_node_group_items几乎一致。
    重写一份的原因，返回结构中，业务类型的下一级不在是单独的商品ID。而是商品ID的数组。
    升配的商品需要单独创建
    if service_type in product_items:
        product_items[service_type].append(res)
    else:
        product_items[service_type] = [res]
    '''
    product_items = {}
    group_price = 0.0
    is_upgrade_kec = False
    is_upgrade_ebs = False
    is_upgrade_service = False

    instance_group_type = instance_group['instance_group_type']
    resource_type = instance_group['resource_type']
    instance_type = instance_group['instance_type_code']
    # 升配需要为每个实例，单独创建商品，所以强制为1
    instance_count = 1

    volume_type = instance_group.get('volume_type', 'LOCAL_SSD')
    volume_size = instance_group.get('volume_size', '20')
    # 升配需要为每个实例，单独创建商品，所以强制为1
    volume_count = 1
    system_disk_type = instance_group.get('system_disk_type', 'SSD3.0')
    system_disk_size = instance_group.get('system_disk_size', '20')

    # 本地SSD型：若CPU、内存变更，需要处理KES和KEC订单。
    #           若CPU、内存保存不变，本地盘变更，只需要处理 KEC订单
    # 本地HDD型：只要有变更，就需要处理KES和KEC订单
    if volume_type == origin_instance_group.volume_type and int(
            volume_size) > int(origin_instance_group.volume_size):
        if volume_type.startswith('CLOUD_'):
            is_upgrade_ebs = True
        elif volume_type.startswith('LOCAL_'):
            is_upgrade_kec = True
            if volume_type == 'LOCAL_HDD':
                is_upgrade_service = True

    flag_instance_type_code = validate_instance_type_code(instance_type, origin_instance_group.instance_type_code)
    if resource_type == 'KEC' and flag_instance_type_code == 1:
        is_upgrade_kec = True
        is_upgrade_service = True

    logger.info(f'Upgrade instance groups >>> is_upgrade_kec:{is_upgrade_kec},is_upgrade_ebs:{is_upgrade_ebs},is_upgrade_service:{is_upgrade_service}')

    if volume_type.startswith('CLOUD_'):
        is_cloud = True
        ebs_type = VOLUME_TYPE_MAP.get(volume_type)
    else:
        is_cloud = False
        ebs_type = None

    # 适配价格体系的配置问题
    if cluster_type == 'KHBASE':
        cluster_type = 'KHBase'

    service_type = replace_service_type(cluster_type, resource_type, volume_type)

    # region cloud ssd
    if is_cloud and is_upgrade_ebs:
        volume_total_number = volume_count * instance_count

        for ebs_instance_id in origin_instance.volumes:

            _info = await charge_client.get_instance_info(ebs_instance_id)
            ebs_origin_product_id = _info.get('productId', '')
            if not ebs_origin_product_id or int(_info.get('status', -1)) != 2:
                raise Exception(f'EBS Instance status is not active. ebs_instance_id:{ebs_instance_id}')

            ebs_items = product_platform.form_ebs_items(
                basic_items, instance_group_type, volume_total_number, availability_zone,
                ebs_type, volume_size, charge_type, cluster_type, cluster_id, purchase_time,
                duration_unit=duration_unit, project_id=project_id,
                origin_instance_id=ebs_instance_id, origin_product_id=ebs_origin_product_id
            )
            # logger.info('========================ebs_items: %s', ebs_items)
            res = await product_client.create_ebs_product(ebs_items, user_id, region, cluster_type)
            res['num'] = volume_total_number
            if 'EBS' in product_items:
                product_items['EBS'].append(res)
            else:
                product_items['EBS'] = [res]
            group_price += res['num'] * res['price']
    # end cloud ssd

    # region kec or epc
    machine_type = None
    if resource_type == 'KEC' and is_upgrade_kec:
        ssd_size = 0
        local_disk = None
        '''
        {
            'KES_EBS': {'ES.basic.2C4G': {'kec_type': 'I1', 'kec_ebs': '2B', 'product_type': '41'},
                        'ES.basic.3C3G': {'product_type': '303', 'kec_type': 'E1', 'kec_ebs': '3A'},
                        'ES.basic.4C4G': {'kec_type': 'S3', 'kec_ebs': '4A', 'product_type': '152'},
                        'ES.test.4C8G': {'product_type': '151', 'kec_type': 'N3', 'kec_ebs': '4B'},
                        },
            'KES_SSD': {'ES.ssd.2C4G': {'kec_type': 'I1', 'kec_ebs': '2B', 'kec_ssd': 50.0, 'product_type': '41'},
                        'ES.ssd.3C3G': {'kec_ssd': 50.0, 'product_type': '303', 'kec_type': 'E1', 'kec_ebs': '3A'},
                        'ES.ssd.4C4G': {'kec_ssd': 50.0, 'product_type': '152', 'kec_type': 'S3', 'kec_ebs': '4A'},
                        'ES.D4.4C8G': {'product_type': '151', 'kec_type': 'N3', 'kec_ebs': '4B', 'local_disk': '4*7400-SATA HDD'},
                        'ES.ssd.8C16G': {'kec_ssd': 800.0, 'kec_type': 'S3', 'kec_ebs': '4B', 'product_type': '152'}},
            'KES_EPC': {'CAL': {}, 'SSD': {}}
        }
        '''
        kec_service_types = product_details.get(service_type, {})
        kec_instance_types = kec_service_types.get(instance_type, {})
        if not kec_instance_types:
            raise Exception(f'Can not find kec instance type, service {service_type}, instance {instance_type}')
        if not is_cloud:
            ssd_size = int(kec_instance_types.get('kec_ssd', 0))
            local_disk = kec_instance_types.get('local_disk', None)

        flavor_code = f'{kec_instance_types["kec_type"]}.{kec_instance_types["kec_ebs"]}'
        machine_type = flavor_code
        product_type = kec_instance_types['product_type']

        _info = await charge_client.get_instance_info(origin_instance.instance_id)
        kec_origin_product_id = _info.get('productId', '')
        if not kec_origin_product_id or int(_info.get('status', -1)) != 2:
            raise Exception(f'KEC Instance status is not active. kec_instance_id:{origin_instance.instance_id}')

        kec_items = product_platform.form_kec_items(
            basic_items, instance_group_type, instance_count, availability_zone,
            flavor_code, ssd_size, product_type, charge_type, cluster_type,
            cluster_id, purchase_time,
            duration_unit=duration_unit, project_id=project_id,
            system_disk_type=system_disk_type, system_disk_size=system_disk_size,
            local_disk=local_disk, origin_instance_id=origin_instance.instance_id, origin_product_id=kec_origin_product_id
        )
        # logger.info('========================kec_items: %s', kec_items)
        res = await product_client.create_kec_product(kec_items, user_id, region)
        res['num'] = instance_count
        # logger.info('========================kec_res: %s', res)
        if 'KEC' in product_items:
            product_items['KEC'].append(res)
        else:
            product_items['KEC'] = [res]
        group_price += res['num'] * res['price']

    # end kec or epc

    # region kes or hbase
    if resource_type == 'KEC' and is_upgrade_service:
        _info = await charge_client.get_instance_info(origin_instance.service_instance_id)
        service_origin_product_id = _info.get('productId', '')
        if not service_origin_product_id or int(_info.get('status', -1)) != 2:
            raise Exception(f'SERVICE Instance status is not active. service_instance_id:{origin_instance.service_instance_id}')

        service_items = product_platform.form_service_items(
            basic_items, instance_group_type, instance_count, availability_zone,
            instance_type, machine_type, charge_type, cluster_type, service_type, user_id,
            cluster_id,
            purchase_time,
            duration_unit=duration_unit,
            project_id=project_id,
            origin_instance_id=origin_instance.service_instance_id, origin_product_id=service_origin_product_id
        )
        # logger.info(f'===form_node_group_items-service_items: {service_items}')
        res = await product_client.create_product(service_items)
        res['num'] = instance_count
        if service_type in product_items:
            product_items[service_type].append(res)
        else:
            product_items[service_type] = [res]
        group_price += res['num'] * res['price']
        # end kes or hbase
        # logger.info(f'===form_node_group_items-product_items : {product_items}')

    return product_items, group_price


async def create_slb_product(user_id, charge_type, availability_zone, region, cluster_id=None, request_id=None,
                             source=1,
                             project_id=0):
    order_product_items = {}
    total_price = 0.0

    basic_items = product_platform.form_basic_items('BUY', charge_type, source)
    slb_bill_type = await price_client.get_slb_bill_type(user_id)
    slb_items = product_platform.form_slb_items(basic_items, availability_zone, slb_bill_type, cluster_id,
                                                project_id)
    # logger.info('=========================slb_items: %s', slb_items)
    if not request_id:
        request_id = gen_uuid4()
    res = await product_client.create_eip_product(slb_items, user_id, region, product_type='SLB')
    res['num'] = 1
    # logger.info('=======================slb res: %s', res)
    order_product_items['SLB'] = res
    total_price += res['num'] * res['price']
    return order_product_items, total_price


async def create_products(cluster, product_details, account_id, source=1):
    """
    :param product_details:
    :param account_id:
    :param cluster:
    :param source: 1-console, 2-operating platform, 3-openapi
    :return: {"EIP": {""}, "DATA": {"KEC":{}, "EPC":{}, "EBS":{}, "KES_EBS":{}}}

    !!! 这里如果有变化记得修改 upgrade_products !!!
    """
    order_product_items = {}
    total_price = 0.0

    cluster_type = cluster['cluster_type']
    availability_zone = cluster['availability_zone']
    project_id = cluster.get('project_id', 0)
    charge_type = cluster['charge_type']
    cluster_id = cluster['cluster_id']
    region = cluster['region']
    purchase_time = cluster.get('purchase_time', 0)
    duration_unit = cluster.get('purchase_time_unit', 2)
    basic_items = product_platform.form_basic_items('BUY', charge_type, source)

    # logger.info('==================basic_items: %s', basic_items)

    # region eip
    if cluster.get('enable_eip', False):
        eip_line_id = cluster.get('eip_line_id', None)
        eip_charge_type = cluster.get('eip_charge_type', None)
        eip_band_width = cluster.get('eip_band_width', 1)
        eip_purchase_time = cluster.get('eip_purchase_time', 0)
        eip_duration_time_unit = cluster.get('eip_purchase_time_unit', 2)
        eip_type = cluster.get('eip_type', 'EIP')
        num = cluster.get('eip_num', 1)
        enable_slb = cluster.get('enable_slb', False)

        if eip_line_id and eip_charge_type:
            # 公网EIP
            if eip_type == 'EIP':
                eip_instance_type = await price_client.get_eip_instance_type(account_id, region)
                eip_items = product_platform.form_eip_items(
                    basic_items, availability_zone, eip_line_id, eip_instance_type, eip_band_width,
                    eip_charge_type, eip_purchase_time, cluster_id,
                    duration_unit=eip_duration_time_unit,
                    project_id=project_id
                )
            # 内网EIP
            else:
                eip_instance_type = await price_client.get_eip_instance_type(account_id, region, value=10)
                eip_items = product_platform.form_eip_items(
                    basic_items, availability_zone, eip_line_id, eip_instance_type, eip_band_width,
                    eip_charge_type, eip_purchase_time, cluster_id,
                    duration_unit=eip_duration_time_unit, num=num, enable_ieip=True,
                    project_id=project_id
                )

            # logger.info('=========================eip_items: %s', eip_items)
            res = await product_client.create_eip_product(eip_items, account_id, region)
            res['num'] = num
            # logger.debug('=======================eip res: %s', res)
            order_product_items['EIP'] = res
            total_price += res['num'] * res['price']

        # slb_items
        if enable_slb:
            slb_bill_type = await price_client.get_slb_bill_type(account_id)
            slb_items = product_platform.form_slb_items(basic_items, availability_zone, slb_bill_type, cluster_id,
                                                        project_id)
            logger.info(f'==slb_items: {slb_items}')
            res = await product_client.create_eip_product(slb_items, account_id, region, product_type='SLB')
            res['num'] = 1
            logger.info(f'==slb res: {res}')
            order_product_items['SLB'] = res
            total_price += res['num'] * res['price']
    # end eip

    for instance_group in cluster.get('instance_groups', []):
        instance_group_type = instance_group['instance_group_type']
        # subnet_id = instance_group['VpcSubnetId']
        products, g_price = await form_node_group_items(
            product_details, instance_group, basic_items, account_id, region,
            availability_zone, charge_type, cluster_type, cluster_id,
            purchase_time, duration_unit=duration_unit, project_id=project_id)
        order_product_items[instance_group_type] = products
        total_price += g_price

    return order_product_items, total_price


async def upgrade_products(cluster, product_details, account_id, source=1):
    """
    :param product_details:
    :param account_id:
    :param cluster:
    :param source: 1-console, 2-operating platform, 3-openapi
    :return: {"EIP": {""}, "DATA": {"KEC":{}, "EPC":{}, "EBS":{}, "KES_EBS":{}}}
    """
    order_product_items = {}
    total_price = 0.0

    cluster_type = cluster['cluster_type']
    availability_zone = cluster['availability_zone']
    project_id = cluster.get('project_id', 0)
    charge_type = cluster['charge_type']
    cluster_id = cluster['cluster_id']
    region = cluster['region']
    purchase_time = cluster.get('purchase_time', 0)
    duration_unit = cluster.get('purchase_time_unit', 2)
    basic_items = product_platform.form_basic_items('SCALE', charge_type, source)

    # logger.info('==================basic_items: %s', basic_items)

    upgrade_instance_group = cluster.get('instance_groups', [])[0]
    upgrade_instance_group_type = upgrade_instance_group['instance_group_type']

    origin_cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)

    for ig in origin_cluster.instance_groups:
        if ig.id != upgrade_instance_group.get('id', ''):
            continue
        for instance in ig.instances:
            products, g_price = await form_node_group_items_for_upgrade(
                product_details, upgrade_instance_group, basic_items, account_id, region,
                availability_zone, charge_type, cluster_type, cluster_id,
                purchase_time, duration_unit=duration_unit, project_id=project_id,
                origin_instance_group=ig, origin_instance=instance)
            if upgrade_instance_group_type in order_product_items:
                order_product_items[upgrade_instance_group_type].append(products)
            else:
                order_product_items[upgrade_instance_group_type] = [products]
            total_price += g_price

    return order_product_items, total_price


async def get_item_value(suborder_id, item_no):
    product_id = await charge_client.find_product_id_by_sub_order(suborder_id)
    if product_id:
        return await product_client.find_item_by_product(product_id, item_no)
    return None


async def get_all_suborders_format(order_id):
    suborder_list = await charge_client.query_sub_orders_by_order_id(order_id)

    result = dict()
    for _suborder in suborder_list:
        _sub_order_id = _suborder.get('subOrderId', '')
        _product_group = int(_suborder.get('productGroup', '0'))
        _product_id = _suborder.get('productId', '')
        _instance_id = _suborder.get('instanceId', None)

        # eip and slb has no instanceGroupType
        _instance_group_type = None
        if _product_id:
            _instance_group_type = await product_client.find_item_by_product(_product_id, 'instanceGroupType')
            if not _instance_group_type:
                _instance_group_type = 'cluster'

        _instance_group_type_dict = result.get(_instance_group_type, dict())

        if product_platform.PRODUCT_GROUP_MAP['KES'] == int(_product_group):
            _kes_pg = _instance_group_type_dict.get('kes', [])
            _kes_pg.append(_sub_order_id)
            _instance_group_type_dict['kes'] = _kes_pg

        elif product_platform.PRODUCT_GROUP_MAP['KHBASE'] == int(_product_group):
            _khbase_pg = _instance_group_type_dict.get('khbase', [])
            _khbase_pg.append(_sub_order_id)
            _instance_group_type_dict['khbase'] = _khbase_pg

        elif product_platform.PRODUCT_GROUP_MAP['EIP'] == int(_product_group):
            _eip_pg = _instance_group_type_dict.get('eip', [])
            _eip_pg.append(_sub_order_id)
            _instance_group_type_dict['eip'] = _eip_pg

        elif product_platform.PRODUCT_GROUP_MAP['SLB'] == int(_product_group):
            _slb_pg = _instance_group_type_dict.get('slb', [])
            _slb_pg.append(_sub_order_id)
            _instance_group_type_dict['slb'] = _slb_pg

        elif product_platform.PRODUCT_GROUP_MAP['EBS'] == int(_product_group):
            _ebs_pg = _instance_group_type_dict.get('ebs', [])
            _ebs_pg.append(_sub_order_id)
            _instance_group_type_dict['ebs'] = _ebs_pg

        elif product_platform.PRODUCT_GROUP_MAP['KEC'] == int(_product_group):
            _kec_pg = _instance_group_type_dict.get('kec', [])
            _kec_pg.append(_sub_order_id)
            _instance_group_type_dict['kec'] = _kec_pg

        elif product_platform.PRODUCT_GROUP_MAP['EPC'] == int(_product_group) or \
                product_platform.PRODUCT_GROUP_MAP['GEPC'] == int(_product_group):
            _epc_pg = _instance_group_type_dict.get('epc', [])
            _epc_pg.append(_sub_order_id)
            _instance_group_type_dict['epc'] = _epc_pg

        else:
            _o_pg = _instance_group_type_dict.get('other', [])
            _o_pg.append(_sub_order_id)
            _instance_group_type_dict['other'] = _o_pg

        result[_instance_group_type] = _instance_group_type_dict

    logger.info(f'==_find_all_sub_order_ids, main order: {order_id}, all sub orders: {result}')

    return result
