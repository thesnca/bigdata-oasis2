import asyncio
import json

from prettytable import PrettyTable
from termcolor import cprint

from conf.charge_conf import CHARGE_BILL_MAP
from conf.charge_conf import MACHINE_ROOM_MAP
from conf.charge_conf import PRODUCT_GROUP_ID_MAP
from conf.charge_conf import PRODUCT_TYPE_MAP
from conf.charge_conf import product_code_map
from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.instance_group import InstanceGroupModel
from oasis.utils import sdk
from oasis.utils.sdk import charge_client
from oasis.utils.sdk import product_client
from oasis.utils.sdk.charging.base import replace_service_type


async def check_order_status(order_id):
    base_info = {}

    table = PrettyTable(field_names=['sub_order_id', 'instance_id', 'product_group',
                                     'order_status', 'instance_status',
                                     'project_id',
                                     'start_time', 'end_time'])
    sub_orders = await charge_client.query_sub_orders_by_order_id(order_id)
    tasks = [charge_client.get_instance_info(so.get('instanceId')) for so in sub_orders]
    infos = await asyncio.gather(*tasks)
    for idx, so in enumerate(sub_orders):
        info = infos[idx] or {}
        sub_order_id = so.get('subOrderId')
        instance_id = so.get('instanceId')
        order_status = so.get('status')
        product_group = int(so.get('productGroup', 0))
        product_name = product_code_map.get(product_group, product_group)
        instance_status = info.get('status')
        start_time = info.get('billingBeginTime', None)
        end_time = info.get('billingEndTime', None)
        project_id = info.get('iamProjectId', None)

        base_info.setdefault('app_id', so.get('appId', None))
        base_info.setdefault('region', so.get('region', None))
        base_info.setdefault('user_id', info.get('userId', None))

        table.add_row([sub_order_id, instance_id, product_name,
                       order_status, instance_status,
                       project_id,
                       start_time, end_time])
    return base_info, table


async def upgrade_instance_group(cluster_id, instance_group_id, display_region, display_az, kec_order_type,
                                 kes_instance_type, product_batch):
    if not cluster_id:
        cprint('Please specify cluster id', 'red')
        return

    if not instance_group_id:
        cprint('Please specify instance group id', 'red')
        return

    cluster = await get_model_by_id(ClusterModel, cluster_id)
    if not cluster:
        cprint(f'Cluster {cluster_id} not found', 'red')
        return

    ig = await get_model_by_id(InstanceGroupModel, instance_group_id)
    if not ig:
        cprint(f'Instance Group {instance_group_id} not found', 'red')
        return

    cluster_type = cluster.cluster_type.upper()
    product_group = PRODUCT_GROUP_ID_MAP.get(cluster_type)
    service_type = replace_service_type(cluster_type, ig.resource_type, ig.volume_type)
    product_type = PRODUCT_TYPE_MAP.get(service_type)
    region = MACHINE_ROOM_MAP.get(cluster.availability_zone)
    bill_type = CHARGE_BILL_MAP.get(cluster.charge_type)
    account_id = cluster.ksc_user_id

    # == Modify KEC ==
    kec_client = getattr(sdk, f'kec_client_{cluster_type.lower()}')
    for instance in ig.instances:
        res = await kec_client.modify_instance_type(instance_id=instance.instance_id, instance_type=kec_order_type,
                                                    account_id=account_id)
        if not res:
            cprint(f'Modify kec instance failed, instance id: {instance.instance_id}', 'red')
            return

    # == Create Product ==
    basic_items = {
        'productUse': 3,  # upgrade
        'productWhat': 1,
        'source': 1,
        'isActivity': 0,
        'promotionList': None,
        'promotion': None,
        'sourceExtend': None,
        'customPrice': False,
        'productName': cluster_type,
        'productGroup': product_group,
        'productType': product_type,
        'userId': account_id,
        'region': region,
        'availabilityZone': cluster.availability_zone,
        'billType': bill_type,
    }

    display = json.dumps([
        {'key': '数据中心', 'value': display_region},
        {'key': '可用区', 'value': display_az},
        {'key': '类型', 'value': kec_order_type}
    ])

    new_product_ids = []

    for instance in ig.instances:
        order_info = await charge_client.get_instance_info(instance.service_instance_id)
        add_items = [
            {
                'itemNo': 'clusterUid',
                'itemName': '集群Uid',
                'unit': None,
                'unitCount': None,
                'value': cluster_id
            },
            {
                'itemNo': 'display',
                'itemName': 'display',
                'unit': None,
                'unitCount': None,
                'value': display
            },
            {
                'itemNo': 'esProductType',
                'itemName': 'es产品类型',
                'unit': None,
                'unitCount': None,
                'value': str(product_group)
            },
            {
                'itemNo': 'iamProjectId',
                'itemName': 'iamProjectId',
                'unit': None,
                'unitCount': None,
                'value': order_info.get('iamProjectId', '0')
            },
            {
                'itemNo': 'instanceGroupType',
                'itemName': '实例组类型',
                'unit': None,
                'unitCount': None,
                'value': ig.instance_group_type
            },
            {
                'itemNo': 'instanceType',
                'itemName': 'KES套餐类型',
                'unit': None,
                'unitCount': None,
                'value': kes_instance_type
            },
            {
                'itemNo': 'productBatch',
                'itemName': '商品批次号',
                'unit': None,
                'unitCount': None,
                'value': product_batch
            },
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
                'value': order_info.get('productId')
            }
        ]

        new_product_param = {
            'instanceId': instance.service_instance_id,
            'items': add_items
        }

        new_product_param.update(basic_items)

        cprint(f'instance {instance.instance_id}, params: {json.dumps(new_product_param, indent=2)}', 'green')

        res = await product_client.create_product(new_product_param)

        cprint(res, 'red')

        if not res:
            cprint(f'Create product failed, instance {instance.instance_id}', 'red')
            return

        new_product_id = res.get('product_id', None)
        if not new_product_id:
            cprint(f'Create product failed, instance {instance.instance_id}', 'red')
            return

        new_product_ids.append(res.get('product_id'))

    # == Create Order ==
    order_product_items = [
        {
            'productId': product_id,
            'num': 1,
            'productGroup': product_group,
        }
        for product_id in new_product_ids
    ]

    new_order_id = await charge_client.create_order(account_id, 'SCALE', order_product_items, cluster_type)

    if not new_order_id:
        cprint('Create order failed!', 'red')
        return

    cprint(f'Create Order succeed, new order id {new_order_id}', 'green')

    # == Update DB==
    ig.instance_type_code = kes_instance_type
    await ig.save()

    # == Notify Order ==
    sub_orders = await charge_client.query_sub_orders_by_order_id(new_order_id)
    for so in sub_orders:
        sub_order_id = so.get('subOrderId')

        await charge_client.notify_suborder_status(sub_order_id, 1)
