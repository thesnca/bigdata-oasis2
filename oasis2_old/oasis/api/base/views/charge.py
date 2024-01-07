from conf.charge_conf import PRODUCT_GROUP_ID_MAP
from oasis.api import BaseView
from oasis.utils import sdk
from oasis.utils.chaos import DISTRIBUTION_SCHEMAS
from oasis.utils.config import config
from oasis.utils.generator import gen_name
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger
from oasis.utils.sdk import price_client
from oasis.worker.tasks.order import TaskCreateProduct
from oasis.utils.sdk.charging.base import upgrade_products


class ChargeView(BaseView):
    """
        routes.append(('/CreateProducts', ChargeView))
    """

    async def create_products(self, *args, **kwargs):
        region = config.get('infra', 'region')
        '''
        kwargs = {
            'name': 'test-kes',
            'cluster_type': 'KES',
            'distribution': 'kes-1.0.0',
            'main_version': '1.4.12',
            'termination_protected': True,
            'instance_groups':
                [
                    {'instance_group_type': 'MASTER',
                     'resource_type': 'KEC',
                     'instance_type_code': 'ES.basic.2C4G',
                     'instance_count': 3,
                     'resouce_attributes': [
                         {'name': 'bandwidth', 'value': '2000'},
                         {'name': 'pps', 'value': '500000'},
                         {'name': 'bond_type', 'value': '0'},
                         {'name': 'raid_type', 'value': 'raid50'}
                     ],
                     'system_volume_type': 'LOCAL',
                     'system_volume_size': 20,
                     'volume_type': 'CLOUD_SSD',
                     'volume_size': 2000,
                     'volume_count': 3,
                     'vpc_id': 'vvvv-pppp-ccccc-1111',
                     'vpc_subnet_id': 'vvvv-pppp-ccccc-ssss',
                     'avalability_zone': 'cn-beijing-6a',
                     'charge_type': 'Monthly',
                     'purchase_time': '',
                     'expire_time': '',
                     'order_id': 'oooo-rrrr-dddd-eeee'
                     },
                    {'instance_group_type': 'CORE',
                     'resource_type': 'KEC',
                     'instance_type_code': 'ES.basic.2C4G',
                     'instance_count': 3,
                     'resouce_attributes':
                         [
                             {'name': 'bandwidth', 'value': '2000'},
                             {'name': 'pps', 'value': '500000'},
                             {'name': 'bond_type', 'value': '0'},
                             {'name': 'raid_type', 'value': 'raid50'}
                         ],
                     'system_volume_type': 'LOCAL',
                     'system_volume_size': 20,
                     'volume_type': 'CLOUD_SSD',
                     'volume_size': 2000,
                     'volume_count': 3,
                     'vpc_id': 'vvvv-pppp-ccccc-1111',
                     'vpc_subnet_id': 'vvvv-pppp-ccccc-ssss',
                     'avalability_zone': 'cn-beijing-6a',
                     'charge_type': 'Monthly',
                     'purchase_time': '',
                     'expire_time': '',
                     'order_id': 'oooo-rrrr-dddd-eeee'}
                ],
            'enable_eip': True,
            'eip_line_id': 'bgp',
            'eip_charge_type': 'Monthly',
            'eip_band_width': 1,
            'eip_purchase_time': 10,
            'eip_purchase_time_unit': 2,
            'ip_addr': '172.0.0.1',
            'allocation_id': 'aaaa-llll-llll-oooo',
            'availability_zone': 'cn-beijing-6a',
            'vpc_domain_id': 'vvvv-pppp-cccc-dddd',
            'vpc_subnet_id': 'vvvv-ssss-iiii-dddd',
            'charge_type': 'Monthly',
            'purchase_time': 2,
            'purchase_time_unit': 2,
            'expire_time': '2020-03-05 12:00:12',
            'order_id': 'oooo-rrrr-dddd-eeee',
            'project_id': 'pppp-rrrr-oooo-jjjj',
            'launch_resume_type': 'all',
            'request_id': 'rrrr-eeee-qqqq-uuuu',
            'dry_run': False
        }
        '''
        cluster_id = kwargs.get('cluster_id', None)
        cluster_name = kwargs.get('cluster_name', None)
        distribution = kwargs.get('distribution', None)
        cluster_type = self.product.upper()

        if not cluster_id:
            cluster_id = gen_uuid4()
            kwargs.setdefault('cluster_id', cluster_id)

        if not cluster_name:
            cluster_name = gen_name(self.product, 'timestamp')
            kwargs.setdefault('cluster_name', cluster_name)

        if distribution not in DISTRIBUTION_SCHEMAS.get(cluster_type, []):
            raise Exception(f'{cluster_type} did not support distribution {distribution}')

        product_details = await price_client.get_product_details(self.account_id,
                                                                 PRODUCT_GROUP_ID_MAP[cluster_type], '1')
        # logger.info(self, f'==product_details: {product_details}')

        kwargs.setdefault('product', self.product)
        kwargs.setdefault('account_id', self.account_id)
        kwargs.setdefault('region', region)
        kwargs.setdefault('cluster_type', cluster_type)
        kwargs.setdefault('product_details', product_details)

        charge_type = kwargs.get('charge_type', None)
        purchase_time = int(kwargs.get('purchase_time', 0))
        if charge_type == 'FreeTrial' and purchase_time <= 0:
            raise Exception(f'Can not create product with purchase_time {purchase_time}')

        # 创建商品
        # Syncronized Task
        task1 = TaskCreateProduct(args=kwargs)
        ret = await task1.run()

        return {
            'Info': ret.get('order_product_details', {}),
            'RequestId': self.request_id,
        }

    async def upgrade_products(self, *args, **kwargs):
        '''
        从业务角度，升配专用。
        从功能角度，这是另一个create_products。
        功能范围：
        1. KEC 升配商品
        2. EBS 升配商品
        3. KES 升配商品

        {
            "name":"dylan-test-upgrade",
            "cluster_type":"KES",
            "distribution":"kes-1.0.0",
            "main_version":"OpenSource-7.4.2",
            "availability_zone":"cn-guangzhou-1a",
            "project_id":0,
            "vpc_domain_id":"77264fb6-b32a-4a1d-af1d-143c1a5cb825",
            "vpc_subnet_id":"687eed49-0111-4eb9-b7a1-e9236f61b831",
            "security_group_id":"2d7cc33c-9e1e-467c-af14-98d290303f65",
            "charge_type":"Minutely",
            "cluster_id":"2c1c8c4a-9c32-450b-9081-373b57d1a50a",
            "instance_groups":[
                {
                    "instance_group_type":"DATA",
                    "resource_type":"KEC",
                    "instance_type_code":"ES.ssd.4C8G",
                    "instance_type":"S3.4B",
                    "instance_count":1,
                    "vpc_id":"77264fb6-b32a-4a1d-af1d-143c1a5cb825",
                    "vpc_subnet_id":"687eed49-0111-4eb9-b7a1-e9236f61b831",
                    "volume_type":"LOCAL_SSD",
                    "volume_size":500,
                    "volume_count":1,
                    "charge_type":"Minutely",
                    "system_disk_type":"Local_SSD",
                    "id":"361cb6ee-02e9-47b4-8f51-39f60fe5a314"
                }
            ]
        }
        '''
        cluster_id = kwargs.get('cluster_id', None)
        cluster_type = self.product.upper()

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        if not (len(kwargs.get('instance_groups', [])) == 1):
            raise Exception('Please specify a single instance_group')

        if not kwargs.get('instance_groups', [])[0].get('id', ""):
            raise Exception('Please specify instance_group.id')

        # if not kwargs.get('instance_groups', [])[0].get('instance_ids', ""):
        #     raise Exception('Please specify instance_group.instance_ids')

        charge_type = kwargs.get('charge_type', None)
        purchase_time = int(kwargs.get('purchase_time', 0))
        if charge_type == 'FreeTrial' and purchase_time <= 0:
            raise Exception(f'Can not create product with purchase_time {purchase_time}')

        product_details = await price_client.get_product_details(self.account_id,
                                                                 PRODUCT_GROUP_ID_MAP[cluster_type], '1')
        # logger.info(self, f'==product_details: {product_details}')

        order_id = kwargs.get('order_id', None)
        order_product_details = {}
        if not order_id:
            order_product_details, _ = await upgrade_products(kwargs, product_details, self.account_id, source=3)

        return {
            'Info': order_product_details,
            'RequestId': self.request_id,
        }
