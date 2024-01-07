from conf.charge_conf import APP_ID_MAP
from conf.charge_conf import product_code_map
from conf.charge_conf import ORDER_USE_MAP
from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.poll_util import wait_until_complete


def _prepare(func):
    async def __inner(*args, **kwargs):
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

        return await func(headers=headers, *args, **kwargs)

    return __inner


class ChargeClient:
    def __init__(self):
        self.endpoint = config.get('charge', 'charge_uri')
        self.instance_endpoint = config.get('charge', 'instance_uri')

    @_prepare
    async def create_order(self, account_id, order_use, order_products, cluster_type,
                           headers=None):
        order_payload = {
            'userId': account_id,
            'orderUse': ORDER_USE_MAP[order_use],
            'source': 3,
            'orderProductItems': order_products,
            'sourceExtend': {
                'appId': APP_ID_MAP[cluster_type.upper()]
            }
        }

        code, ret = await http.post(f'{self.endpoint}/trade/orders', data=order_payload, headers=headers)
        if 199 < code < 300:
            return ret.get('orderId', None)
        raise Exception(f'Create order failed, return {code}, {ret}')

    @_prepare
    async def query_sub_orders_by_order_id(self, order_id, headers=None):
        params = {
            'orderId': order_id,
        }
        code, ret = await http.get(f'{self.endpoint}/trade/querySubOrdersByOrderId', params=params, headers=headers)

        return ret.get('subOrders', [])

    @_prepare
    async def query_sub_orders_by_instance_ids(self, instance_id_list: list, status='1,2,3', headers=None):
        params = {
            'instanceIds': ','.join(instance_id_list),
            'status': status,
        }
        code, ret = await http.get(f'{self.endpoint}/trade/querySubOrdersByInstanceIds', params=params, headers=headers)
        res = {item.get('instanceId'): item
               for item in ret.get('subOrders', [])
               if item.get('instanceId', None) in instance_id_list}

        return res

    @_prepare
    async def query_sub_orders(self, suborder_id, headers=None):
        params = {
            'subOrderId': suborder_id
        }
        code, ret = await http.get(f'{self.endpoint}/trade/querySubOrders', params=params, headers=headers)
        return ret.get('subOrders', [])

    @_prepare
    async def _update_suborder_status(self, suborder_id, status, *,
                                      instance_id=None, owner_product_group=0,
                                      owner_instance_id='', headers=None):
        payload = {
            'subOrderId': suborder_id,
            'status': status,
        }

        # 告知instance id
        if instance_id:
            payload.setdefault('instanceId', instance_id)

        # 关联主实例
        if owner_instance_id and owner_product_group > 0:
            payload.setdefault('ownerProductGroup', owner_product_group)
            payload.setdefault('ownerInstanceId', owner_instance_id)

        code, ret = await http.post(f'{self.endpoint}/trade/notifySubOrderStatus', data=payload, headers=headers)
        if 199 < code < 300:
            return True
        raise Exception(f'_update_suborder_status failed, suborder: {suborder_id}, target status: {status}')

    async def find_all_kec_suborders(self, order_id, headers=None):
        sub_orders = await self.query_sub_orders_by_order_id(order_id)

        res = {}
        for item in sub_orders:
            if item['productGroup'] == 100:
                res.setdefault(item['subOrderId'], item['instanceId'])
        return res

    async def find_all_eip_suborders(self, order_id, headers=None):
        sub_orders = await self.query_sub_orders_by_order_id(order_id)

        res = []
        for item in sub_orders:
            if int(item['productGroup']) == 102:
                res.append({
                    'subOrderId': item['subOrderId'],
                    'productGroup': item['productGroup'],
                    'productId': item['productId'],
                    'status': item['status'],
                })
        return res

    async def find_all_order_info_for_upgrade(self, order_id, header=None):
        '''
        该函数仅提供给upgrade_instance_group使用。
        当前升配只会处理KEC/EBS/Owner_Service。
        并且由于升配流程控制，导致一个子订单必然会对应一个实例ID。
        后续其他逻辑不建议使用该函数
        '''
        sub_orders = await self.query_sub_orders_by_order_id(order_id)

        res = {}
        for item in sub_orders:
            product_group_name = product_code_map[int(item['productGroup'])]
            if product_group_name in res:
                res[product_group_name][item['instanceId']] = item['subOrderId']
            else:
                res[product_group_name] = {item['instanceId']: item['subOrderId']}

        return res

    @wait_until_complete(timeout=120, interval=5)
    async def wait_suborders_by_instance_id_list(self, instance_id_list: list, status='1,2,3'):
        sub_orders = await self.query_sub_orders_by_instance_ids(instance_id_list, status)

        if len(sub_orders) != len(instance_id_list):
            return {}
        return sub_orders

    async def notify_suborder_status(self, suborder_id, status, *,
                                     instance_id=None, owner_product_group=0,
                                     owner_instance_id=''):
        return await self._update_suborder_status(suborder_id, status, instance_id=instance_id,
                                                  owner_product_group=owner_product_group,
                                                  owner_instance_id=owner_instance_id)

    async def get_suborder_status(self, suborder_id):
        sub_orders = await self.query_sub_orders(suborder_id)

        if not sub_orders:
            return 'Unknown'
        return sub_orders[0].get('status', 'Unknown')

    async def find_product_id_by_sub_order(self, suborder_id):
        sub_orders = await self.query_sub_orders(suborder_id)
        if not sub_orders:
            return None
        return sub_orders[0].get('productId', None)

    @_prepare
    async def batch_delete_instances(self, instance_ids: list, account_id,
                                     headers=None):
        if not instance_ids:
            return True

        payload = {
            'instanceIds': ','.join(instance_ids),
            'userId': int(account_id),
            'source': 2
        }
        code, ret = await http.post(f'{self.endpoint}/trade/batchRefundInstances', data=payload, headers=headers)

        if 199 < code < 300:
            return True
        return False

    @_prepare
    async def get_instance_info(self, instance_id, headers=None):
        if not instance_id:
            return {}
        params = {
            'instanceId': instance_id,
        }
        headers.setdefault('host', 'instance.inner.sdns.ksyun.com')
        _, res = await http.get(f'{self.instance_endpoint}/instance/info', params=params, headers=headers)
        return res.get('data', {})
