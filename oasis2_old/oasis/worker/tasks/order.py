from conf.charge_conf import MAIN_INSTANCE_POLICY
from conf.charge_conf import PRODUCT_GROUP_ID_MAP
from conf.charge_conf import PRODUCT_GROUP_MAP
from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.cluster_order import ClusterOrderModel
from oasis.utils import sdk
from oasis.utils.exceptions import ChargeException
from oasis.utils.logger import logger
from oasis.utils.sdk import charge_client
from oasis.utils.sdk import price_client
from oasis.utils.sdk.charging.base import create_products, upgrade_products
from oasis.utils.sdk.charging.base import get_all_suborders_format
from oasis.worker.tasks import BaseTask
from oasis.worker.tasks import check_rollback
from oasis.worker.tasks import check_task


class TaskCreateProduct(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        order_id = self.args.get('order_id', None)
        source = self.args.get('source', 3)
        product_details = self.args.get('product_details', {})

        order_product_details = None
        if not order_id:
            order_product_details, _ = await create_products(self.args, product_details, account_id, source=source)

        return {
            'order_product_details': order_product_details,
            'order_id': order_id,
        }

    @check_rollback
    async def rollback(self):
        # Do not need to rollback
        return True


class TaskUpgradeProduct(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        order_id = self.args.get('order_id', None)
        source = self.args.get('source', 3)
        product_details = self.args.get('product_details', {})

        order_product_details = None
        if not order_id:
            order_product_details, _ = await upgrade_products(self.args, product_details, account_id, source=source)

        return {
            'order_product_details': order_product_details,
            'order_id': order_id,
        }

    @check_rollback
    async def rollback(self):
        # Do not need to rollback
        return True


class TaskCreateOrder(BaseTask):
    @check_task
    async def run(self):
        order_id = self.args.get('order_id', None)
        account_id = self.args.get('account_id', None)
        cluster_type = self.args.get('cluster_type', None)
        if not order_id:
            order_product_details = self.args.get('order_product_details', None)
            order_products = []
            for key in order_product_details:
                if key in ('EIP', 'SLB'):
                    order_products.append({
                        'productId': order_product_details[key]['product_id'],
                        'num': order_product_details[key]['num'],
                        'productGroup': order_product_details[key]['product_group']
                    })
                else:
                    instance_group_details = order_product_details[key]
                    for k in instance_group_details:
                        order_products.append({
                            'productId': instance_group_details[k]['product_id'],
                            'num': instance_group_details[k]['num'],
                            'productGroup': instance_group_details[k]['product_group']
                        })

            order_id = await charge_client.create_order(account_id, 'BUY', order_products, cluster_type)

        sub_orders = await get_all_suborders_format(order_id)
        logger.info(self, f'==order_id: {order_id}, ==sub_orders: {sub_orders}')
        self.context['order_id'] = order_id
        self.context.setdefault('sub_orders', sub_orders)

        return {'order_id': order_id}

    @check_rollback
    async def rollback(self):
        # Write sub orders to failed
        order_id = self.context.get('order_id', None)
        cluster_type = self.context.get('cluster_type', None)
        account_id = self.context.get('account_id', None)

        if not order_id:
            logger.warn(self, f'Rollback TaskCreateOrder failed, order_id {order_id} not found.')

        suborder_list = await charge_client.query_sub_orders_by_order_id(order_id)
        kec_suborder_dict = {}
        epc_suborder_dict = {}
        ebs_suborder_dict = {}
        other_suborder_list = []
        for suborder in suborder_list:
            suborder_id = suborder.get('subOrderId')
            product_group = suborder.get('productGroup')
            instance_id = suborder.get('instanceId', None)

            if product_group == PRODUCT_GROUP_MAP['KEC'] and instance_id:
                kec_suborder_dict.setdefault(suborder_id, instance_id)
            elif product_group in [PRODUCT_GROUP_MAP['EPC'], PRODUCT_GROUP_MAP['GEPC']] and instance_id:
                epc_suborder_dict.setdefault(suborder_id, instance_id)
            elif product_group == PRODUCT_GROUP_MAP['EBS'] and instance_id:
                ebs_suborder_dict.setdefault(suborder_id, instance_id)
            elif not instance_id:
                other_suborder_list.append(suborder_id)

        logger.info(self, f'Rollback TaskCreateOrder kec_suborder_dict: {kec_suborder_dict}')
        logger.info(self, f'Rollback TaskCreateOrder epc_suborder_dict: {epc_suborder_dict}')
        logger.info(self, f'Rollback TaskCreateOrder ebs_suborder_dict: {ebs_suborder_dict}')
        logger.info(self, f'Rollback TaskCreateOrder other_suborder_list: {other_suborder_list}')

        kec_client = getattr(sdk, f'kec_client_{cluster_type.lower()}')
        epc_client = getattr(sdk, f'epc_client_{cluster_type.lower()}')
        ebs_client = getattr(sdk, f'ebs_client_{cluster_type.lower()}')
        
        # TODO: 暂时屏蔽写订单失败，方便你测试，不回滚删除主机...测试完一定要打开下面注释！！！
        # Failed
        status = 2
        for suborder_id, instance_id in kec_suborder_dict.items():
            await kec_client.notify_suborder_status(instance_id, suborder_id, status, account_id=account_id)
        for suborder_id, instance_id in epc_suborder_dict.items():
            await epc_client.notify_suborder_status(instance_id, suborder_id, status, account_id=account_id)
        for suborder_id, instance_id in ebs_suborder_dict.items():
            await ebs_client.notify_suborder_status_ebs(instance_id, suborder_id, status, account_id=account_id)
        for suborder_id in other_suborder_list:
            await charge_client.notify_suborder_status(suborder_id, status)

        return True


class TaskDeleteServiceInstance(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.get('cluster_id', None)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        scale_in_instance_ids = self.args.get('scale_in_instance_ids', None)
        service_instance_ids = []

        instances = []
        for ig in cluster.instance_groups:
            instances.extend(ig.instances)
        
        for instance in instances:
            if not scale_in_instance_ids or instance.instance_id in scale_in_instance_ids:
                if instance.service_instance_id:
                    service_instance_ids.append(instance.service_instance_id)

        delete_instances = await charge_client.batch_delete_instances(service_instance_ids, account_id)
        if not delete_instances:
            raise Exception(f'Delete instance failed, service_instance_ids {service_instance_ids}')

        return True

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        return True


class TaskNotifyOrder(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.get('cluster_id', None)
        order_id = self.args.get('order_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if not order_id:
            raise ChargeException(f'Charge fail at order_id not exists! cluster_id: {cluster_id}')

        # As long as arrive here, all orders is succeed
        status = 1
        cluster_type = cluster.cluster_type
        cluster_extra = dict(cluster.extra)
        main_instance_id = cluster_extra.get('main_instance_id', None)

        query = model_query(ClusterOrderModel)
        query.filter(ClusterOrderModel.order_id == order_id)
        sub_orders = await query.query_all()

        # Find main instance
        _mp = MAIN_INSTANCE_POLICY.get(cluster_type, {})
        main_instance = None

        if not main_instance_id:
            main_instance = None
            for _ng in cluster.instance_groups:
                if main_instance:
                    break
                if _mp.get('INSTANCE_GROUP_TYPE', '') != _ng.instance_group_type:
                    continue
                for _instance in _ng.instances:
                    if _instance.instance_name.endswith(_mp.get('INSTANCE_NAME', '$None$')) or \
                            _instance.instance_name.endswith(_mp.get('EPC_INSTANCE_NAME', '$None$')):
                        main_instance = _instance
                        break

            if not main_instance:
                raise ChargeException(f'Charge fail at main_instance not exists! cluster_id: {cluster_id}')

            main_instance_id = main_instance.service_instance_id
            cluster_extra['main_instance_id'] = main_instance_id
            await cluster.save({'extra': cluster_extra})

        main_product_group = PRODUCT_GROUP_ID_MAP[cluster_type]

        kec_suborder_dict = dict()
        epc_suborder_dict = dict()
        ebs_suborder_dict = dict()

        for sub_order in sub_orders:
            sub_order_data = sub_order.get('data', {})
            service_order_id = sub_order_data.get('service_order_id', None)
            service_instance_id = sub_order_data.get('service_instance_id', None)

            # Main order
            if main_instance and sub_order.instance_id == main_instance.instance_id:
                await charge_client.notify_suborder_status(service_order_id, status,
                                                           instance_id=service_instance_id)

            # 其他服务费需要关联到主实例上
            else:
                await charge_client.notify_suborder_status(service_order_id, status,
                                                           instance_id=service_instance_id,
                                                           owner_product_group=main_product_group,
                                                           owner_instance_id=main_instance_id)

            if 'kec_order_id' in sub_order_data:
                kec_suborder_dict.setdefault(sub_order.instance_id, sub_order_data.get('kec_order_id', None))

            if 'epc_order_id' in sub_order_data:
                epc_suborder_dict.setdefault(sub_order.instance_id, sub_order_data.get('epc_order_id', None))

            if 'ebs_order_id' in sub_order_data:
                for ebs_id, ebs_order_id in zip(sub_order_data.get('ebs_instance_id', []),
                                                sub_order_data.get('ebs_order_id', [])):
                    ebs_suborder_dict.setdefault(ebs_id, ebs_order_id)

        logger.info(self, f'kec_suborder_dict==>{kec_suborder_dict}')
        logger.info(self, f'epc_suborder_dict==>{epc_suborder_dict}')
        logger.info(self, f'ebs_suborder_dict==>{ebs_suborder_dict}')

        kec_client = getattr(sdk, f'kec_client_{cluster_type.lower()}')
        epc_client = getattr(sdk, f'epc_client_{cluster_type.lower()}')
        ebs_client = getattr(sdk, f'ebs_client_{cluster_type.lower()}')

        # kec, epc, ebs 需要调用各个业务线接口进行通知
        for instance_id, order_id in kec_suborder_dict.items():
            await kec_client.notify_suborder_status(instance_id, order_id, status,
                                                    owner_product_group=main_product_group,
                                                    owner_instance_id=main_instance_id,
                                                    account_id=account_id)

        for instance_id, order_id in epc_suborder_dict.items():
            await epc_client.notify_suborder_status(instance_id, order_id, status,
                                                    owner_product_group=main_product_group,
                                                    owner_instance_id=main_instance_id,
                                                    account_id=account_id)

        for instance_id, order_id in ebs_suborder_dict.items():
            await ebs_client.notify_suborder_status_ebs(instance_id, order_id, status,
                                                        owner_product_group=main_product_group,
                                                        owner_instance_id=main_instance_id,
                                                        account_id=account_id)

        return {'order_id': order_id, 'status': status}

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        return True


class TaskInitUpgradeOrder(BaseTask):
    '''
    升配流程与新建（扩容）不同，不需要等待业务线实例ID（新建）。
    所以可以预先加入cluster_order
    '''

    @check_task
    async def run(self):

        product = self.args.get('product', 'kes')
        # account_id = self.args.get('account_id', None)
        cluster_id = self.args.get('cluster_id', None)
        order_id = self.args.get('order_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if not order_id:
            raise ChargeException(f'Charge fail at order_id not exists! order_id: {order_id}')

        # 该函数下，这个变量是基础。
        order_info_res = await charge_client.find_all_order_info_for_upgrade(order_id)

        if not order_info_res:
            raise ChargeException(f'Charge fail at order_res not exists! order_id: {order_id}')

        logger.info(self, f'Upggrade Order Infos {order_info_res}')

        upgrade_instance_group = self.args.get('upgrade_instance_group', {})
        upgrade_instance_group_id = upgrade_instance_group.get('id', '')

        origin_instance_group = None
        for ig in cluster.instance_groups:
            if ig.resource_type == 'KEC' and ig.id == upgrade_instance_group_id:
                origin_instance_group = ig

        # 可以通过遍历order_info_res 直接入库，
        # 这里绕了一下的目的，是为了做下check
        for origin_instance in origin_instance_group.instances:

            # 每KEC实例一个data
            data = {}

            kec_order_infos = order_info_res.get('KEC', {})
            # 升配允许不升配KEC
            if kec_order_infos:
                # 升配必须全组升配
                data['kec_order_id'] = kec_order_infos[origin_instance.instance_id]
                data['kec_instance_id'] = origin_instance.instance_id

            ebs_order_infos = order_info_res.get('EBS', {})
            if ebs_order_infos:
                # 不允许存在订单，但没有实例的情况
                if not origin_instance.volumes:
                    raise ChargeException(f'Charge fail at origin instance volumes not exists!')
                # 多盘
                ebs_order_ids = []
                for origin_volume_id in origin_instance.volumes:
                    # 多盘不支持只升一块盘
                    ebs_order_ids.append(ebs_order_infos[origin_volume_id])

                data['ebs_order_id'] = ebs_order_ids
                data['ebs_instance_id'] = origin_instance.volumes

            service_order_infos = order_info_res.get(product.upper(), {})
            if service_order_infos:
                data['service_order_id'] = service_order_infos[origin_instance.service_instance_id]
                data['service_instance_id'] = origin_instance.service_instance_id

            if not data:
                raise ChargeException(f'Charge fail at cluster_order_entity not exists! order_id: {order_id} order_info_res: {order_info_res}')

            model = ClusterOrderModel()
            # 这里的instance_id 代表一种关联关系，而不是有kec的订单
            model.update({
                'order_id': order_id,
                'cluster_id': cluster_id,
                'instance_group_id': upgrade_instance_group_id,
                'instance_id': origin_instance.instance_id,
                'data': data,
            })
            await model.save()

        res = {'order_info_res': order_info_res}
        self.context.update(res)
        return res

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        # order status
        return True


class TaskNotifyOrderForUpgrade(BaseTask):

    @check_task
    async def run(self):
        cluster_id = self.args.get('cluster_id', None)
        order_id = self.args.get('order_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if not order_id:
            raise ChargeException(f'Charge fail at order_id not exists! cluster_id: {cluster_id}')

        # As long as arrive here, all orders is succeed
        status = 1

        order_info_res = self.args.get('order_info_res', {})

        logger.info(self, f'TaskNotifyOrderForUpgrade order_info_res:{order_info_res}')

        for _, service_dict in order_info_res.items():
            for _, service_sub_order_id in service_dict.items():
                await charge_client.notify_suborder_status(service_sub_order_id, status)

        return {'order_id': order_id, 'status': status}

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        return True
