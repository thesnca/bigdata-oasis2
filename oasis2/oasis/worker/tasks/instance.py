import asyncio

from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.cluster_order import ClusterOrderModel
from oasis.db.models.instance import InstanceModel
from oasis.db.models.instance_group import InstanceGroupModel
from oasis.utils import sdk
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger
from oasis.utils.sdk import gringotts_client
from oasis.utils.sdk.base import create_kec_instance_cluster
from oasis.utils.sdk.base import instance_add
from oasis.utils.sdk.charging.base import form_epc_param
from oasis.utils.sdk.charging.base import form_kec_param
from oasis.worker.tasks import BaseTask
from oasis.worker.tasks import check_rollback
from oasis.worker.tasks import check_task
from oasis.worker.tasks import set_rolled_instance


class TaskCreateInstance(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        order_id = self.args.get('order_id', None)
        sec_group_id = self.args.get('security_group_id', None)
        sub_orders = self.args.get('sub_orders')
        product_details = self.args.get('product_details', None)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        cluster_type = cluster.cluster_type

        logger.info(self, f'Create Instance ==sub orders==> {sub_orders}')

        _kec_param_instance_list = []
        _epc_param_instance_list = []

        _epc_all_instance_list = []
        _kec_all_instance_list = []

        kec_client = getattr(sdk, f'kec_client_{cluster_type.lower()}')
        epc_client = getattr(sdk, f'epc_client_{cluster_type.lower()}')

        # form kec \ epc params
        for instance_group in cluster.instance_groups:
            data_guard_id = None
            instance_group_type = instance_group.instance_group_type
            resource_type = instance_group.resource_type

            # Validate create count
            start_index = instance_group.count + 1
            create_count = instance_group.dest_count - instance_group.count
            if create_count <= 0:
                continue

            # Validate suborder count
            ng_sub_orders = sub_orders.get(instance_group_type, {})
            ng_service_suborders = ng_sub_orders.get(cluster_type.lower(), [])
            ng_sub_order_count = len(ng_service_suborders)
            if create_count != ng_sub_order_count:
                raise Exception(f'Create instance failed, create count = {create_count}, '
                                f'ng_sub_order = {ng_sub_order_count}, does not match.')

            # KEC
            if resource_type == 'KEC':
                _kec_suborders = ng_sub_orders.get('kec', [])
                _ebs_suborders = ng_sub_orders.get('ebs', [])

                # Validate kec count
                if create_count != len(_kec_suborders):
                    raise Exception(f'Create KEC instance failed, create count = {create_count}, '
                                    f'kec_suborders = {len(_kec_suborders)}, does not match.')

                # Validate ebs count
                if instance_group.volume_type.startswith('CLOUD_'):
                    if create_count * instance_group.volume_count != len(_ebs_suborders):
                        raise Exception(f'Create instance failed, create count = {create_count}, '
                                        f'volume_count = {instance_group.volume_count}, '
                                        f'ebs_suborders = {len(_ebs_suborders)}, does not match.')

                # Data guard only support 10 kec
                # TODO manage vm and data guard @WANGYAZHOU
                # kec D4 do not use data guard @ZHUBEIBEI1
                # is_kec_d4 = '.D4.' in instance_group.instance_type_code.upper()
                # if create_count <= 10 and not is_kec_d4:
                #     data_guard_id = await kec_client.create_data_guard_group(
                #         instance_group.name, account_id=account_id)

                # Construct kec param
                _param_instance = await form_kec_param(
                    create_count, cluster, instance_group,
                    sub_order_id=_kec_suborders[0],
                    sec_group_id=sec_group_id,
                    data_guard_id=data_guard_id,
                    idx=start_index,
                    product_details=product_details,
                )

                _kec_param_instance_list.append({
                    'cluster_id': cluster_id,
                    'instance_group_id': instance_group.id,
                    'instance_group_type': instance_group_type,
                    'data_guard_id': data_guard_id,
                    'volume_type': instance_group.volume_type,
                    'volume_count': instance_group.volume_count,
                    'kec_params': _param_instance,
                    'ng_service_suborders': ng_service_suborders,
                    'ng_ebs_suborders': _ebs_suborders,
                    'instance_name_prefix': _param_instance.get('InstanceName'),  # The only way to recognize kec
                })

            # EPC
            elif resource_type == 'EPC':
                epc_suborders = ng_sub_orders.get('epc', [])
                if create_count != len(epc_suborders):
                    raise Exception(f'Create EPC instance failed, create count = {create_count}, '
                                    f'epc_suborders = {len(epc_suborders)}, does not match.')

                host_type = '-'.join(instance_group.instance_type_code.split('-')[:-1])
                for idx, epc_sub_order_id in enumerate(epc_suborders):
                    _epc_param = form_epc_param(cluster, instance_group, epc_sub_order_id,
                                                sec_group_id=sec_group_id, idx=start_index + idx,
                                                host_type=host_type,
                                                kes_agent=True)

                    _epc_param_instance_list.append({
                        'cluster_id': cluster_id,
                        'instance_group_id': instance_group.id,
                        'instance_group_type': instance_group_type,
                        'epc_params': _epc_param,
                        'epc_sub_order_id': epc_sub_order_id,
                        'service_suborder_id': ng_service_suborders[idx],
                    })

        logger.info(self, f'Create Instance ==_kec_param_instance_list==> {_kec_param_instance_list}')
        logger.info(self, f'Create Instance ==_epc_param_instance_list==> {_epc_param_instance_list}')

        # Start create kec
        all_kec_instance_dict = {}
        all_instance_suborder_dict = {}
        if _kec_param_instance_list:
            all_kec_instance_dict, all_instance_suborder_dict = await create_kec_instance_cluster(
                [kec_params.get('kec_params') for kec_params in _kec_param_instance_list],
                order_id, account_id, kec_client)
            _kec_all_instance_list.extend(all_kec_instance_dict.keys())

        logger.info(self, f'Create Instance ==all_kec_instance_dict==> {all_kec_instance_dict}')
        logger.info(self, f'Create Instance ==all_instance_suborder_dict==> {all_instance_suborder_dict}')

        # Start create epc
        for _epc_params in _epc_param_instance_list:
            epc_info = await epc_client.create_instance(_epc_params.get('epc_params'), account_id=account_id)
            _epc_params.setdefault('instance_id', epc_info.get('HostId'))
            _epc_params.setdefault('instance_name', epc_info.get('HostName'))
            _epc_params.setdefault('service_instance_id', gen_uuid4())
            _epc_all_instance_list.append(epc_info.get('HostId'))

        # Wait kec create
        all_kec_active = await kec_client.wait_create_active(_kec_all_instance_list, account_id)
        if not all_kec_active:
            # TODO make better alert
            raise Exception(f'Not all kec are active, task failed.')
        logger.info(self, f'Create Instance ==_kec_all_instance_list==> {_kec_all_instance_list}')

        # Update database
        for instance_id, instance_info in all_kec_instance_dict.items():
            instance_name = instance_info.get('instance_name')
            service_instance_id = instance_info.get('service_instance_id')
            kec_param = {}

            # Only way to find instance group of kec
            for kec_params in _kec_param_instance_list:
                instance_name_prefix = kec_params.get('instance_name_prefix')
                if instance_name.startswith(instance_name_prefix):
                    kec_param = kec_params
                    break

            instance_group_id = kec_param.get('instance_group_id')
            instance_group_type = kec_param.get('instance_group_type')
            data_guard_id = kec_param.get('data_guard_id')
            volume_type = kec_param.get('volume_type')
            volume_count = kec_param.get('volume_count')
            ng_sub_orders = sub_orders.get(instance_group_type, {})
            ng_service_suborders = ng_sub_orders.get(cluster_type.lower(), [])
            ebs_suborders = ng_sub_orders.get('ebs', [])

            logger.info(self, f'Create Instance ==instance_id==> {instance_id}, \n'
                              f'==instance_name==> {instance_name}, \n'
                              f'==kec_param==> {kec_param}, \n'
                              f'==ng_service_suborders==> {ng_service_suborders}, \n'
                              f'==ebs_suborders==> {ebs_suborders}, \n'
                        )

            instance_db_info = {
                'instance_id': instance_id,
                'instance_name': instance_info.get('instance_name'),
                'service_instance_id': service_instance_id,
                'data_guard_id': data_guard_id,
            }
            await instance_add(instance_group_id, instance_db_info)

            instance_ebs_suborders = []
            if volume_type.startswith('CLOUD_'):
                for i in range(volume_count):
                    instance_ebs_suborders.append(ebs_suborders.pop())

            data = {
                'kec_order_id': all_instance_suborder_dict.get(instance_id).get('subOrderId'),
                'kec_instance_id': instance_id,
                'service_order_id': ng_service_suborders.pop(),
                'service_instance_id': instance_info.get('service_instance_id'),
                'ebs_order_id': instance_ebs_suborders,
            }

            model = ClusterOrderModel()
            model.update({
                'order_id': order_id,
                'cluster_id': cluster_id,
                'instance_group_id': instance_group_id,
                'instance_id': instance_id,
                'data': data,
            })
            await model.save()

        # Wait epc create
        all_epc_active = await epc_client.wait_create_active(_epc_all_instance_list, account_id)
        if not all_epc_active:
            # TODO make better alert
            raise Exception(f'Not all epc are active, task failed.')

        logger.info(self, f'Create Instance ==_epc_all_instance_list==> {_epc_all_instance_list}')

        for epc_info in _epc_param_instance_list:
            instance_group_id = epc_info.get('instance_group_id')
            instance_id = epc_info.get('instance_id')
            service_instance_id = epc_info.get('service_instance_id')
            instance_db_info = {
                'instance_id': instance_id,
                'instance_name': epc_info.get('instance_name'),
                'service_instance_id': service_instance_id,
                'status': InstanceModel.STATUS.ACTIVE,
            }
            await instance_add(instance_group_id, instance_db_info)

            data = {
                'epc_order_id': epc_info.get('epc_sub_order_id'),
                'epc_instance_id': instance_id,
                'service_order_id': epc_info.get('service_suborder_id'),
                'service_instance_id': service_instance_id,
            }

            model = ClusterOrderModel()
            model.update({
                'order_id': order_id,
                'cluster_id': cluster_id,
                'instance_group_id': instance_group_id,
                'instance_id': instance_id,
                'data': data,
            })
            await model.save()

        res = {
            'kec_instance_ids': _kec_all_instance_list,
            'epc_instance_ids': _epc_all_instance_list,
            'new_instance_ids': _kec_all_instance_list + _epc_all_instance_list,
        }
        self.context.update(res)
        return res

    @check_rollback
    async def rollback(self):
        account_id = self.context.get('account_id', '')
        cluster_id = self.context.get('cluster_id', None)

        new_instance_ids = self.context.get('new_instance_ids', [])
        new_service_instance_ids = []

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        kec_client = getattr(sdk, f'kec_client_{cluster.cluster_type.lower()}')
        epc_client = getattr(sdk, f'epc_client_{cluster.cluster_type.lower()}')

        kec_instance_ids = []
        epc_instance_ids = []

        for ig in cluster.instance_groups:
            for instance in ig.instances:
                if instance.instance_id not in new_instance_ids:
                    continue

                await instance.save({'status': InstanceModel.STATUS.DELETING})
                new_service_instance_ids.append(instance.service_instance_id)

                if ig.resource_type == 'KEC':
                    kec_instance_ids.append(instance.instance_id)
                elif ig.resource_type == 'EPC':
                    epc_instance_ids.append(instance.instance_id)

        # 创建过程未回写订单，不需要删除，订单回写失败即可
        # res = await kec_client.delete_instances(
        #     instance_ids=kec_instance_ids,
        #     account_id=account_id
        # )
        # if not res:
        #     raise Exception(f'Delete kec instance failed, instance ids {kec_instance_ids}')
        #
        # for epc_instance_id in epc_instance_ids:
        #     res = await epc_client.delete_instance(
        #         host_id=epc_instance_id,
        #         account_id=account_id
        #     )
        #     if not res:
        #         raise Exception(f'Delete epc failed, host id {epc_instance_id}')
        #
        # kec_res = await kec_client.wait_instances_delete(instance_ids=kec_instance_ids, account_id=account_id)
        # if not kec_res:
        #     raise Exception(f'Delete kec instance not fully complete, instance ids {kec_instance_ids}')
        #
        # epc_res = await epc_client.wait_instances_delete(instance_ids=epc_instance_ids, account_id=account_id)
        # if not epc_res:
        #     raise Exception(f'Delete epc instance not fully complete, host ids {epc_instance_ids}')

        for ig in cluster.instance_groups:
            for instance in ig.instances:
                if instance.instance_id not in new_instance_ids:
                    continue

                await instance.delete(hard=True)

        for service_instance_id in new_service_instance_ids:
            await set_rolled_instance(service_instance_id, cluster_id)

        return True


class TaskStopInstance(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        kec_client = getattr(sdk, f'kec_client_{cluster.cluster_type.lower()}')
        epc_client = getattr(sdk, f'epc_client_{cluster.cluster_type.lower()}')

        kec_instance_ids = []
        epc_instance_ids = []

        for ig in cluster.instance_groups:
            for instance in ig.instances:
                if ig.resource_type == 'KEC':
                    kec_instance_ids.append(instance.instance_id)
                elif ig.resource_type == 'EPC':
                    epc_instance_ids.append(instance.instance_id)

        res = await kec_client.stop_instances(
            instance_ids=kec_instance_ids,
            account_id=account_id
        )
        if not res:
            raise Exception(f'Stop kec instance failed, instance ids {kec_instance_ids}')

        for epc_instance_id in epc_instance_ids:
            res = await epc_client.stop_instance(
                host_id=epc_instance_id,
                account_id=account_id
            )
            if not res:
                raise Exception(f'Stop epc failed, host id {epc_instance_id}')

        kec_res = await kec_client.wait_create_active(instance_ids=kec_instance_ids,
                                                      flag_state='stopped',
                                                      account_id=account_id)
        if not kec_res:
            raise Exception(f'Stop kec instance not fully complete, instance ids {kec_instance_ids}')

        epc_res = await epc_client.wait_create_active(instance_ids=epc_instance_ids,
                                                      flag_state='Stopped',
                                                      account_id=account_id)
        if not epc_res:
            raise Exception(f'Stop epc instance not fully complete, host ids {epc_instance_ids}')

        for ig in cluster.instance_groups:
            for instance in ig.instances:
                await instance.save({'status': InstanceModel.STATUS.STOPPED})

        return True

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        return True


class TaskStartInstance(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        kec_client = getattr(sdk, f'kec_client_{cluster.cluster_type.lower()}')
        epc_client = getattr(sdk, f'epc_client_{cluster.cluster_type.lower()}')

        kec_instance_ids = []
        epc_instance_ids = []

        for ig in cluster.instance_groups:
            for instance in ig.instances:
                if ig.resource_type == 'KEC':
                    kec_instance_ids.append(instance.instance_id)
                elif ig.resource_type == 'EPC':
                    epc_instance_ids.append(instance.instance_id)

        res = await kec_client.start_instances(
            instance_ids=kec_instance_ids,
            account_id=account_id
        )
        if not res:
            raise Exception(f'Start kec instance failed, instance ids {kec_instance_ids}')

        for epc_instance_id in epc_instance_ids:
            res = await epc_client.start_instance(
                host_id=epc_instance_id,
                account_id=account_id
            )
            if not res:
                raise Exception(f'Stop epc failed, host id {epc_instance_id}')

        kec_res = await kec_client.wait_create_active(instance_ids=kec_instance_ids,
                                                      flag_state='active',
                                                      account_id=account_id)
        if not kec_res:
            raise Exception(f'Start kec instance not fully complete, instance ids {kec_instance_ids}')

        epc_res = await epc_client.wait_create_active(instance_ids=epc_instance_ids,
                                                      flag_state='Running',
                                                      account_id=account_id)
        if not epc_res:
            raise Exception(f'Start epc instance not fully complete, host ids {epc_instance_ids}')

        for ig in cluster.instance_groups:
            for instance in ig.instances:
                await instance.save({'status': InstanceModel.STATUS.ACTIVE})

        return True

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        return True


class TaskDeleteInstance(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        # 待缩实例
        scale_in_instance_ids = self.args.get('scale_in_instance_ids', None)
        # 待缩实例组（含实例
        scale_in_instance_groups = self.args.get('scale_in_instance_groups', None)

        kec_client = getattr(sdk, f'kec_client_{cluster.cluster_type.lower()}')
        epc_client = getattr(sdk, f'epc_client_{cluster.cluster_type.lower()}')

        kec_instance_ids = []
        epc_instance_ids = []

        for ig in cluster.instance_groups:
            for instance in ig.instances:
                if not scale_in_instance_ids or instance.instance_id in scale_in_instance_ids:
                    await instance.save({'status': InstanceModel.STATUS.DELETING})

                    if ig.resource_type == 'KEC':
                        kec_instance_ids.append(instance.instance_id)
                    elif ig.resource_type == 'EPC':
                        epc_instance_ids.append(instance.instance_id)

        res = await kec_client.delete_instances(
            instance_ids=kec_instance_ids,
            account_id=account_id
        )
        if not res:
            raise Exception(f'Delete kec instance failed, instance ids {kec_instance_ids}')

        for epc_instance_id in epc_instance_ids:
            res = await epc_client.delete_instance(
                host_id=epc_instance_id,
                account_id=account_id
            )
            if not res:
                raise Exception(f'Delete epc failed, host id {epc_instance_id}')

        kec_res = await kec_client.wait_instances_delete(instance_ids=kec_instance_ids, account_id=account_id)
        if not kec_res:
            raise Exception(f'Delete kec instance not fully complete, instance ids {kec_instance_ids}')

        epc_res = await epc_client.wait_instances_delete(instance_ids=epc_instance_ids, account_id=account_id)
        if not epc_res:
            raise Exception(f'Delete epc instance not fully complete, host ids {epc_instance_ids}')

        for ig in cluster.instance_groups:
            for instance in ig.instances:
                if not scale_in_instance_ids or instance.instance_id in scale_in_instance_ids:
                    await instance.save({'status': InstanceModel.STATUS.DELETED})

            # 按量减少实例组中 count/dest_count字段值
            # 当count为0时，关闭该组
            # dest_count字段为操作前的预期值（历史遗留字段，当前没有应用该字段
            # 只提供给缩容操作使用。释放操作不进行该操作
            if scale_in_instance_groups and ig.id in scale_in_instance_groups:
                temp_dest_count = ig.count - len(scale_in_instance_groups[ig.id])
                temp_group = {'count': temp_dest_count, 'dest_count': temp_dest_count}
                if temp_dest_count == 0:
                    temp_group['status'] = InstanceGroupModel.STATUS.DELETED
                await ig.save(temp_group)

        return True

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        return True


class TaskDeleteDataguard(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        kec_client = getattr(sdk, f'kec_client_{cluster.cluster_type.lower()}')

        data_guard_ids = []
        for ig in cluster.instance_groups:
            # resource_type = instance_group.resource_type
            for ins in ig.instances:
                if ins.data_guard_id:
                    data_guard_ids.append(ins.data_guard_id)

        data_guard_ids = list(set(data_guard_ids))

        res = await kec_client.delete_data_guard_group(guard_ids=data_guard_ids, account_id=account_id)
        if not res:
            raise Exception(f'Delete dataguard failed, data guard ids {data_guard_ids}')

        return True

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        return True


class TaskCreateSshKey(BaseTask):
    @check_task
    async def run(self):
        product = self.args.get('product', 'kes')
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        sks_client = getattr(sdk, f'sks_client_{product}')

        key_res = await sks_client.create_key(account_id=account_id)

        await cluster.save(key_res)
        return key_res

    @check_rollback
    async def rollback(self):
        product = self.context.get('product', 'kes')
        account_id = self.context.get('account_id', None)
        cluster_id = self.context.get('cluster_id', None)
        # Cluster may not start create yet
        try:
            cluster = await get_model_by_id(ClusterModel, cluster_id)
            sks_client = getattr(sdk, f'sks_client_{product}')

            await sks_client.delete_key(key_id=cluster.management_keypair_id,
                                        account_id=account_id)
        except:
            pass

        return True


class TaskDeleteEpcKey(BaseTask):
    @check_task
    async def run(self):
        product = self.args.get('product', 'kes')
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        sks_client = getattr(sdk, f'sks_client_{product}')

        await sks_client.delete_key(key_id=cluster.management_keypair_id,
                                    account_id=account_id)

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        return True


class TaskCheckInstanceReady(BaseTask):
    @check_task
    async def run(self):
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        instances = []
        for ig in cluster.instance_groups:
            instances.extend(ig.instances)

        for instance in instances:
            retry = 3
            while retry:
                retry -= 1
                try:
                    logger.info(f'Try remote ssh to instance [{instance.instance_id}] {instance.instance_name}')
                    remote = await instance.remote()
                    async with remote as conn:
                        await conn.execute('ls')
                    break
                except:
                    logger.warn(f'Remote ssh to instance [{instance.instance_id}] {instance.instance_name} failed, '
                                f'retry {3 - retry} / 3 times.')
                    # EPC is very slow
                    await asyncio.sleep(120)
                    if retry == 0:
                        raise Exception(f'Remote ssh to instance '
                                        f'[{instance.instance_id}] {instance.instance_name} failed.')

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        return True


class TaskUpdateInstanceStatus(BaseTask):
    @check_task
    async def run(self):
        cluster_id = self.args.pop('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        status = self.args.pop('status', None)
        if not status:
            raise Exception(f'Instance status not set, id {cluster_id}')

        for ig in cluster.instance_groups:
            for ins in ig.instances:
                await ins.save({'status': status})

        return {'cluster_id': cluster_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskUpgradeInstance(BaseTask):

    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        is_upgrade_kec = self.args.get('is_upgrade_kec', False)
        if not is_upgrade_kec:
            return True
        is_upgrade_ebs = self.args.get('is_upgrade_ebs', False)

        upgrade_instance_group = self.args.get('upgrade_instance_group', {})

        upgrade_instance_type = upgrade_instance_group['instance_type']
        upgrade_instance_type_code = upgrade_instance_group['instance_type_code']

        # 如果'本地'磁盘没有'升配'，这个项最好不要传。
        upgrade_volume_size = upgrade_instance_group.get(
            'volume_size', None) if not is_upgrade_ebs else None

        kec_client = getattr(sdk, f'kec_client_{cluster.cluster_type.lower()}')

        origin_instance_group = None
        for ig in cluster.instance_groups:
            if ig.resource_type == 'KEC' and ig.id == upgrade_instance_group.get('id', ''):
                origin_instance_group = ig

        keclst = []
        for origin_instance in origin_instance_group.instances:
            # 套餐不等于目标且状态为active
            check_flag = await kec_client.check_instance_upgrade(
                instance_id=origin_instance.instance_id,
                upgrade_instance_type=upgrade_instance_type,
                account_id=account_id)
            if check_flag:
                keclst.append(origin_instance.instance_id)
                res = await kec_client.modify_instance_type(
                    instance_id=origin_instance.instance_id,
                    instance_type=upgrade_instance_type,
                    data_disk_gb=upgrade_volume_size,
                    account_id=account_id,
                    auto_notify=0
                )
                if not res:
                    raise Exception(f'Upgrade kec instance failed, instance ids {origin_instance.instance_id}')

        for instance_id in keclst:
            upgrade_flag = await kec_client.wait_instance_upgrade(
                instance_id=instance_id,
                upgrade_instance_type=upgrade_instance_type,
                account_id=account_id)

            if not upgrade_flag:
                raise Exception(f'Upgrade kec instance not fully complete, instance id is {instance_id}')

        upgrade_entity = {'instance_type_code': upgrade_instance_type_code}
        if upgrade_volume_size:
            upgrade_entity['volume_size'] = upgrade_volume_size
        await origin_instance_group.save(upgrade_entity)

        return True

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        # 如果失败，很难回滚
        return True


class TaskRollingRestart(BaseTask):
    @check_task
    async def run(self):
        token = self.args.pop('auth_token', None)
        if not token:
            raise Exception('Cannot verify user')

        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        restart_instance_id = self.args.get('restart_instance_id', False)
        if not restart_instance_id:
            raise Exception('Please specify restart_instance_id')

        is_upgrade_kec = self.args.get('is_upgrade_kec', False)
        if not is_upgrade_kec:
            return True

        kec_client = getattr(sdk, f'kec_client_{cluster.cluster_type.lower()}')
        account_id = self.args.get('account_id', '')

        gg_kwargs = {
            'cluster_id': cluster_id,
            'instances': [restart_instance_id],
            'component': 'ELASTICSEARCH',
            'control_type': '',  # stop & start
        }

        describe_res = await kec_client.describe_instances(instance_ids=gg_kwargs['instances'],
                                                           account_id=account_id)

        if not describe_res:
            raise Exception(f'Could not find instance KEC {restart_instance_id}')

        # 判断KEC状态：升配后未重启
        # https://docs.ksyun.com/documents/816)
        # TODO: 检查这个状态的作用，是为了提供“任务流”级别的重试。
        instance_state = describe_res[0].get('InstanceState', '')

        # 完成升配后才执行下面操作...
        if instance_state not in ['migrating_success', 'migrating_success_off_line', 'resize_success_local']:
            return True

        # 升配后：先停es，然后重启主机，等待30s后，再启es:
        logger.info(self, f'Start gringotts component control... gg_kwargs: {gg_kwargs}, token:{token}')

        # 先停es
        gg_kwargs['control_type'] = 'stop'
        _op_id = await gringotts_client.component_control(token=token, **gg_kwargs)
        await gringotts_client.wait_gg_op_active(_op_id, token=token)

        # 然后重启主机
        # 调用KEC强制重启服务 ForceReboot=True
        # https://docs.ksyun.com/documents/810
        # TODO: 滚动重启，重启一台后，再重启下一台，不然一直等待
        change_instance_state = await kec_client.wait_reboot_instances(
            gg_kwargs['instances'],
            account_id=cluster.ksc_user_id,
            force_reboot='false'
        )

        if not change_instance_state:
            raise Exception(f'reboot instances [{restart_instance_id}] failed.')

        is_active = await kec_client.wait_create_active(instance_ids=gg_kwargs['instances'],
                                                        flag_state='active',
                                                        account_id=account_id)
        if not is_active:
            raise Exception(f'Reboot kec instances not fully complete, instance ids [{restart_instance_id}] is not active')

        # 重启主机之后最好先等待30s
        await asyncio.sleep(30)

        # 再启es
        gg_kwargs['control_type'] = 'start'
        _op_id = await gringotts_client.component_control(token=token, **gg_kwargs)
        await gringotts_client.wait_gg_op_active(_op_id, token=token)

        return True

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        # 如果失败，很难回滚
        return True
