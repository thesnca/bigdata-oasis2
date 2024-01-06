import asyncio

from conf.infra_conf import VOLUME_TYPE_MAP
from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.cluster_order import ClusterOrderModel
from oasis.utils import sdk
from oasis.utils.logger import logger
from oasis.worker.tasks import BaseTask
from oasis.worker.tasks import check_rollback
from oasis.worker.tasks import check_task


class TaskCreateEbs(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        order_id = self.args.get('order_id', None)
        new_instance_ids = self.args.get('new_instance_ids', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        ebs_client = getattr(sdk, f'ebs_client_{cluster.cluster_type.lower()}')

        query = model_query(ClusterOrderModel)
        query.filter(ClusterOrderModel.order_id == order_id)
        cluster_orders = await query.query_all()

        instance_order_data = {ebs_order.instance_id: ebs_order.data
                               for ebs_order in cluster_orders}
        logger.info(self, f'==========instance_order_data: {instance_order_data}')

        ebs_count = 0
        total_volume_ids = []

        for instance_group in cluster.instance_groups:
            if instance_group.volume_type.startswith('CLOUD_'):
                ebs_size = str(instance_group.volume_size)
                ebs_count += instance_group.count * instance_group.volume_count
                volume_key = f'{VOLUME_TYPE_MAP.get(instance_group.volume_type)}|{ebs_size}'

                for instance in instance_group.instances:
                    if instance.instance_id not in new_instance_ids:
                        continue

                    ebs_orders = {volume_key: instance_order_data[instance.instance_id]['ebs_order_id']}
                    ebs_instances = await ebs_client.create_ebs(ebs_orders, cluster.charge_type,
                                                                cluster.availability_zone,
                                                                purchase_time=cluster.purchase_time,
                                                                account_id=account_id)
                    volume_ids = [v_id for id_list in ebs_instances.values() for v_id in id_list]
                    total_volume_ids.extend(volume_ids)
                    await instance.save({'volumes': volume_ids})
                    data = instance_order_data[instance.instance_id]
                    logger.info(self, f'==vKey data: {data}')
                    data['ebs_instance_id'] = volume_ids

                    order_query = model_query(ClusterOrderModel)
                    order_query = order_query.filter(ClusterOrderModel.instance_id == instance.instance_id)
                    order_model = await order_query.query_one()
                    await order_model.save({'data': data})

        for volume_id in total_volume_ids:
            await ebs_client.wait_ebs_status(volume_id,
                                             ['available'],
                                             account_id=account_id)
        return {'total_volume_ids': total_volume_ids}

    @check_rollback
    async def rollback(self):
        # cluster_id = self.args.get('cluster_id', None)
        # account_id = self.args.get('account_id', '')
        # total_volume_ids = self.results.get('total_volume_ids', [])
        #
        # cluster = await get_model_by_id(ClusterModel, cluster_id)
        # ebs_client = getattr(sdk, f'ebs_client_{cluster.cluster_type.lower()}')
        #
        # for volume_id in total_volume_ids:
        #     await ebs_client.notify_suborder_status_ebs(volume_id, )
        return True


class TaskAttachEbs(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        new_instance_ids = self.args.get('new_instance_ids', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        ebs_client = getattr(sdk, f'ebs_client_{cluster.cluster_type.lower()}')

        attach_tasks = []

        for instance_group in cluster.instance_groups:
            for instance in instance_group.instances:
                instance_id = instance.instance_id
                if instance_id not in new_instance_ids:
                    continue

                if not instance.volumes:
                    continue

                for volume_id in instance.volumes:
                    attach_tasks.append(self.attach_ebs_and_wait(ebs_client, instance_id,
                                                                 volume_id, account_id))
        await asyncio.gather(*attach_tasks)

        return True

    @check_rollback
    async def rollback(self):
        account_id = self.context.get('account_id', '')
        cluster_id = self.context.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        ebs_client = getattr(sdk, f'ebs_client_{cluster.cluster_type.lower()}')

        for instance_group in cluster.instance_groups:
            for instance in instance_group.instances:
                if not instance.volumes:
                    continue
                for volume_id in instance.volumes:
                    try:
                        await ebs_client.detach_ebs(volume_id, account_id=account_id)
                    except:
                        pass

                    await ebs_client.wait_ebs_status(
                        volume_id, ['available'],
                        account_id=account_id)

    async def attach_ebs_and_wait(self, ebs_client, instance_id, volume_id, account_id):
        logger.info(self, f'Start attach ebs, instance: {instance_id}, volume: {volume_id}')
        await ebs_client.attach_ebs(instance_id, volume_id, account_id=account_id)

        await ebs_client.wait_ebs_status(
            volume_id, ['in-use'],
            account_id=account_id)

        logger.info(self, f'Finish attach ebs, instance: {instance_id}, volume: {volume_id}')


class TaskDetachEbs(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        ebs_client = getattr(sdk, f'ebs_client_{cluster.cluster_type.lower()}')

        for instance_group in cluster.instance_groups:
            for instance in instance_group.instances:
                if not instance.volumes:
                    continue
                for volume_id in instance.volumes:
                    await ebs_client.detach_ebs(volume_id, account_id=account_id)

                    await ebs_client.wait_ebs_status(
                        volume_id, ['available'],
                        account_id=account_id)

        return True

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        return True


class TaskReattachEbs(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        ebs_client = getattr(sdk, f'ebs_client_{cluster.cluster_type.lower()}')

        for instance_group in cluster.instance_groups:
            for instance in instance_group.instances:
                instance_id = instance.instance_id
                if not instance.volumes:
                    continue

                for volume_id in instance.volumes:
                    res = await ebs_client.attach_ebs(instance_id, volume_id,
                                                      account_id=account_id)
                    if not res:
                        raise Exception(f'Reattach ebs failed. Instance id {instance_id}')

                    await ebs_client.wait_ebs_status(
                        volume_id, ['in-use'],
                        account_id=account_id)

        return True

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        return True


class TaskMountEbs(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        new_instance_ids = self.args.get('new_instance_ids', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        ebs_client = getattr(sdk, f'ebs_client_{cluster.cluster_type.lower()}')

        mount_tasks = []
        for instance_group in cluster.instance_groups:
            resource_type = instance_group.resource_type
            instance_type_code = instance_group.instance_type_code
            if '.D4.' in instance_type_code.upper():
                resource_type = 'D4'

            for instance in instance_group.instances:
                instance_id = instance.instance_id
                if instance_id not in new_instance_ids:
                    continue

                mount_tasks.append(self.mount_ebs_and_wait(ebs_client, instance,
                                                           resource_type, account_id))

        await asyncio.gather(*mount_tasks)

        return True

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        return True

    async def mount_ebs_and_wait(self, ebs_client, instance, resource_type, account_id):
        logger.info(self, f'Start mount ebs, instance_id: {instance.instance_id}')
        remote = await instance.remote()
        async with remote as conn:
            mount_point_prefix = '/mnt'

            # KEC Cloud-Ebs
            if resource_type == 'KEC' and instance.volumes:
                for volume_id in instance.volumes:
                    await ebs_client.wait_ebs_status(
                        volume_id,
                        expect_status=['in-use'],
                        unexpect_status=['error'],
                        account_id=account_id,
                    )
                device_path_list = await ebs_client.get_mount_point(instance.volumes, account_id=account_id)

            # EPC
            elif resource_type == 'EPC':
                _, part_list = await conn.execute(
                    "cat /proc/partitions | awk '{print $4}' | grep -v sda | grep sd",
                    raise_when_error=False
                )
                if not part_list:
                    return
                device_path_list = [f'/dev/{part}' for part in part_list.split('\n')]

                await conn.execute(r'sudo sed -i "/\/dev\/sdb/,+1d;:go;{P;N;D};N;bgo" /etc/fstab')

            # KEC D4
            elif resource_type == 'D4':
                _, part_list = await conn.execute(
                    "cat /proc/partitions | awk '{print $4}' | grep -v vda | grep vd",
                    raise_when_error=False
                )
                if not part_list:
                    return
                device_path_list = [f'/dev/{part}' for part in part_list.split('\n')]

            # Local Ebs
            else:
                device_path_list = ['/dev/vdb']

            logger.info(self, f'===device_path_list===>{device_path_list}')

            for volume_index, device_path in enumerate(device_path_list):
                try:
                    if volume_index == 0:
                        mount_point = mount_point_prefix
                    else:
                        mount_point = f'{mount_point_prefix}{volume_index}'
                    fs_opts = '-i 262144 -m 1 -O dir_index,extents,^has_journal'
                    mount_opts = '-o data=writeback,noatime,nodiratime'
                    fstab_str = f'{device_path}    {mount_point}    ext4    ' \
                                f'data=writeback,noatime,nodiratime    0    0'

                    exit_status, std_out = await conn.execute(f'df -h | grep {device_path} | wc -l',
                                                              raise_when_error=False)
                    if exit_status == 0 and int(std_out) == 0:
                        exit_status, std_out = await conn.execute(f'sudo ls {mount_point}',
                                                                  raise_when_error=False)
                        if exit_status != 0:
                            await conn.execute(f'sudo mkdir {mount_point}',
                                               raise_when_error=False)
                        await conn.execute(f'rm -rf {mount_point}')
                        # TODO /etc/fstab

                        await conn.execute(f'sudo mkdir -p {mount_point}')
                        # epc need umount first
                        if resource_type == 'EPC':
                            await conn.execute(f'sudo umount {device_path}')
                        await conn.execute(f'sudo mkfs.ext4 -F {fs_opts} {device_path}',
                                           raise_when_error=False)
                        await conn.execute(f'sudo mount {mount_opts} {device_path} {mount_point}')
                        await conn.execute(f'sudo echo {fstab_str} >> /etc/fstab')
                        await conn.execute(f'chmod 777 {mount_point}')
                        await conn.execute(f'sudo mkdir -p {mount_point}/nginx/logs')
                except Exception as e:
                    logger.error(f"Error mounting volume to instance {instance.instance_id}, Error: {e}")
                    raise Exception(f"Error mounting volume to instance {instance.instance_id}, {e}")
        logger.info(self, f'Finish mount ebs, instance_id: {instance.instance_id}')


class TaskDeleteEbs(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        scale_in_instance_ids = self.args.get('scale_in_instance_ids', None)
        ebs_client = getattr(sdk, f'ebs_client_{cluster.cluster_type.lower()}')

        instances = []
        for ig in cluster.instance_groups:
            instances.extend(ig.instances)

        for instance in instances:
            if not scale_in_instance_ids or instance.instance_id in scale_in_instance_ids:
                if not instance.volumes:
                    continue
                for volume_id in instance.volumes:
                    ret = await ebs_client.delete_ebs(volume_id, account_id=account_id)
                    if not ret:
                        raise Exception(f'Delete ebs failed, instance {instance.instance_id}')

        return True

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        return True


class TaskUpgradeEbs(BaseTask):

    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        is_upgrade_ebs = self.args.get('is_upgrade_ebs', False)
        if not is_upgrade_ebs:
            return True

        # TaskInitUpgradeOrder透传
        order_info_res = self.args.get('order_info_res', {})

        ebs_order_infos = order_info_res.get('EBS', {})

        if not ebs_order_infos:
            raise Exception(f'Ebs order info not found, order_info_res {order_info_res}')

        upgrade_instance_group = self.args.get('upgrade_instance_group', {})

        # upgrade_volume_type = upgrade_instance_group.get('volume_type', 'LOCAL_SSD')
        upgrade_volume_size = upgrade_instance_group['volume_size']

        ebs_client = getattr(sdk, f'ebs_client_{cluster.cluster_type.lower()}')

        origin_instance_group = None
        for ig in cluster.instance_groups:
            if ig.resource_type == 'KEC' and ig.id == upgrade_instance_group.get('id', ''):
                origin_instance_group = ig

        ebslst = []
        for origin_instance in origin_instance_group.instances:
            for origin_volume_id in origin_instance.volumes:
                # VolumeStatus为in-use才可以升配，且无异常
                check_flag = await ebs_client.check_ebs_upgrade(origin_volume_id,
                                                                account_id=account_id)

                if check_flag:
                    ebslst.append(origin_volume_id)
                    res = await ebs_client.resize_ebs(
                        origin_volume_id,
                        upgrade_volume_size,
                        sub_order_id=ebs_order_infos[origin_volume_id],
                        account_id=account_id
                    )
                    
                    if not res:
                        raise Exception(f'Upgrade ebs volume failed, volume id {origin_volume_id}')

        for volume_id in ebslst:
            upgrade_flag = await ebs_client.wait_ebs_upgrade(
                volume_id, upgrade_volume_size, account_id=account_id)

            if not upgrade_flag:
                raise Exception(f'Upgrade kec instance not fully complete, volume id is {volume_id}')

        await origin_instance_group.save({
            'volume_size': upgrade_volume_size
        })

        return True

    @check_rollback
    async def rollback(self):
        # TODO rollback order
        # 如果失败，很难回滚
        return True
