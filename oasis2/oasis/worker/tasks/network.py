from datetime import datetime
from conf.infra_conf import DEFAULT_LINK, TAG_REP

from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.eip import EIPModel
from oasis.utils import sdk
from oasis.utils.config import config
from oasis.utils.logger import logger
from oasis.utils.sdk import neutron_client
from oasis.utils.sdk.base import create_eip
from oasis.utils.sdk.charging.base import get_all_suborders_format
from oasis.utils.sdk.platform.tag import TagResource
from oasis.worker.tasks import BaseTask
from oasis.worker.tasks import check_rollback
from oasis.worker.tasks import check_task


class TaskCreateSecurityGroup(BaseTask):
    @check_task
    async def run(self):
        product = self.args.get('product', '')
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        security_group_id = self.args.get('security_group_id', None)
        if not security_group_id:
            vpc_domain_id = cluster.vpc_domain_id
            sec_group_name = f'{product.upper()}-{datetime.now().strftime("%Y%m%d")}'
            sec_group_description = f'{cluster.cluster_type}-{cluster.id}'
            security_group = await neutron_client.get_or_create_sec_group(sec_group_name,
                                                                          vpc_domain_id,
                                                                          sec_group_description,
                                                                          cluster.cluster_type,
                                                                          tenant_id=self.args.get('tenant_id', None)
                                                                          )
            security_group_id = security_group.get('id', None)

        await cluster.save({'security_group_id': security_group_id})
        self.context.setdefault('security_group_id', security_group_id)
        return {'security_group_id': security_group_id}

    @check_rollback
    async def rollback(self):
        # 不处理安全组
        return True


# 历史集群没有安全组ID，为了适应历史集群，增加此任务
class TaskGetSecurityGroup(BaseTask):
    @check_task
    async def run(self):
        product = self.args.get('product', '')
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        security_group_id = self.args.get('security_group_id', None)
        if not security_group_id:
            vpc_domain_id = cluster.vpc_domain_id
            cluster_type = cluster.cluster_type
            # 获取旧的安全组ID，名称为KSCKES、KSCKMR..
            sec_group_name = f'KSC{cluster_type}'
            sec_group_description = f'{cluster_type}-{cluster.id}'
            security_group = await neutron_client.get_or_create_sec_group(sec_group_name,
                                                                          vpc_domain_id,
                                                                          sec_group_description,
                                                                          cluster_type,
                                                                          tenant_id=self.args.get('tenant_id', None)
                                                                          )
            security_group_id = security_group.get('id', None)

        await cluster.save({'security_group_id': security_group_id})
        return {'security_group_id': security_group_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskCreateControlSecurityGroup(BaseTask):
    @check_task
    async def run(self):
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        vpc_domain_id = cluster.vpc_domain_id
        control_security_group = await neutron_client.get_or_create_sec_group(
            'KMRCONTROL',
            vpc_domain_id,
            f'{cluster.cluster_type} Control SG',
            cluster.cluster_type,
            tenant_id=self.args.get('tenant_id',
                                    None)
        )
        control_security_group_id = control_security_group.get('id', None)

        return {'control_security_group_id': control_security_group_id}

    @check_rollback
    async def rollback(self):
        return True


class TaskCreateSubnet(BaseTask):
    @check_task
    async def run(self):
        product = self.args.get('product', '')
        account_id = self.args.get('account_id', '')
        tenant_id = self.args.get('tenant_id', '')
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        sec_group_id = self.args.get('security_group_id', None)

        cluster_type = cluster.cluster_type
        subnet_ids = []
        if cluster.vpc_subnet_id:
            subnet_ids.append(cluster.vpc_subnet_id)
        for ig in cluster.instance_groups:
            subnet_id = ig.vpc_subnet_id
            if subnet_id and subnet_id not in subnet_ids:
                subnet_ids.append(subnet_id)

        vpc_client = getattr(sdk, f'vpc_client_{product}')
        subnets = await vpc_client.describe_subnets(
            [cluster.vpc_domain_id],
            subnet_ids,
            cluster.availability_zone,
            account_id=account_id,
        )
        logger.info(self, f'==create_cluster, subnets: {subnets}')

        if len(subnets) != len(subnet_ids):
            raise Exception(f'Subnets do not exist, expect ids: {subnet_ids}, got {subnets}.')

        expect_rules = []
        for subnet in subnets:
            cidr_block = subnet.get('CidrBlock')
            subnet_ip, subnet_mask = cidr_block.split('/')
            if cluster_type == 'KES':
                expect_rules.append(('in', subnet_ip, int(subnet_mask), 'tcp', 9200, 9200))
                expect_rules.append(('in', subnet_ip, int(subnet_mask), 'tcp', 9300, 9300))
            elif cluster_type == 'KHBASE':
                expect_rules.append(('in', subnet_ip, int(subnet_mask), 'tcp', 1, 65535))

        sg = await neutron_client.get_security_group_by_id(sec_group_id,
                                                           tenant_id=tenant_id,
                                                           add_kmr_tag=False)
        sg_rules = sg.get('rules', [])
        logger.info(self, f'==sec_group_rules: {sg_rules}')

        for sg_rule in sg_rules:
            rule = (sg_rule.get('direction', None), sg_rule.get('ip', None),
                    sg_rule.get('mask', None), sg_rule.get('protocol', None),
                    sg_rule.get('port_start', None), sg_rule.get('port_end', None))
            if rule in expect_rules:
                expect_rules.remove(rule)

        if not expect_rules:
            logger.info(self, f'All security group rules satisfied, security group id: {sec_group_id}.')
            return True

        add_rules = [{
            'direction': expect_rule[0],
            'ip': expect_rule[1],
            'mask': expect_rule[2],
            'protocol': expect_rule[3],
            'port_start': expect_rule[4],
            'port_end': expect_rule[5],
        } for expect_rule in expect_rules]

        add_rule_args = {
            'vpc_securitygroup': {
                'rules': {
                    'create': add_rules,
                }
            }
        }

        logger.info(self, f'Add rules to security group {sec_group_id}, rules: {add_rule_args}')
        await neutron_client.add_rules_to_sec_group(sec_group_id, add_rule_args, tenant_id=tenant_id)

    @check_rollback
    async def rollback(self):
        return True


class TaskCreateInnerLB(BaseTask):
    # ELB for ssh to 1505
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        tenant_id = self.args.get('tenant_id', '')
        instance_ids = self.args.get('new_instance_ids', None)
        cluster_id = self.args.get('cluster_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        kec_client = getattr(sdk, f'kec_client_{cluster.cluster_type.lower()}')
        epc_client = getattr(sdk, f'epc_client_{cluster.cluster_type.lower()}')

        for instance_group in cluster.instance_groups:
            instance_type = instance_group.resource_type
            for instance in instance_group.instances:
                if instance_ids:
                    if instance.instance_id not in instance_ids:
                        continue
                internal_ip = ''
                if instance_type == 'KEC':
                    res = await kec_client.describe_instances(instance_ids=[instance.instance_id],
                                                              account_id=account_id)
                    if not res:
                        raise Exception(f'Could not find instance KEC {instance.instance_id}')
                    internal_ip = res[0].get('PrivateIpAddress', '')
                elif instance_type == 'EPC':
                    res = await epc_client.describe_instances(instance_ids=[instance.instance_id],
                                                              account_id=account_id)
                    if not res:
                        raise Exception(f'Could not find instance EPC {instance.instance_id}')
                    internal_ip = res[0].get('PrivateIpAddress', '')

                lb_eip = await neutron_client.create_inner_lbs(cluster, instance,
                                                               internal_ip, tenant_id=tenant_id)
                logger.info(self, f'Provision_vpc instance {instance.instance_id} '
                                  f'internal_ip {internal_ip}, lb_eip {lb_eip}')
                await instance.save({
                    'internal_ip': internal_ip,
                    'inner_eip': lb_eip,
                })

    @check_rollback
    async def rollback(self):
        cluster_id = self.context.get('cluster_id', None)
        tenant_id = self.context.get('tenant_id', None)
        new_instance_ids = self.context.get('new_instance_ids', [])

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        for instance_group in cluster.instance_groups:
            for instance in instance_group.instances:
                if instance.instance_id not in new_instance_ids:
                    continue
                await neutron_client.delete_inner_lbs(instance, tenant_id=tenant_id)
        return True


class TaskCreateEIP(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.get('cluster_id', None)

        eip_line_id = self.args.get('eip_line_id', None)
        eip_charge_type = self.args.get('eip_charge_type', None)
        sub_orders = self.args.get('sub_orders', {})
        eip_orders = sub_orders.get('cluster', {}).get('eip', [])
        eip_order_id = eip_orders[0] if eip_orders else None

        ip_addr = self.args.get('ip_addr', None)
        allocation_id = self.args.get('allocation_id', None)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        old_eip_query = model_query(EIPModel).filter(
            EIPModel.cluster_id == cluster_id,
            EIPModel.load_balancer_type == 0,
            EIPModel.status != EIPModel.STATUS.DELETED)
        old_eip = await old_eip_query.query_one()

        if old_eip:
            raise Exception(f'Cannot create eip, cluster {cluster_id} already have eip {old_eip.to_dict()}')

        # create eip and bind eip
        if eip_line_id and eip_charge_type:
            logger.info(self, f'==create eip ==> eip_line_id: {eip_line_id}, eip_charge_type: {eip_charge_type}')

            project_id = self.args.get('project_id', None)
            eip_band_width = self.args.get('eip_band_width', '1')
            eip_purchase_time = self.args.get('eip_purchase_time', None)
            # purchase_time_unit = self.args.get('purchase_time_unit', None)

            if not eip_order_id:
                logger.error(self, f'==create eip ==> eip_order_id is {eip_order_id}')
                return

            ip_addr, allocation_id = await create_eip(
                cluster,
                eip_order_id,
                eip_line_id,
                eip_charge_type,
                project_id,
                eip_purchase_time,
                account_id=account_id,
                band_width=eip_band_width)

            logger.info(self, f'==create eip succeed, ip_addr: {ip_addr}, allocate_address_id: {allocation_id}')

        # now we should have ip_addr and allocation_id
        if ip_addr and allocation_id:
            eip_client = getattr(sdk, f'eip_client_{cluster.cluster_type.lower()}')
            ip_addr_ret = await eip_client.describe_address(allocation_id=allocation_id,
                                                            account_id=account_id)
            if not ip_addr_ret:
                raise Exception(self, f'Eip allocation_id {allocation_id} does not exist.')
            ip_addr = ip_addr_ret.get('PublicIp', '')
            logger.info(self, f'==bind eip==>ip: {ip_addr}, allocation_id: {allocation_id}')
            db_values = {
                'cluster_id': cluster_id,
                'allocate_address_id': allocation_id,
                'eip_address': ip_addr,
                'status': EIPModel.STATUS.UNBINDED,
                'eip_line_id': eip_line_id,
                'eip_charge_type': eip_charge_type,
                'eip_order_id': eip_order_id,
            }
            eip_model = EIPModel()
            eip_model.update(db_values)
            await eip_model.save()

        return {
            'allocation_id': allocation_id,
        }

    @check_rollback
    async def rollback(self):
        account_id = self.context.get('account_id', None)
        cluster_id = self.context.get('cluster_id', None)
        eip_line_id = self.args.get('eip_line_id', None)
        eip_charge_type = self.args.get('eip_charge_type', None)

        allocation_id = self.results.get('allocation_id', None)

        if not allocation_id:
            # 没有绑定EIP或EIP创建失败
            return True

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        # 如果是这里创建的，则回滚时释放EIP，否则不作处理
        if eip_line_id and eip_charge_type:
            eip_client = getattr(sdk, f'eip_client_{cluster.cluster_type.lower()}')

            eip_query = model_query(EIPModel).filter(
                EIPModel.allocate_address_id == allocation_id,
                EIPModel.load_balancer_type == 0,
                EIPModel.cluster_id == cluster_id,
                EIPModel.status == EIPModel.STATUS.UNBINDED)
            eip_info = await eip_query.query_one()

            if eip_info:
                await eip_client.release_address_eip(eip_info.allocate_address_id,
                                                     account_id=account_id)

        return True


class TaskCreateSLB(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.get('cluster_id', None)

        load_balancer_type = self.args.get('load_balancer_type', 'public')
        subnet_id = self.args.get('subnet_id', None)
        private_ip_address = self.args.get('private_ip_address', None)

        sub_orders = self.args.get('sub_orders', {})
        slb_orders = sub_orders.get('cluster', {}).get('slb', [])
        slb_order_id = slb_orders[0] if slb_orders else None

        if not slb_order_id:
            logger.info(f'==> skip register_instances_to_lb, got slb_order_id: {slb_order_id}')
            return

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        slb_client = getattr(sdk, f'slb_client_{cluster.cluster_type.lower()}')

        slb_id = await slb_client.create_load_balancer(slb_order_id,
                                                       cluster.vpc_domain_id,
                                                       load_balancer_type=load_balancer_type,
                                                       slb_name=f'kes_slb_{cluster.name[:20]}',
                                                       subnet_id=subnet_id,
                                                       private_ip_address=private_ip_address,
                                                       account_id=account_id)

        return {'slb_id': slb_id}

    @check_rollback
    async def rollback(self):
        # Delete SLB
        account_id = self.context.get('account_id', '')
        cluster_id = self.context.get('cluster_id', None)
        slb_id = self.results.get('slb_id', None)
        if not slb_id:
            return

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        slb_client = getattr(sdk, f'slb_client_{cluster.cluster_type.lower()}')

        listener_ids = await slb_client.describe_listeners(
            slb_id, account_id=account_id
        )
        for listener_id in listener_ids:
            await slb_client.delete_listeners(
                listener_id, account_id=account_id
            )
        await slb_client.delete_load_balancer(
            slb_id, account_id=account_id)


class TaskAllocateEIP2SLB(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.get('cluster_id', None)
        slb_id = self.args.get('slb_id', None)
        allocation_id = self.args.get('allocation_id', None)

        if not slb_id or not allocation_id:
            logger.info(f'==>skip allocate eip to slb, slb_id={slb_id}, eip_id={allocation_id}')
            return

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        eip_client = getattr(sdk, f'eip_client_{cluster.cluster_type.lower()}')
        slb_client = getattr(sdk, f'slb_client_{cluster.cluster_type.lower()}')

        if not await eip_client.associate_slb_eip(slb_id, allocation_id, account_id=account_id):
            raise Exception(f'==>allocate eip to slb failed, slb_id={slb_id}, '
                            f'allocation_id={allocation_id}')
        if not await slb_client.modify_load_balancer(slb_id, 'start', account_id=account_id):
            raise Exception(f'==>allocate eip to slb failed, start slb failed, slb_id={slb_id}')

        listener_ids = {}
        health_check_ids = {}
        register_ids = {}
        public_service_ports = DEFAULT_LINK.get(cluster.cluster_type.lower()).get('public', {}).values()

        for service_port in public_service_ports:
            listener_id = await slb_client.create_listeners(slb_id, service_port, account_id=account_id)
            if not listener_id:
                raise Exception(
                    f'==>allocate eip to slb failed, [slb_id={slb_id}, nginx_listen_ports={service_port}]')

            listener_ids[service_port] = listener_id

            health_check_id = await slb_client.configure_health_check(listener_id,
                                                                      account_id=account_id)
            if not health_check_id:
                raise Exception(f'==>allocate eip to slb failed, [listener_id={listener_id}]')
            health_check_ids[service_port] = health_check_id

            register_instance_ids = []

            for ig in cluster.instance_groups:
                for instance in ig.instances:
                    register_id = await slb_client.register_instances_with_listener(listener_id,
                                                                                    instance.internal_ip,
                                                                                    service_port,
                                                                                    account_id=account_id)
                    if not register_id:
                        raise Exception(f'==>allocate eip to slb failed, [instance={instance.instance_id}] '
                                        f'not get register to [listener_id={listener_id}] '
                                        f'on [port={service_port}]')
                    register_instance_ids.append(register_id)

            register_ids[service_port] = register_instance_ids
            # rs会绑定到多个slb，不在使用instance.slb_register_id存储rs关系。
            # await instance.save({'slb_register_id': register_ids})

        eip_query = model_query(EIPModel).filter(
            EIPModel.allocate_address_id == allocation_id,
            EIPModel.load_balancer_type == 0,
            EIPModel.cluster_id == cluster_id,
            EIPModel.status == EIPModel.STATUS.UNBINDED)
        eip_info = await eip_query.query_one()

        await eip_info.save({
            'listener_id': listener_ids,
            'health_check_id': health_check_ids,
            'load_balancer_id': slb_id,
            'register_id': register_ids,
            'status': EIPModel.STATUS.BINDED,
        })

        await cluster.save({'enable_eip': True})
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

        instances = []
        for ig in cluster.instance_groups:
            instances.extend(ig.instances)

        eip_info_query = model_query(EIPModel).filter(
            EIPModel.cluster_id == cluster_id,
            EIPModel.load_balancer_type == 0,
            EIPModel.status != EIPModel.STATUS.DELETED
        )
        eip_infos = await eip_info_query.query_all()
        if not eip_infos:
            logger.info(self, f'Can not find any eip info of cluster {cluster_id}')
            return

        eip_client = getattr(sdk, f'eip_client_{cluster.cluster_type.lower()}')

        slb_ids = []
        for eip_info in eip_infos:
            if eip_info.status == EIPModel.STATUS.BINDED:
                unbind = await eip_client.disassociate_address_eip(eip_info.allocate_address_id,
                                                                   account_id=account_id)
                if not unbind:
                    logger.error(self, f'Unbind EIP failed, eip {eip_info.id}')
                    raise Exception(f'Unbind EIP failed, eip {eip_info.id}')

                slb_ids.append(eip_info.load_balancer_id)

            await eip_info.save({'status': EIPModel.STATUS.UNBINDED})

        await cluster.save({'enable_eip': False})

        return {'slb_ids': slb_ids}


class TaskScaleInReleaseSlb(BaseTask):

    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.get('cluster_id', None)
        scale_in_instance_ids = self.args.get('scale_in_instance_ids', None)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        # 0:公网 1：私网 -1：全部（释放）
        unbind_slb_type = self.args.get('unbind_slb_type', None)
        if unbind_slb_type is None:
            raise Exception('Please specify unbind_slb_type')

        public_service_ports = DEFAULT_LINK.get(cluster.cluster_type.lower()).get('public', {}).values()
        private_service_ports = DEFAULT_LINK.get(cluster.cluster_type.lower()).get('private', {}).values()

        eip_info_query = model_query(EIPModel).filter(
            EIPModel.cluster_id == cluster_id,
            EIPModel.status != EIPModel.STATUS.DELETED
        )

        if unbind_slb_type != -1:
            eip_info_query = eip_info_query.filter(EIPModel.load_balancer_type == int(unbind_slb_type))

        eip_infos = await eip_info_query.query_all()
        if not eip_infos:
            logger.info(self, f'Can not find any private slb info of cluster {cluster_id}')
            return

        slb_client = getattr(sdk, f'slb_client_{cluster.cluster_type.lower()}')

        for eipinfo in eip_infos:
            register_ids = eipinfo.register_id or {}
            temp_ports = public_service_ports if eipinfo.load_balancer_type == 0 else private_service_ports

            if not eipinfo.listener_id:
                raise Exception(f'==>scale slb failed, [eipinfo_id={eipinfo.id}]')

            for service_port in temp_ports:
                listener_id = eipinfo.listener_id.get(service_port, '')
                if not listener_id:
                    # 兼容旧集群
                    logger.info(f'==>scale slb failed, [service_port={service_port}]')
                    continue

                register_instance_ids = register_ids.get(service_port, [])

                for ig in cluster.instance_groups:
                    for instance in ig.instances:
                        if instance.instance_id not in scale_in_instance_ids:
                            continue

                        slb_id = instance.instance_id
                        real_listeners = await slb_client.describe_listeners_all(slb_id,
                                                                                 account_id=account_id)
                        for real_listener in real_listeners:
                            for rss in real_listener.get('RealServer', []):
                                register_id = rss.get('RegisterId')
                                await slb_client.deregister_instances_from_listener(register_id,
                                                                                    account_id=account_id)

                                if register_id in register_instance_ids:
                                    register_instance_ids.remove(register_id)

                register_ids[service_port] = register_instance_ids

            await eipinfo.save({'register_id': register_ids})
            await eipinfo.save({'status': EIPModel.STATUS.DELETED})

        return True

    @check_rollback
    async def rollback(self):
        return True


class TaskScaleOutBindSlb(BaseTask):

    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.get('cluster_id', None)
        new_instance_ids = self.args.get('new_instance_ids', None)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        slb_client = getattr(sdk, f'slb_client_{cluster.cluster_type.lower()}')

        public_service_ports = DEFAULT_LINK.get(cluster.cluster_type.lower()).get('public', {}).values()
        private_service_ports = DEFAULT_LINK.get(cluster.cluster_type.lower()).get('private', {}).values()

        eip_info_query = model_query(EIPModel).filter(
            EIPModel.cluster_id == cluster_id,
            EIPModel.status == EIPModel.STATUS.BINDED
        )
        eip_infos = await eip_info_query.query_all()

        for eipinfo in eip_infos:

            register_ids = eipinfo.register_id or {}
            temp_ports = public_service_ports if eipinfo.load_balancer_type == 0 else private_service_ports

            if not eipinfo.listener_id:
                raise Exception(f'==>scale slb failed, [eipinfo_id={eipinfo.id}]')
            for service_port in temp_ports:

                listener_id = eipinfo.listener_id.get(service_port, '')
                if not listener_id:
                    # 兼容旧集群
                    logger.info(f'==>scale slb failed, [service_port={service_port}]')
                    continue

                register_instance_ids = register_ids.get(service_port, [])

                for ig in cluster.instance_groups:
                    for instance in ig.instances:
                        if instance.instance_id not in new_instance_ids:
                            continue

                        register_id = await slb_client.register_instances_with_listener(listener_id,
                                                                                        instance.internal_ip,
                                                                                        service_port,
                                                                                        account_id=account_id)
                        if not register_id:
                            raise Exception(f'==>allocate eip to slb failed, [instance={instance.instance_id}] '
                                            f'not get register to [listener_id={listener_id}] '
                                            f'on [port={service_port}]')

                        register_instance_ids.append(register_id)

                    register_ids[service_port] = register_instance_ids
                    # rs会绑定到多个slb，不在使用instance.slb_register_id存储rs关系。
                    # await instance.save({'slb_register_id': register_ids})

            await eipinfo.save({'register_id': register_ids})

        return True

    @check_rollback
    async def rollback(self):
        return True


class TaskBindPrivateSLB(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', None)
        cluster_id = self.args.get('cluster_id', None)
        private_ip_address = self.args.get('private_ip_address', None)
        slb_id = self.args.get('slb_id', None)

        if not slb_id:
            logger.info(f'==>skip allocate eip to slb, slb_id={slb_id}')
            return

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        slb_client = getattr(sdk, f'slb_client_{cluster.cluster_type.lower()}')

        if not await slb_client.modify_load_balancer(slb_id, 'start', account_id=account_id):
            raise Exception(f'==>allocate eip to slb failed, start slb failed, slb_id={slb_id}')

        listener_ids = {}
        health_check_ids = {}
        register_ids = {}
        private_service_ports = DEFAULT_LINK.get(cluster.cluster_type.lower()).get('private', {}).values()

        for service_port in private_service_ports:
            listener_id = await slb_client.create_listeners(slb_id, service_port, account_id=account_id)
            if not listener_id:
                raise Exception(
                    f'==>allocate eip to slb failed, [slb_id={slb_id}, nginx_listen_ports={service_port}]')

            listener_ids[service_port] = listener_id

            health_check_id = await slb_client.configure_health_check(listener_id,
                                                                      account_id=account_id)
            if not health_check_id:
                raise Exception(f'==>allocate eip to slb failed, [listener_id={listener_id}]')
            health_check_ids[service_port] = health_check_id

            register_instance_ids = []

            for ig in cluster.instance_groups:
                for instance in ig.instances:
                    register_id = await slb_client.register_instances_with_listener(listener_id,
                                                                                    instance.internal_ip,
                                                                                    service_port,
                                                                                    account_id=account_id)
                    if not register_id:
                        raise Exception(f'==>allocate eip to slb failed, [instance={instance.instance_id}] '
                                        f'not get register to [listener_id={listener_id}] '
                                        f'on [port={service_port}]')

                    register_instance_ids.append(register_id)

                register_ids[service_port] = register_instance_ids
                # rs会绑定到多个slb，不在使用instance.slb_register_id存储rs关系。
                # await instance.save({'slb_register_id': register_ids})

        db_values = {
            'cluster_id': cluster_id,
            'load_balancer_type': 1,
            'eip_address': private_ip_address,
            'listener_id': listener_ids,
            'health_check_id': health_check_ids,
            'load_balancer_id': slb_id,
            'register_id': register_ids,
            'status': EIPModel.STATUS.BINDED,
        }
        eip_model = EIPModel()
        eip_model.update(db_values)
        await eip_model.save()

        await cluster.save({'enable_private_slb': True})
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

        instances = []
        for ig in cluster.instance_groups:
            instances.extend(ig.instances)

        eip_info_query = model_query(EIPModel).filter(
            EIPModel.cluster_id == cluster_id,
            EIPModel.status != EIPModel.STATUS.DELETED,
            EIPModel.load_balancer_type == 1
        )
        eip_infos = await eip_info_query.query_all()
        if not eip_infos:
            logger.info(self, f'Can not find any eip info of cluster {cluster_id}')
            return

        slb_ids = []
        for eip_info in eip_infos:
            await eip_info.save({'status': EIPModel.STATUS.DELETED})

        await cluster.save({'enable_private_slb': False})

        return {'slb_ids': slb_ids}


class TaskDisassociateEIP(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        is_delete = self.args.get('is_delete', 0)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        instances = []
        for ig in cluster.instance_groups:
            instances.extend(ig.instances)

        eip_info_query = model_query(EIPModel).filter(
            EIPModel.cluster_id == cluster_id,
            EIPModel.load_balancer_type == 0,
            EIPModel.status != EIPModel.STATUS.DELETED
        )
        eip_infos = await eip_info_query.query_all()
        if not eip_infos:
            logger.info(self, f'Can not find any eip info of cluster {cluster_id}')
            return

        eip_client = getattr(sdk, f'eip_client_{cluster.cluster_type.lower()}')
        tag_client = getattr(sdk, f'tag_client_{cluster.cluster_type.lower()}')

        # 伪系统标签'集群ID'
        default_cluster_tag_id = ''
        for tags in list(cluster.tags):
            if tags and isinstance(tags, dict) and tags.get('tag_key', '') == TagResource.SYS_CLUSTER_TAG.CLUSTER_ID:
                default_cluster_tag_id = tags.get('tag_id', '')

        slb_ids = []
        listener_ids = []

        for eip_info in eip_infos:
            if eip_info.status == EIPModel.STATUS.BINDED:
                real_eip_info = await eip_client.describe_address(eip_info.allocate_address_id, account_id=account_id)
                real_state = real_eip_info.get('State', 'unknown')
                # real_instance_type = real_eip_info.get('InstanceType', 'unknown')
                # real_instance_id = real_eip_info.get('InstanceId', 'unknown')

                # if real_state != 'associate':
                #     raise Exception(f'Unbind EIP failed, EIP {eip_info.id} status not [associate].')

                # if real_instance_type != 'Slb' or real_instance_id != eip_info.load_balancer_id:
                #     raise Exception(f'Unbind EIP failed, EIP {eip_info.id} binding info was wrong, '
                #                     f'please contact administrator.')

                if real_eip_info:
                    if real_state == 'associate':
                        unbind = await eip_client.disassociate_address_eip(eip_info.allocate_address_id,
                                                                           account_id=account_id)
                        if not unbind:
                            raise Exception(f'Unbind EIP failed, eip {eip_info.id}')

                    if int(is_delete) == 1:
                        release = await eip_client.release_address_eip(eip_info.allocate_address_id,
                                                                       account_id=account_id)
                        if not release:
                            raise Exception(f'Release EIP failed, eip {eip_info.id}')

                    # slb_ids.append(eip_info.load_balancer_id)

                    # for listener_id in eip_info.listener_id.values():
                    #     listener_ids.append(listener_id)

                # region 确保解绑时，同时解绑资源tag
                try:
                    await tag_client.detach_resource_tags(default_cluster_tag_id, TAG_REP.get('EIP', ''), eip_info.allocate_address_id, account_id=account_id)
                except Exception as e:
                    logger.warn(self, f'Detach EIP Tag Error ,Error:{e} EIP:{eip_info.allocate_address_id}')
                # endregion

            await eip_info.save({'status': EIPModel.STATUS.UNBINDED})

        await cluster.save({'enable_eip': False})
        # 兼容slb 改造后，这个返回没有意义了。
        # 感觉需求后续还会发生变化，这里先留作纪念吧。
        return {'slb_ids': slb_ids, 'listener_ids': listener_ids}

    @check_rollback
    async def rollback(self):
        return True


class TaskDeleteSLB(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        # slb_ids = self.args.get('slb_ids', [])
        # listener_ids = self.args.get('listener_ids', [])
        # 为了兼容私网slb改造，slb/listener不再接受外传。且使用unbind_type，作为筛选条件
        slb_ids = []
        listener_ids = []
        # 0:公网 1：私网 -1：全部（释放
        unbind_slb_type = self.args.get('unbind_slb_type', None)
        is_delete = self.args.get('is_delete', 0)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if unbind_slb_type is None:
            raise Exception('Please specify unbind_slb_type')

        eip_info_query = model_query(EIPModel).filter(
            EIPModel.cluster_id == cluster_id,
            EIPModel.status != EIPModel.STATUS.DELETED
        )

        if unbind_slb_type != -1:
            eip_info_query = eip_info_query.filter(EIPModel.load_balancer_type == int(unbind_slb_type))

        eip_infos = await eip_info_query.query_all()
        if not eip_infos:
            logger.info(self, f'Can not find any private slb info of cluster {cluster_id}')
            return

        for eip_info in eip_infos:
            slb_ids.append(eip_info.load_balancer_id)
            listener_ids += eip_info.listener_id.values() if eip_info.listener_id else []

        slb_client = getattr(sdk, f'slb_client_{cluster.cluster_type.lower()}')
        tag_client = getattr(sdk, f'tag_client_{cluster.cluster_type.lower()}')

        # 伪系统标签'集群ID'
        default_cluster_tag_id = ''
        for tags in list(cluster.tags):
            if tags and isinstance(tags, dict) and tags.get('tag_key', '') == TagResource.SYS_CLUSTER_TAG.CLUSTER_ID:
                default_cluster_tag_id = tags.get('tag_id', '')

        for slb_id in slb_ids:
            real_listeners = await slb_client.describe_listeners_all(
                slb_id, account_id=account_id
            )
            for real_listener in real_listeners:
                real_listener_id = real_listener.get('ListenerId', '')
                # JIRA问题 @PMUED-7361
                # if real_listener_id not in listener_ids:
                #     raise Exception(f'listener id {real_listener_id} of slb id {slb_id} not in {listener_ids}, '
                #                     f'please contact administrator...')
                # slb 兼容后，关闭时，二次提示后强制处理。
                logger.info(f'listener id {real_listener_id} of slb id {slb_id} not in {listener_ids}, '
                            f'please contact administrator...')
                for rss in real_listener.get('RealServer', []):
                    await slb_client.deregister_instances_from_listener(rss.get('RegisterId'), account_id=account_id)

                if int(is_delete) == 1:
                    await slb_client.delete_listeners(real_listener_id, account_id=account_id)

            if int(is_delete) == 1:
                await slb_client.delete_load_balancer(slb_id, account_id=account_id)

            # region 确保解绑时，同时解绑资源tag
            try:
                await tag_client.detach_resource_tags(default_cluster_tag_id, TAG_REP.get('SLB', ''), slb_id, account_id=account_id)
            except Exception as e:
                logger.warn(self, f'Detach SLB Tag Error ,Error:{e} SLB:{slb_id}')
            # endregion

        for eip_info in eip_infos:
            await eip_info.save({'status': EIPModel.STATUS.DELETED})

        await cluster.save({'enable_private_eip': False})

    @check_rollback
    async def rollback(self):
        return True


class TaskProvisionControlSecurityGroup(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        tenant_id = self.args.get('tenant_id', '')
        cluster_id = self.args.get('cluster_id', None)
        instance_ids = self.args.get('new_instance_ids', [])
        control_security_group_id = self.args.get('control_security_group_id', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        instances = []
        for ig in cluster.instance_groups:
            for instance in ig.instances:
                if instance.instance_id in instance_ids:
                    instances.append(instance)

        if not control_security_group_id:
            logger.warning(self, f'Get kmr control security group failed. cluster id: {cluster_id}')
        else:
            await neutron_client.bind_kmr_control_sg(instances, control_security_group_id, cluster.cluster_type,
                                                     account_id=account_id, tenant_id=tenant_id)

    @check_rollback
    async def rollback(self):
        return True


class TaskBindInternalEIP(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)
        new_instance_ids = self.args.get('new_instance_ids', None)
        inner_eip_order_id = self.args.get('inner_eip_order_id', None)
        project_id = self.args.get('project_id', None)
        line_id = self.args.get('line_id', None)
        charge_type = self.args.get('charge_type', None)
        band_width = self.args.get('band_width', None)
        purchase_time = self.args.get('purchase_time', None)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        if not inner_eip_order_id:
            logger.info(self, f'No inner_eip_order_id, skip...')
            return True

        sub_orders = await get_all_suborders_format(inner_eip_order_id)
        eip_sub_orders = sub_orders.get('cluster', {}).get('eip', [])

        if not eip_sub_orders:
            logger.info(self, f'No eip_sub_orders, skip...')
            return True

        extra_dict = dict(cluster.extra) if cluster.extra else dict()
        extra_dict['inner_eip_order_id'] = inner_eip_order_id
        extra_dict['bind_eip_status'] = 'Binding'
        await cluster.save({'extra': extra_dict})

        eip_client = getattr(sdk, f'eip_client_{cluster.cluster_type.lower()}')
        vpc_client = getattr(sdk, f'vpc_client_{cluster.cluster_type.lower()}')

        instances = []
        for ig in cluster.instance_groups:
            # scale out
            if new_instance_ids:
                add_instances = [ins for ins in ig.instances
                                 if ins.instance_id in new_instance_ids]
            # first time bind ieip
            else:
                add_instances = ig.instances

            instances.extend(add_instances)

        if len(eip_sub_orders) != len(instances):
            raise Exception(f'Instance num: {len(instances)} does not match eip_sub_order: {len(eip_sub_orders)}')

        for idx, instance in enumerate(instances):
            instance_id = instance.instance_id
            eip_sub_order = eip_sub_orders[idx]
            interface_id = await vpc_client.get_network_interface_id(
                instance_id, account_id=account_id)
            logger.info(self, f'==bind internal eip start, instance_id: {instance_id}, '
                              f'network_interface_id: {interface_id}, '
                              f'eip sub order: {eip_sub_order}')
            allocation_id, management_ip = await eip_client.get_allocate_address_id(
                charge_type, eip_sub_order, line_id, project_id,
                purchase_time, band_width=band_width,
                account_id=account_id
            )
            await eip_client.associate_ipfwd_eip(instance_id, interface_id,
                                                 allocation_id, account_id=account_id)
            logger.info(self, f'==bind internal eip finished, instance_id: {instance_id}, '
                              f'network_interface_id: {interface_id}, '
                              f'eip sub order: {eip_sub_order}, '
                              f'allocation_id: {allocation_id}, '
                              f'management_ip: {management_ip}')
            await instance.save({'inner_manager_ip': management_ip,
                                 'allocate_address_id': allocation_id})

        extra_dict['bind_eip_status'] = 'Complete'
        await cluster.save({'extra': extra_dict})

    @check_rollback
    async def rollback(self):
        # TODO accomplish this
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        extra_dict = dict(cluster.extra) if cluster.extra else dict()
        extra_dict.pop('inner_eip_order_id', None)
        extra_dict.pop('bind_eip_status', None)
        await cluster.save({'extra': extra_dict})

        extra_dict['bind_eip_status'] = 'Complete'
        await cluster.save({'extra': extra_dict})


class TaskReleaseInternalEIP(BaseTask):
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

        extra_dict = dict(cluster.extra) if cluster.extra else dict()
        rule = extra_dict.get('bind_eip_status', None)

        if rule != 'Complete':
            return True

        eip_client = getattr(sdk, f'eip_client_{cluster.cluster_type.lower()}')

        instances = []
        for ig in cluster.instance_groups:
            instances.extend(ig.instances)

        # 第一步：解绑弹性IP，将指定弹性IP与实例解绑
        for instance in instances:
            if not scale_in_instance_ids or instance.instance_id in scale_in_instance_ids:
                allocate_address_id = instance.allocate_address_id
                logger.info(self, f'=======bind_eip_instances_allocate_address_id: {allocate_address_id}')
                if allocate_address_id is not None:
                    is_disassociate = await eip_client.disassociate_address_eip(allocate_address_id,
                                                                                account_id=account_id)
                    if is_disassociate:
                        logger.debug(self, f'cluster {cluster.id} unbind eip of instance '
                                     f'{instance.instance_id}: unbind eip success')
                        await instance.save({'inner_manager_ip': None})

                    else:
                        raise Exception(f'cluster {cluster.id} unbind eip of instance '
                                        f'{instance.instance_id}: unbind eip failed')
        # 第二步：删除弹性IP，删除指定弹性IP
        for instance in instances:
            if not scale_in_instance_ids or instance.instance_id in scale_in_instance_ids:
                allocate_address_id = instance.allocate_address_id
                logger.info(self, f'=======create_eip_instances_allocate_address_id: {allocate_address_id}')

                if await eip_client.release_address_eip(allocate_address_id, account_id=account_id):
                    logger.debug(self, f'cluster {cluster.id} delete eip of instance '
                                 f'{instance.instance_id}: delete eip success')
                    await instance.save({'allocate_address_id': None})
                else:
                    raise Exception(f'cluster {cluster.id} delete eip of instance '
                                    f'{instance.instance_id}: delete eip failed')

        return True

    @check_rollback
    async def rollback(self):
        return True


class TaskDeleteInnerLB(BaseTask):
    @check_task
    async def run(self):
        tenant_id = self.args.get('tenant_id', '')
        cluster_id = self.args.get('cluster_id', None)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        scale_in_instance_ids = self.args.get('scale_in_instance_ids', None)

        instances = []
        for ig in cluster.instance_groups:
            instances.extend(ig.instances)

        for instance in instances:
            if not scale_in_instance_ids or instance.instance_id in scale_in_instance_ids:
                await neutron_client.delete_inner_lbs(instance, tenant_id=tenant_id)

    @check_rollback
    async def rollback(self):
        return True
