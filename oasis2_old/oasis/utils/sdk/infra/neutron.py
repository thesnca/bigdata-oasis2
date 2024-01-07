from conf import infra_conf
from conf.infra_conf import NEUTRON_API
from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.utils import http
from oasis.utils import sdk
from oasis.utils.config import config
from oasis.utils.exceptions import VpcNeutronException
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger


def _prepare(func):
    async def __inner(self, *args, **kwargs):
        tenant_id = kwargs.get('tenant_id', None)
        if not tenant_id:
            raise Exception(f'Please specify tenant_id, got {tenant_id}')
        token = f'kmr:{tenant_id}'
        request_id = kwargs.pop('request_id', gen_uuid4())
        add_kmr_tag = kwargs.get('add_kmr_tag', False)

        headers = {
            'X-Auth-Token': token,
            'X-Ksc-Request-Id': request_id,
            'Accept': 'application/json',
            # 'Authorization': aws_auth,
        }

        # KEEP
        if add_kmr_tag:
            headers.setdefault('X-Auth-User-Tag', 'kmr')

        return await func(self, headers=headers, *args, **kwargs)

    return __inner


class Neutron:
    def __init__(self):
        self.endpoint = config.get('infra', 'neutron_endpoint')
        self.region = config.get('infra', 'region')
        self.network_id = config.get('network_id', 'kes')

    @_prepare
    async def _delete_default_group(self, sg_id, *, tenant_id=None,
                                    params: dict = None, headers: dict = None):
        api = NEUTRON_API['delete_sg'].format(sg_id=sg_id)
        code, ret = await http.delete(f'{self.endpoint}{api}', params=params, headers=headers)

        if 199 < code < 300:
            return True
        raise Exception(f'_delete_default_group failed, return: {ret}')

    @_prepare
    async def _create_security_group(self, sg_name, vpc_domain_id, sec_group_description, *,
                                     tenant_id=None, add_kmr_tag=False,
                                     params: dict = None, headers: dict = None):
        api = NEUTRON_API['create_sg']
        # control will use 'kmr', and sg_id must be '252'
        payload = {
            'name': sg_name,
            'domain_id': vpc_domain_id,
            'type': 'other',
            'description': sec_group_description,
        }
        if add_kmr_tag:
            payload.setdefault('sg_id', 252)

        data = {
            'vpc_securitygroup': payload,
        }

        code, ret = await http.post(f'{self.endpoint}{api}', params=params, data=data, headers=headers)

        if 199 < code < 300:
            vpc_securitygroup = ret.get('vpc_securitygroup', None)
            return vpc_securitygroup
        raise Exception(f'_create_security_group failed, return: {ret}')

    @_prepare
    async def _create_sec_group_rule(self, sec_group_id, cluster_type, *,
                                     tenant_id=None, add_kmr_tag=False,
                                     params: dict = None, headers: dict = None):
        if not sec_group_id:
            logger.warn(f'_create_sec_group_rule got no sec_group_id.')
            return False
        api = NEUTRON_API['create_rules'].format(sg_id=sec_group_id)
        data = {
            'vpc_securitygroup': {
                'rules': {
                    'create': getattr(infra_conf, f'DEFAULT_SEC_GROUP_RULE_{cluster_type}'),
                }
            }
        }
        code, _ = await http.put(f'{self.endpoint}{api}', params=params, data=data, headers=headers)
        if 199 < code < 300:
            logger.info(f'Add KMR security group rules succeeded. security group id: {sec_group_id}, '
                        f'add_kmr_tag: {add_kmr_tag}')
            return True
        raise VpcNeutronException(f'_create_sec_group_rule failed. security group id: {sec_group_id}, '
                                  f'add_kmr_tag: {add_kmr_tag}')

    @_prepare
    async def get_or_create_sec_group(self, sg_name, vpc_domain_id, sec_group_description,
                                      cluster_type, *,
                                      tenant_id=None, tag=False,
                                      params: dict = None, headers: dict = None):
        add_kmr_tag = True if 'CONTROL' in sg_name else False
        if add_kmr_tag:
            headers.setdefault('X-Auth-User-Tag', 'kmr')

        sec_group = await self.get_security_group_by_name(sg_name, vpc_domain_id,
                                                          add_kmr_tag=add_kmr_tag,
                                                          tenant_id=tenant_id)
        if not sec_group:
            if tag:
                raise VpcNeutronException('acquired security group id failed')
            sec_group = await self._create_security_group(sg_name, vpc_domain_id, sec_group_description,
                                                          tenant_id=tenant_id,
                                                          add_kmr_tag=add_kmr_tag)
            sec_group_id = sec_group.get('id', None)
            if not sec_group_id:
                raise Exception(f'get_or_create_sec_group create sec_group failed.')
            if not await self._create_sec_group_rule(sec_group_id, cluster_type,
                                                     tenant_id=tenant_id,
                                                     add_kmr_tag=add_kmr_tag):
                raise Exception(f'get_or_create_sec_group create sec_group rule failed. '
                                f'security group id: {sec_group_id}')

            logger.info(f'Created security group id: {sec_group_id}, add_kmr_tag: {add_kmr_tag}')

        return sec_group

    @_prepare
    async def get_security_group_by_name(self, sg_name, vpc_id, *, add_kmr_tag=False,
                                         tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['get_sg'].format(sg_name=sg_name, domain_id=vpc_id)
        code, ret = await http.get(f'{self.endpoint}{api}', params=params, headers=headers)
        vpc_sec_groups = ret.get('vpc_securitygroups', [])
        if vpc_sec_groups:
            sec_group = vpc_sec_groups[0]
            sec_group_id = sec_group.get('id', None)
            logger.info(f'Got security group id: {sec_group_id}, add_kmr_tag: {add_kmr_tag}')
            return sec_group
        return None

    @_prepare
    async def get_security_group_by_id(self, sg_id, *, add_kmr_tag=False,
                                       tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['get_sg_id'].format(sg_id=sg_id)

        code, ret = await http.get(f'{self.endpoint}{api}', params=params, headers=headers)

        return ret.get('vpc_securitygroup', {})

    async def is_rule_exist(self, sg_id, direction, ip, mask, protocol, start_port, end_port,
                            tenant_id=None, add_kmr_tag=False):
        sec_group = await self.get_security_group_by_id(sg_id, tenant_id=tenant_id, add_kmr_tag=add_kmr_tag)
        sec_group_rules = sec_group.get('rules', [])

        logger.info(f'==sec_group_rules: {sec_group_rules}')
        for _eco_rule in sec_group_rules:
            if direction == _eco_rule['direction'] and ip == _eco_rule['ip'] and protocol == _eco_rule[
                'protocol'] and mask == _eco_rule['mask'] and start_port == _eco_rule['port_start'] and end_port == \
                    _eco_rule['port_end']:
                return True
        return False

    async def check_security_group_rules(self, sg_id, cluster_type, tenant_id=None, add_kmr_tag=False):
        rules = infra_conf.DEFAULT_SEC_GROUP_CONTROL_RULE if add_kmr_tag \
            else getattr(infra_conf, f'DEFAULT_SEC_GROUP_RULE_{cluster_type}')
        security_group = await self.get_security_group_by_id(sg_id, tenant_id=tenant_id, add_kmr_tag=add_kmr_tag)

        # 客户可能修改安全组规则，所以跳过校验IP协议以及CIDR
        sec_group_rules = security_group.get('rules', [])
        for rule in rules:
            protocol = rule.get('protocol', None)
            checked = False

            if protocol == 'ip':
                continue

            for sec_rule in sec_group_rules:
                if rule.get('direction') != sec_rule.get('direction'):
                    continue
                # if rule.get('ip') != sec_rule.get('ip'):
                #     continue
                if protocol == 'tcp':
                    if rule.get('port_start') != sec_rule.get('port_start'):
                        continue
                    if rule.get('port_end') != sec_rule.get('port_end'):
                        continue
                # elif protocol == 'ip':
                #     if protocol != sec_rule.get('protocol'):
                #         continue

                checked = True
                break

            if not checked:
                logger.error(f'==check_security_group_rules==> rule: {rule} does not exist in security_group: {sg_id}')
                return False

        return True

    @_prepare
    async def add_rules_to_sec_group(self, sg_id, rules,
                                     tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['create_rules'].format(sg_id=sg_id)

        code, _ = await http.put(f'{self.endpoint}{api}', params=params, data=rules, headers=headers)
        if 199 < code < 300:
            logger.info(f'Add extra rules to security group succeeded. sec_id: {sg_id}')
            return sg_id
        raise Exception(f'Add extra rules to security group failed. sec_id: {sg_id}')

    @_prepare
    async def _create_inner_eip(self, add_kmr_tag=True,
                                tenant_id=None, params: dict = None, headers: dict = None):
        network_id = None
        api = NEUTRON_API['get_inner_networks']
        code, ret = await http.get(f'{self.endpoint}{api}', params=params, headers=headers)

        if 199 < code < 300:
            if ret and ret['networks'] and len(ret['networks']) > 0:
                # network_id = ret['networks'][0]['id']
                network_id = self.network_id or 'b6d8767b-4ce3-45a2-9960-8a1ccf34e953'
                logger.debug(f'Got inner eip network id: {network_id}')
        if network_id is None:
            logger.warning("Can not get inner eip network id.")
            raise VpcNeutronException('Can not get inner eip network id')

        api = NEUTRON_API['create_inner_floating_ip']
        payload = {
            "floatingip": {"floating_network_id": network_id,
                           "egress": 1,
                           "ingress": 5,
                           "subnet_id": None,
                           "floating_ip_address": None
                           }
        }
        code, ret = await http.post(f'{self.endpoint}{api}', params=params, data=payload, headers=headers)

        if 199 < code < 300:
            if ret and ret['floatingip']:
                eip_id = ret['floatingip']['id']
                eip = ret['floatingip']['floating_ip_address']
                logger.debug(f'Successful created inner eip: {eip_id}')
                return eip_id, eip
        raise Exception(f'_create_inner_eip failed, error: {ret}')

    @_prepare
    async def _delete_inner_eip(self, eip_id, add_kmr_tag=False,
                                tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['delete_inner_floating_ip'].format(eip_id=eip_id)
        code, ret = await http.delete(f'{self.endpoint}{api}', params=params, headers=headers)
        if 199 < code < 300:
            logger.debug(f'Delete inner eip succeed by id: {eip_id}')
            return True
        raise Exception(f'Delete inner eip failed id: {eip_id}')

    @_prepare
    async def _create_lb_pool(self, domain_id, eip_id, endpoint_id=None, add_kmr_tag=True,
                              tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['create_pool']
        payload = {
            "pool": {
                "name": "kmr-pool",
                "domain_id": domain_id,
                "type": "common",
                "floatingip_id": eip_id,
                "admin_state_up": "true",
                "description": "KMR pool of inner lbs for accessing users vms."
            }
        }

        code, ret = await http.post(f'{self.endpoint}{api}', params=params, data=payload, headers=headers)
        if 199 < code < 300:
            if ret and ret['pool']:
                lb_id = ret['pool']['id']
                logger.debug(f'Create lb pool succeed on eip: {eip_id}')
                return lb_id
        raise Exception(f'Create lb pool failed on eip: {eip_id}, error: {ret}')

    @_prepare
    async def _delete_lb_pool(self, pool_id, add_kmr_tag=True,
                              tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['delete_pool'].format(pool_id=pool_id)

        code, _ = await http.delete(f'{self.endpoint}{api}', params=params, headers=headers)

        if 199 < code < 300:
            logger.debug("Delete lb pool succeed by id: {pool_id}".format(pool_id=pool_id))
            return True
        logger.warning("Delete lb pool failed by id: {pool_id}".format(pool_id=pool_id))
        return False

    @_prepare
    async def _create_vip(self, pool_id, out_port, add_kmr_tag=True,
                          tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['create_vip']
        payload = {
            "vip": {
                "name": "vip-tcp",
                "pool_id": pool_id,
                "protocol": "TCP",
                "protocol_port": out_port,
                "lb_method": "ROUND_ROBIN",
                "lb_kind": "FNAT",
                "emode": True,
                "syn_proxy": True
            }
        }
        code, ret = await http.post(f'{self.endpoint}{api}', params=params, data=payload, headers=headers)

        if 199 < code < 300:
            if ret and ret['vip']:
                vip_id = ret['vip']['id']
                logger.debug(f'Create vip succeed with id: {vip_id} on pool: {pool_id}')
                return vip_id
        raise Exception(f'Create vip failed on vm_id: {pool_id}')

    @_prepare
    async def _delete_vip(self, vip_id, add_kmr_tag=True,
                          tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['delete_vip'].format(vip_id=vip_id)
        code, _ = await http.delete(f'{self.endpoint}{api}', params=params, headers=headers)

        if 199 < code < 300:
            logger.debug(f'Delete vip succeed by id: {vip_id}')
            return True
        logger.warning(f'Delete vip failed by id: {vip_id}')
        return False

    @_prepare
    async def _create_lb_member(self, vip_id, vm_ip, inner_port, add_kmr_tag=True,
                                tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['create_lb_member']
        payload = {
            "member": {
                "vip_id": vip_id,
                "type": "vpcvm",
                "address": vm_ip,
                "protocol_port": inner_port,
                "weight": 100
            }
        }
        code, ret = await http.post(f'{self.endpoint}{api}', params=params, data=payload, headers=headers)

        if 199 < code < 300:
            if ret and ret['member']:
                member_id = ret['member']['id']
                logger.debug(f'Create lb member succeed with id: {member_id} on vip: {vip_id} to vm ip: {vm_ip}')
                return vip_id
        logger.warning(f'Create lb member fail on vip: {vip_id} to vm ip: {vm_ip}')
        return None

    @_prepare
    async def _delete_lb_member(self, member_id, add_kmr_tag=True,
                                tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['delete_lb_member'].format(member_id=member_id)
        code, _ = await http.delete(f'{self.endpoint}{api}', params=params, headers=headers)

        if 199 < code < 300:
            logger.debug(f'Delete lb member succeed by id: {member_id}')
            return True
        logger.warning(f'Delete lb member failed by id: {member_id}')
        return False

    @_prepare
    async def _get_vips_by_pool_id(self, pool_id, add_kmr_tag=True,
                                   tenant_id=None, params: dict = None, headers: dict = None):
        vip_arr = []
        api = NEUTRON_API['list_vips_by_pool_id'].format(pool_id=pool_id)
        code, ret = await http.get(f'{self.endpoint}{api}', params=params, headers=headers)

        if 199 < code < 300:
            if ret and ret['vips'] and len(ret['vips']) > 0:
                for vip in ret['vips']:
                    vip_arr.append(vip['id'])
        return vip_arr

    @_prepare
    async def _get_eip_id_from_pool(self, pool_id, add_kmr_tag=True,
                                    tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['get_pool'].format(pool_id=pool_id)

        code, ret = await http.get(f'{self.endpoint}{api}', params=params, headers=headers)
        if 199 < code < 300:
            if ret and ret['pools'] and len(ret['pools']) > 0:
                eip = ret['pools'][0]['address']
                api = NEUTRON_API['get_eip_id_by_ip'].format(eip=eip)
                code, ret = await http.get(f'{self.endpoint}{api}', params=params, headers=headers)
                if 199 < code < 300:
                    if ret and ret['floatingips'] and len(ret['floatingips']) > 0:
                        eip_id = ret['floatingips'][0]['id']
                        logger.debug(f'Successful got inner eip_id: {eip_id} by pool id: {pool_id}')
                        return eip_id
        raise Exception(f'Cannot get inner eip_id by pool id: {pool_id}')

    @_prepare
    async def get_eip_id_from_eip(self, eip, add_kmr_tag=True,
                                  tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['get_eip_id_by_ip'].format(eip=eip)
        code, ret = await http.get(f'{self.endpoint}{api}', params=params, headers=headers)
        if 199 < code < 300:
            if ret and ret['floatingips'] and len(ret['floatingips']) > 0:
                eip_id = ret['floatingips'][0]['id']
                return eip_id
        raise Exception(f'get_eip_id_from_eip failed, return: {ret}')

    @_prepare
    async def _get_pool_id_by_vip_id(self, vip_id, add_kmr_tag=True,
                                     tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['get_vip'].format(vip_id=vip_id)
        code, ret = await http.get(f'{self.endpoint}{api}', params=params, headers=headers)

        if 199 < code < 300:
            vip = ret.get('vip', {})
            pool_id = vip.get('pool_id', None)
            logger.debug(f'Successful got pool id: {pool_id} by vip id: {vip_id}')
            return pool_id
        raise Exception(f'Cannot get pool id by vip id: {vip_id}')

    @_prepare
    async def _get_inner_eip(self, inner_eip_id,
                             tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['get_inner_floating_ip'].format(eip_id=inner_eip_id)
        code, ret = await http.get(f'{self.endpoint}{api}', params=params, headers=headers)

        if 199 < code < 300:
            floatingips = ret.get('floatingips', [])
            if floatingips:
                eip = floatingips[0]['floating_ip_address']
                logger.debug(f'Successful got inner eip: {eip} by id: {inner_eip_id}')
                return eip
        raise Exception(f'Cannot get inner eip by id: {inner_eip_id}')

    async def _rollback(self, eip_id=None, pool_id=None, add_kmr_tag=True):
        if eip_id is not None:
            await self._delete_inner_eip(eip_id, add_kmr_tag)
            return
        if pool_id is not None:
            for vip_id in await self._get_vips_by_pool_id(pool_id, add_kmr_tag):
                await self._delete_vip(vip_id, add_kmr_tag)
            eip_id = await self._get_eip_id_from_pool(pool_id, add_kmr_tag)
            await self._delete_lb_pool(pool_id, add_kmr_tag)
            if eip_id is not None:
                await self._delete_inner_eip(eip_id, add_kmr_tag)

    async def create_lbs(self, cluster_id, tenant_id):
        cluster = await get_model_by_id(ClusterModel, cluster_id)
        instances = []
        for ig in cluster.instance_groups:
            instances.extend(ig.instances)
        kmr_tag = True
        inner_eip_dict = {}
        for instance in instances:
            # for scale scenario: some instance already done the lb way.
            if instance.inner_eip is not None:
                continue
            # create inner lb from beginning
            if instance.internal_ip is None:
                logger.warn(
                    f'Instance internal eip is not found: {instance.instance_id}')
                raise VpcNeutronException('Instance internal eip not found')
            eip_id, eip = await self._create_inner_eip(add_kmr_tag=kmr_tag, tenant_id=tenant_id)
            logger.warn(f'==EIP_ID,EIP=={eip_id}, {eip}')
            if eip_id is None:
                logger.warning(f'Create inner lbs for cluster: {cluster.id} failed [eip create fail]')
                raise VpcNeutronException('Create eip id failed')
            pool_id = await self._create_lb_pool(cluster.vpc_domain_id, eip_id,
                                                 add_kmr_tag=kmr_tag, tenant_id=tenant_id)
            logger.warn(f'==POOL_ID=={pool_id}')
            if pool_id is None:
                logger.warning(f'Create inner lbs for cluster: {cluster.id} failed [lb pool create fail]')
                # roll back to delete eip
                raise VpcNeutronException('Create pool id failed')
            port = config.get('vpc', 'ssh_port')
            vip_id = await self._create_vip(pool_id, port, add_kmr_tag=kmr_tag, tenant_id=tenant_id)
            logger.warn(f'==VIP_ID=={vip_id}')
            if vip_id is None:
                logger.warning(f'Create inner lbs for cluster: {cluster.id} failed [vip for {port} create fail]')
                raise VpcNeutronException('create vip id failed')
            lb_member_id = await self._create_lb_member(vip_id, vm_ip=instance.internal_ip, inner_port=port,
                                                        add_kmr_tag=kmr_tag, tenant_id=tenant_id)
            if lb_member_id is None:
                logger.warning(
                    f'Create inner lbs for cluster: {cluster.id} failed [lb_member for {port} create fail]')
                raise VpcNeutronException('create lb member id failed')
            # success create lb for instance, and update instance information
            inner_eip_dict[instance.instance_id] = eip

        logger.info(f'Create inner lbs for cluster: {cluster.id} succeed')
        return inner_eip_dict

    async def create_inner_lbs(self, cluster, instance, internal_ip, tenant_id):
        kmr_tag = True
        port = config.get('vpc', 'ssh_port')

        # create inner lb from beginning
        if internal_ip is None:
            raise VpcNeutronException(f'Instance internal eip not found: {instance.instance_id}')
        eip_id, eip = await self._create_inner_eip(add_kmr_tag=kmr_tag, tenant_id=tenant_id)
        logger.info(f'==EIP_ID,EIP=={eip_id}, {eip}')
        if not eip_id:
            raise VpcNeutronException(f'Create inner lbs for cluster: {cluster.id} failed [eip create fail]')
        pool_id = await self._create_lb_pool(
            cluster.vpc_domain_id, eip_id, add_kmr_tag=kmr_tag, tenant_id=tenant_id)
        logger.info(f'==POOL_ID=={pool_id}')
        if not pool_id:
            raise VpcNeutronException(f'Create inner lbs for cluster: {cluster.id} failed [lb pool create fail]')
        vip_id = await self._create_vip(pool_id, port, add_kmr_tag=kmr_tag, tenant_id=tenant_id)
        logger.info(f'==VIP_ID=={vip_id}')
        if not vip_id:
            raise VpcNeutronException(f'Create inner lbs for cluster: {cluster.id} failed [vip for {port} create fail]')
        lb_member_id = await self._create_lb_member(vip_id, vm_ip=internal_ip, inner_port=port,
                                                    add_kmr_tag=kmr_tag, tenant_id=tenant_id)
        if not lb_member_id:
            raise VpcNeutronException(f'Create inner lbs for cluster: {cluster.id} failed '
                                      f'[lb_member for {port} create fail]')

        return eip

    @_prepare
    async def _get_lb_member_by_vm_id(self, vm_id, add_kmr_tag=True, tenant_id=None,
                                      params: dict = None, headers: dict = None):
        logger.debug(f'Attempted to delete inner lb of instance {vm_id}...')
        api = NEUTRON_API['get_lb_member'].format(vm_id=vm_id)
        code, ret = await http.get(f'{self.endpoint}{api}', params=params, headers=headers)
        if 199 < code < 300:
            return ret.get('members', [])
        return []

    async def delete_inner_lbs(self, instance, add_kmr_tag=True,
                               tenant_id=None):
        members = await self._get_lb_member_by_vm_id(instance.instance_id,
                                                     add_kmr_tag=add_kmr_tag, tenant_id=tenant_id)
        pool_id = None
        for member in members:
            vip_id = member['vip_id']
            pool_id = await self._get_pool_id_by_vip_id(vip_id, add_kmr_tag=add_kmr_tag, tenant_id=tenant_id)
            logger.debug(f'Success got vip id: {vip_id} and pool id: {pool_id}')
            ret = await self._delete_vip(vip_id, add_kmr_tag=add_kmr_tag, tenant_id=tenant_id)
            if ret is not True:
                logger.warning(f'Delete inner lb for instance: {instance.instance_id} '
                               f'failed [vip delete fail]')
                raise VpcNeutronException('delete vip id failed')
        if pool_id is not None:
            eip_id = await self._get_eip_id_from_pool(pool_id, add_kmr_tag=add_kmr_tag, tenant_id=tenant_id)
            ret = await self._delete_lb_pool(pool_id, add_kmr_tag=add_kmr_tag, tenant_id=tenant_id)
            if ret is not True:
                logger.warning(f'Delete inner lb for instance: {instance.instance_id} '
                               f'failed [pool delete fail]')
                raise VpcNeutronException('pool id delete fail')
            if eip_id is not None:
                ret = await self._delete_inner_eip(eip_id, add_kmr_tag=add_kmr_tag, tenant_id=tenant_id)
                if ret is not True:
                    logger.warning(f'Delete inner lb for instance: {instance.instance_id} '
                                   f'failed [inner eip delete fail]')
                    raise VpcNeutronException('inner eip delete fail')
        logger.info(f'Delete inner lb for instance: {instance.instance_id} succeed')

    async def bind_kmr_control_sg(self, instances, sg_id, cluster_type,
                                  account_id=None, tenant_id=None):
        vpc_client = getattr(sdk, f'vpc_client_{cluster_type.lower()}')
        vifs = list()
        for instance in instances:
            network_interface_id = await vpc_client.get_network_interface_id(instance.instance_id,
                                                                             account_id=account_id)
            vifs.append(network_interface_id)
        t_hold = int(config.get('vpc', 'bind_vif_nums_once'))
        if len(vifs) <= t_hold:
            await self.bind_sg_and_vm(vifs, sg_id,
                                      add_kmr_tag=True,
                                      tenant_id=tenant_id)
        else:
            for i in range(0, len(vifs), t_hold):
                await self.bind_sg_and_vm(vifs[i:i + t_hold], sg_id,
                                          add_kmr_tag=True,
                                          tenant_id=tenant_id)

    @_prepare
    async def bind_sg_and_vm(self, vifs, sg_id, add_kmr_tag=True,
                             tenant_id=None, params: dict = None, headers: dict = None):
        api = NEUTRON_API['handle_vif_sg'].format(sg_id=sg_id)
        payload = {
            "vifs": {
                "nova_vif_list": vifs,
                "action": "bind"
            }
        }
        code, ret = await http.put(f'{self.endpoint}{api}', params=params, data=payload, headers=headers)

        if 199 < code < 300:
            logger.info(f'Bind control security group id: {sg_id}, vifs: {vifs}')
            return True
        raise VpcNeutronException(f"Can not bind sahara control security group with vms. sg_id: {sg_id}")

    @_prepare
    async def get_floating_ips(self, add_kmr_tag=True,
                               tenant_id=None, params: dict = None, headers: dict = None):
        api = 'floatingips'

        code, ret = await http.get(f'{self.endpoint}{api}', params=params, headers=headers)

        if 199 < code < 300:
            return ret.get('floatingips', [])
        return None
