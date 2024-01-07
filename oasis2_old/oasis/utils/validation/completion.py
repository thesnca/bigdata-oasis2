from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.utils.convert import convert_volume_type
from oasis.utils.exceptions import ValidationError
from oasis.utils.generator import gen_name


async def list_clusters_completion(product, params):
    params.setdefault('cluster_status', 'NONDELETED')
    return params


async def describe_cluster_completion(product, params):
    return params


async def launch_cluster_completion(product, params: dict):
    main_version = params.pop('main_version')
    params.setdefault('main_version', f'OpenSource-{main_version}')
    params.setdefault('distribution', f'{product}-1.0.0')
    params.setdefault('plugins', ['analysis-ik'])

    name = params.pop('cluster_name', None) or gen_name(product, 'timestamp')
    params.setdefault('name', name)

    params.setdefault('cluster_type', product.upper())

    charge_type = params.pop('charge_type')
    purchase_time = params.pop('purchase_time', 0)
    if charge_type == 'HourlyInstantSettlement':
        charge_type = 'Minutely'
    if charge_type == 'Monthly':
        if not purchase_time:
            raise ValidationError(f'\'PurchaseTime\' should between 1-36, '
                                  f'got {purchase_time}.')
        params.setdefault('purchase_time', purchase_time)
    params.setdefault('charge_type', charge_type)

    enable_eip = params.get('enable_eip', False)
    eip_id = params.pop('eip_id', None)
    eip_charge_type = params.pop('eip_charge_type', None)
    eip_purchase_time = params.pop('eip_purchase_time', None)
    eip_line_id = params.pop('eip_line_id', None)
    eip_band_width = params.pop('eip_band_width', None)

    if enable_eip:
        # OpenAPI只开放公网EIP
        params.setdefault('eip_type', 'EIP')
        params.setdefault('enable_slb', True)
        if eip_id:
            params.setdefault('allocation_id', eip_id)
            params.setdefault('ip_addr', '0.0.0.0')  # Fill it on TaskCreateEip
        elif eip_charge_type and eip_line_id:
            if eip_charge_type == 'Monthly':
                if not eip_purchase_time:
                    raise ValidationError(f'\'EipPurchaseTime\' should between 1-36, '
                                          f'got {eip_purchase_time}.')
                params.setdefault('eip_purchase_time', eip_purchase_time)

            params.setdefault('eip_charge_type', eip_charge_type)
            params.setdefault('eip_line_id', eip_line_id)
            params.setdefault('eip_band_width', eip_band_width)

    vpc_domain_id = params.get('vpc_domain_id')
    vpc_subnet_id = params.get('vpc_subnet_id', None)
    vpc_epc_subnet_id = params.get('vpc_epc_subnet_id', None)

    instance_groups = params.get('instance_groups', [])

    new_ig_types = []
    for ig in instance_groups:
        instance_group_type = ig.get('instance_group_type')
        instance_count = ig.get('instance_count')
        if instance_group_type in ['DATA'] and instance_count < 3:
            raise ValidationError(f'\'{instance_group_type}\' group should have at lease 3 instances, '
                                  f'got {instance_count}.')
        elif instance_group_type in ['MASTER'] and instance_count != 3:
            raise ValidationError(f'\'{instance_group_type}\' group should have 3 instances, '
                                  f'got {instance_count}.')
        elif instance_group_type in ['WARM', 'COORDINATOR'] and instance_count < 2:
            raise ValidationError(f'\'{instance_group_type}\' group should have at lease 2 instances, '
                                  f'got {instance_count}.')
        new_ig_types.append(instance_group_type)

        resource_type = ig.pop('resource_type', None) or 'KEC'

        # for EPC,
        # instance_type_code: CAL-ES.normal.4C4G ==> instance_type_code: CAL, instance_type: ES.normal.4C4G
        instance_type_code = ig.pop('instance_type')
        if resource_type == 'EPC':
            tmps = instance_type_code.split('-')
            instance_type_code = '-'.join(tmps[:-1])
            instance_type = tmps[-1]
            ig.setdefault('instance_type', instance_type)
        ig.setdefault('instance_type_code', instance_type_code)

        raid_type = ig.pop('raid_type', None)
        multi_instance_count = ig.pop('multi_instance_count', None)
        if resource_type == 'KEC':
            ig.setdefault('vpc_subnet_id', vpc_subnet_id)
        elif resource_type == 'EPC':
            if not raid_type:
                raise ValidationError(f'\'RaidType\' invalid for epc instance, got: {raid_type}.')

            if not vpc_epc_subnet_id:
                raise ValidationError(f'\'VpcEpcSubnetId\' invalid for epc instance, got: {vpc_epc_subnet_id}.')

            resource_attributes = [
                {
                    'name': 'bond_type',
                    'value': 'bond4',
                },
                {
                    'name': 'raid_type',
                    'value': raid_type,
                },
            ]

            ig.setdefault('resource_attributes', resource_attributes)
            ig.setdefault('vpc_subnet_id', vpc_epc_subnet_id)
            ig.setdefault('multi_instance_count', multi_instance_count)

        volume_type = ig.pop('volume_type', None)
        volume_type = convert_volume_type(volume_type)
        volume_size = ig.pop('volume_size', None)
        if '.D4.' in instance_type_code:
            # D4不支持按量付费
            if charge_type == 'Minutely':
                raise ValidationError(f'\'{instance_type_code}\' did not support charge type: {charge_type}.')

            # D4只支持DATA、WARM节点
            if instance_group_type in ['MASTER', 'COORDINATOR']:
                raise ValidationError(
                    f'\'{instance_type_code}\' did not support instance group type: {instance_group_type}.')

            volume_type = 'LOCAL_HDD'
            volume_size = 7400  # volume_size and volume_count will be filled in TaskInitClusterCreate
            ig.setdefault('system_disk_type', 'SSD3.0')

        elif resource_type == 'EPC':
            volume_size = 0
        else:
            if not volume_type:
                raise ValidationError(f'\'VolumeType\' invalid, '
                                      f'got {volume_type}.')
            if not volume_size:
                raise ValidationError(f'\'VolumeSize\' invalid, '
                                      f'got {volume_size}.')

        ig.setdefault('vpc_id', vpc_domain_id)
        ig.setdefault('resource_type', resource_type)
        ig.setdefault('volume_type', volume_type)
        ig.setdefault('volume_size', volume_size)
        ig.setdefault('volume_count', 1)

    if 'DATA' not in new_ig_types:
        raise ValidationError(f'\'DATA\' group must be created, '
                              f'got {new_ig_types}.')

    return params


async def scale_out_instance_groups_completion(product, params):
    params.setdefault('cluster_type', product.upper())

    cluster_id = params.get('cluster_id')
    cluster = await get_model_by_id(ClusterModel, cluster_id)

    charge_type = cluster.charge_type
    purchase_time = cluster.purchase_time or 0
    params.setdefault('charge_type', charge_type)
    params.setdefault('purchase_time', purchase_time)

    vpc_domain_id = cluster.vpc_domain_id
    vpc_subnet_id = cluster.vpc_subnet_id
    security_group_id = cluster.security_group_id
    vpc_epc_subnet_id = None

    params.setdefault('security_group_id', security_group_id)

    instance_groups = params.get('instance_groups', [])

    if not instance_groups:
        raise ValidationError(f'Got no instance group to scale out.')

    if len(instance_groups) > 1:
        raise ValidationError(f'ScaleOut only support one instance group at a time, '
                              f'got {len(instance_groups)}.')

    ig = instance_groups[0]
    instance_group_type = ig.get('instance_group_type')
    o_ig = None
    for old_ig in cluster.instance_groups:
        if old_ig.instance_group_type == instance_group_type:
            o_ig = old_ig
            break

    # Scale exist instance group
    if o_ig:
        ig['id'] = o_ig.id
        ig['resource_type'] = o_ig.resource_type
        ig['instance_type'] = o_ig.instance_type_code
        ig['resource_attr'] = o_ig.resource_attr
        ig['multi_instance_count'] = o_ig.multi_instance_count
        ig['volume_type'] = o_ig.volume_type
        ig['volume_size'] = o_ig.volume_size
        ig['volume_count'] = o_ig.volume_count

        if instance_group_type == 'EPC':
            vpc_epc_subnet_id = o_ig.vpc_subnet_id

    # For new instance group, fill params
    else:
        instance_count = ig.get('instance_count')
        if instance_group_type in ['DATA', 'MASTER'] and instance_count < 3:
            raise ValidationError(f'\'{instance_group_type}\' group should have at lease 3 instances, '
                                  f'got {instance_count}.')
        elif instance_group_type in ['WARM', 'COORDINATOR'] and instance_count < 2:
            raise ValidationError(f'\'{instance_group_type}\' group should have at lease 2 instances, '
                                  f'got {instance_count}.')
        ig['volume_type'] = convert_volume_type(ig.pop('volume_type', None))

    resource_type = ig.pop('resource_type', None) or 'KEC'
    # for EPC,
    # instance_type_code: CAL-ES.normal.4C4G ==> instance_type_code: CAL, instance_type: ES.normal.4C4G
    instance_type_code = ig.pop('instance_type')
    if resource_type == 'EPC':
        tmps = instance_type_code.split('-')
        instance_type_code = '-'.join(tmps[:-1])
        instance_type = tmps[-1]
        ig.setdefault('instance_type', instance_type)

    multi_instance_count = ig.pop('multi_instance_count', 1)
    ig.setdefault('instance_type_code', instance_type_code)
    ig.setdefault('vpc_id', vpc_domain_id)
    if resource_type == 'KEC':
        ig.setdefault('vpc_subnet_id', vpc_subnet_id)
    elif resource_type == 'EPC':
        resource_attributes = ig.get('resource_attr', [])
        ig.setdefault('resource_attributes', resource_attributes)
        ig.setdefault('vpc_subnet_id', vpc_epc_subnet_id)
        ig.setdefault('multi_instance_count', multi_instance_count)

    volume_type = ig.pop('volume_type', None)
    volume_size = ig.pop('volume_size', None)
    if '.D4.' in instance_type_code:
        # D4只支持DATA、WARM节点
        if instance_group_type in ['MASTER', 'COORDINATOR']:
            raise ValidationError(
                f'\'{instance_type_code}\' did not support instance group type: {instance_group_type}.')

        volume_type = 'LOCAL_HDD'
        volume_size = 7400  # volume_size and volume_count will be filled in TaskInitClusterCreate
        ig.setdefault('system_disk_type', 'SSD3.0')

    elif resource_type == 'EPC':
        volume_size = 0
    else:
        if not volume_type:
            raise ValidationError(f'\'VolumeType\' invalid, '
                                  f'got {volume_type}.')
        if not volume_size:
            raise ValidationError(f'\'VolumeSize\' invalid, '
                                  f'got {volume_size}.')

    ig.setdefault('resource_type', resource_type)
    ig.setdefault('volume_type', volume_type)
    ig.setdefault('volume_size', volume_size)
    ig.setdefault('volume_count', 1)

    return params


async def restart_cluster_completion(product, params):
    if 'rolling' not in params:
        params.setdefault('rolling', 1)
    params.setdefault('control_type', 'restart')
    return params
