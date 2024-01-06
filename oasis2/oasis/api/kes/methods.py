from sqlalchemy.sql.elements import and_
from sqlalchemy.sql.elements import or_

from oasis.api.base.methods import get_bind_eip
from oasis.api.base.methods import get_unbind_eip
from oasis.api.kes.results.kes_cluster import KesCluster
from oasis.api.kes.results.kes_cluster_summary import KesClusterSummary
from oasis.api.kes.results.kes_plugin import KesPlugin
from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.es_plugin import EsPluginModel
from oasis.db.models.user import UserModel
from oasis.utils.convert import replace_wildcards
from oasis.utils.generator import gen_uuid4
from oasis.utils.generator import get_url_suffix


async def describe_cluster_from_db(cluster_id, account_id=None):
    cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
    if not cluster:
        raise Exception(f'Cluster not found, id {cluster_id}')
    kes_cluster = KesCluster(cluster).__dict__
    if kes_cluster['EnableEip']:
        eip_info = await get_bind_eip(cluster_id)
        if eip_info:
            kes_cluster['Eip'] = eip_info.eip_address
            kes_cluster['SlbId'] = eip_info.load_balancer_id
        else:
            slb_info = await get_unbind_eip(cluster_id)
            if slb_info:
                kes_cluster['SlbId'] = slb_info.load_balancer_id

    # region gringotts
    kes_cluster['ProxyPort'] = 28291
    kes_cluster['ProxyUrlSuffix'] = get_url_suffix(
        cluster_id, product_type=cluster.cluster_type.lower())

    return kes_cluster


async def list_clusters_from_db(filters, offset, limit, *,
                                cluster_status=None, created_after=None, created_before=None,
                                account_id=None,
                                ):
    user_model = await get_model_by_id(UserModel, account_id)
    if not user_model:
        raise Exception(f'User not found, id {account_id}')
    account_role = user_model.role

    query = model_query(ClusterModel)
    query = query.filter(ClusterModel.cluster_type == 'KES')

    if account_role == UserModel.ROLE.NORMAL:
        if not account_id:
            return 0, []
        query = query.filter(ClusterModel.ksc_user_id == account_id)
    elif account_role != UserModel.ROLE.ADMIN:
        return 0, []

    # TODO do we need this?
    # query = query.filter_by(**kwargs)

    # ALL > NonDeleted > others
    if cluster_status:
        if 'All' in cluster_status:
            pass
        elif 'NonDeleted' in cluster_status:
            query = query.filter(ClusterModel.status != 'Deleted')
        else:
            query = query.filter(ClusterModel.status.in_(tuple(cluster_status)))

    if created_after:
        query = query.filter(ClusterModel.created_at >= created_after)
        # outer_createtime_filter_exist = True
    if created_before:
        query = query.filter(ClusterModel.created_at <= created_before)
        # outer_createtime_filter_exist = True

    for f in filters:
        prop = f.get('name', None)
        values = f.get('values', None)
        contain_arr = []
        match_arr = []
        for value in values:
            value = replace_wildcards(value)
            value = value.replace(' ', '')
            if '*' in value or '?' in value:
                value = value.replace('*', '%')
                value = value.replace('?', '_')
                contain_arr.append(value)
            else:
                match_arr.append(value)

        if prop == 'Name':
            name_matches = []
            name_contains = []
            if len(contain_arr) != 0:
                name_contains = contain_arr
            if len(match_arr) != 0:
                name_matches = match_arr
            if len(name_contains) > 0 or len(name_matches) > 0:
                query = query.filter(
                    or_(or_(ClusterModel.name.like(name_contain) for name_contain in name_contains),
                        or_(ClusterModel.name == name_match for name_match in name_matches)))

        elif prop == 'Id':
            id_contains = []
            id_matches = []
            if len(contain_arr) != 0:
                id_contains = contain_arr
            if len(match_arr) != 0:
                id_matches = match_arr
            if len(id_contains) > 0 or len(id_matches) > 0:
                query = query.filter(or_(or_(ClusterModel.id.like(id_contain) for id_contain in id_contains),
                                         or_(ClusterModel.id == id_match for id_match in id_matches)))
        elif prop == 'Fuzzy':
            fuzzy_contains = []
            fuzzy_matches = []
            if len(contain_arr) != 0:
                fuzzy_contains = contain_arr
            if len(match_arr) != 0:
                fuzzy_matches = match_arr
            if len(fuzzy_contains) > 0 or len(fuzzy_matches) > 0:
                query = query.filter(
                    or_(or_(ClusterModel.id.like(fuzzy_contain) for fuzzy_contain in
                            fuzzy_contains),
                        or_(ClusterModel.id == fuzzy_match for fuzzy_match in fuzzy_matches),
                        or_(ClusterModel.name.like(fuzzy_contain) for fuzzy_contain in
                            fuzzy_contains),
                        or_(ClusterModel.name == fuzzy_match for fuzzy_match in fuzzy_matches)))

        elif prop == 'Tag':
            tag_matches = []
            tag_contains = []
            if len(contain_arr) != 0:
                tag_contains = contain_arr
            if len(match_arr) != 0:
                tag_matches = match_arr
            if len(tag_contains) > 0 or len(tag_matches) > 0:
                query = query.filter(
                    or_(ClusterModel.tag_keys.like(tag_contain) for tag_contain in tag_contains)
                )

    query = query.order_by(ClusterModel.created_at.desc())
    count, clusters = await query.query_all(count=True, offset=offset, limit=limit)

    f_clusters = []
    for cluster in clusters:
        summary_cluster = KesClusterSummary(cluster).__dict__ or {}
        instance_id_list = summary_cluster.get('InstanceIdList', {}) or {}
        eip_instance_list = instance_id_list.get('EIP', [])
        slb_instance_list = instance_id_list.get('SLB', [])
        if summary_cluster['EnableEip']:
            eip_info = await get_bind_eip(summary_cluster['ClusterId'])
            if eip_info:
                summary_cluster['Eip'] = eip_info.eip_address
                summary_cluster['SlbId'] = eip_info.load_balancer_id
                eip_instance_list.append(eip_info.allocate_address_id)
                slb_instance_list.append(eip_info.load_balancer_id)

            else:
                slb_info = await get_unbind_eip(summary_cluster['ClusterId'])
                if slb_info:
                    summary_cluster['SlbId'] = slb_info.load_balancer_id
                    slb_instance_list.append(eip_info.load_balancer_id)

        if cluster.extra and cluster.extra.get('bind_eip_status', '') == 'Complete':
            for ig in cluster.instance_groups:
                for ins in ig.instances:
                    if not ins.allocate_address_id:
                        continue
                    eip_instance_list.append(ins.allocate_address_id)

        instance_id_list.setdefault('EIP', eip_instance_list)
        instance_id_list.setdefault('SLB', slb_instance_list)
        summary_cluster.setdefault('InstanceIdList', instance_id_list)
        f_clusters.append(summary_cluster)

    return count, f_clusters


async def list_instance_groups(cluster_id, account_id=None):
    cluster = await get_model_by_id(ClusterModel, cluster_id, account_id=account_id)
    if not cluster:
        raise Exception(f'Cluster not found, id {cluster_id}')
    instance_groups = cluster.instance_groups
    instance_group_list = []
    for instance_group in instance_groups:
        instance_group_id = instance_group['id']
        resource_type = instance_group['resource_type']
        instance_group_type = instance_group['instance_type_code']
        cpus = 0
        ram = 0
        ig = {
            'Id': instance_group_id,
            'InstanceGroupType': instance_group['instance_group_type'],
            'ResourceType': resource_type,
            # for EPC instance_type_code, CAL-ES.normal.4C4G ==> CAL
            # 'InstanceTypeCode': '-'.join(instance_group_type.split('-')[:-1])
            # if resource_type == 'EPC' else instance_group_type,
            # frontend will use origin type code CAL-ES.normal.4C4G
            'InstanceTypeCode': instance_group_type,
            'InstanceCount': instance_group['count'],
            'VolumeType': instance_group['volume_type'],
            'VolumeSize': instance_group['volume_size'],
            'VolumeCount': instance_group['volume_count'],
            'VpcId': instance_group['vpc_domain_id'],
            'VpcSubnetId': instance_group['vpc_subnet_id'],
            'AvalabilityZone': instance_group['availability_zone'],
            'MultiInstanceCount': instance_group.get('multi_instance_count', 1),
        }

        instance_list = []
        for instance in instance_group.instances:
            ins = {
                'Id': instance['id'],
                'InstanceGroupId': instance_group_id,
                'InstanceId': instance['instance_id'],
                'InstanceName': instance['instance_name'],
                'Domain': f"{instance['instance_name']}.ksc.com",
                'InternalIp': instance['internal_ip'],
                'Volumes': instance['volumes']
            }
            cpus = instance['cpus']
            ram = instance['ram']
            instance_list.append(ins)
        ig['Instances'] = instance_list
        ig['cpus'] = cpus
        ig['ram'] = ram
        instance_group_list.append(ig)

    return {
        'InstanceGroups': instance_group_list,
        'Total': len(instance_group_list),
    }


async def list_plugins_from_db(cluster_id, filters, offset, limit, *,
                               created_after=None,
                               created_before=None):
    query = model_query(EsPluginModel)
    query = query.filter(
        and_(
            EsPluginModel.cluster_id == cluster_id,
            EsPluginModel.status != EsPluginModel.STATUS.DELETE_STATUS  # 列出非删除
        )
    )

    if created_after:
        query = query.filter(EsPluginModel.created_at >= created_after)
    if created_before:
        query = query.filter(EsPluginModel.created_at <= created_before)

    for f in filters:
        prop = f.get('name', None)
        values = f.get('values', None)
        contain_arr = []
        match_arr = []
        for value in values:
            value = replace_wildcards(value)
            value = value.replace(' ', '')
            if '*' in value or '?' in value:
                value = value.replace('*', '%')
                value = value.replace('?', '_')
                contain_arr.append(value)
            else:
                match_arr.append(value)

        if prop == 'Name':
            name_matches = []
            name_contains = []
            if len(contain_arr) != 0:
                name_contains = contain_arr
            if len(match_arr) != 0:
                name_matches = match_arr
            if len(name_contains) > 0 or len(name_matches) > 0:
                query = query.filter(
                    or_(or_(EsPluginModel.name.like(name_contain) for name_contain in name_contains),
                        or_(EsPluginModel.name == name_match for name_match in name_matches)))

    query = query.order_by(EsPluginModel.created_at.desc())
    count, plugins = await query.query_all(count=True, offset=offset, limit=limit)

    f_plugins = []
    for plugin in plugins:
        plugin_result = KesPlugin(plugin).__dict__
        f_plugins.append(plugin_result)
    f_plugins.extend(KesPlugin.system_plugin_results)

    return count, f_plugins


async def get_plugin(cluster_id, plugin_name, file_name):
    if not cluster_id or not plugin_name:
        return None

    query = model_query(EsPluginModel)
    query = query.filter(
        EsPluginModel.cluster_id == cluster_id,
        EsPluginModel.status != EsPluginModel.STATUS.DELETE_STATUS
    )
    query = query.filter(or_(
        or_(EsPluginModel.ks3_address.like(f'%{file_name}%')),
        or_(EsPluginModel.name == plugin_name)
    ))

    return await query.query_one()


async def add_plugin(cluster_id, plugin_name, upload_type, ks3_address, description):
    plugin = EsPluginModel()
    plugin.id = gen_uuid4()
    plugin.name = plugin_name
    plugin.cluster_id = cluster_id
    plugin.plugin_type = EsPluginModel.SOURCE.USER_DEFINE_PLUGIN
    plugin.upload_type = upload_type
    plugin.status = EsPluginModel.STATUS.UNINSTALL_STATUS
    plugin.description = str(description) if description else ''
    plugin.ks3_address = ks3_address

    await plugin.save()

    return plugin.id


async def delete_plugin(plugin):
    plugin.status = EsPluginModel.STATUS.DELETE_STATUS

    await plugin.save()

    return True
