from sqlalchemy.sql.elements import or_
from sqlalchemy.sql.expression import false

from oasis.api.base.results.operation_summary import OpClusterSummary
from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.eip import EIPModel
from oasis.db.models.job import JobModel
from oasis.db.models.notification import NotificationModel
from oasis.db.models.user import UserModel
from oasis.utils.convert import replace_wildcards


def slb_check_listener(listeners: list, db_listener: dict, instance_ids: list) -> bool:
    '''
    单独拿出来，是为了验证多条监听器与多个rs时，链路是否符合业务(N:N)
    '''
    result = True
    msg = ''

    if len(listeners) != len(db_listener):
        # 用户创建了SLB，SLB下有其他监听器或者缺少预期监听器：负载均衡下监听器异常（缺少必要监听器或多监听器），请检查您的监听器
        return False, f'负载均衡下监听器异常（缺少必要监听器或多监听器），请检查您的监听器'

    for listener in listeners:
        real_listener_port = str(listener.get('ListenerPort', 0))
        real_listener_id = listener.get('ListenerId', '')
        real_server_ids = []

        # 如果挂了非当前业务线的端口，认为是异常情况
        if db_listener and db_listener.get(real_listener_port, 'N') != real_listener_id:
            result = False
            # 用户创建了SLB，监听器监听资源端口不符：监听器监听集群资源端口异常，请检查您的监听器
            msg = '监听器监听集群资源端口异常，请检查您的监听器'
            break

        if listener.get('ListenerState', '') != 'start':
            result = False
            # 用户创建了SLB，监听器关闭：负载均衡下监听器关闭，访问地址不可用
            msg = '负载均衡下监听器关闭，访问地址不可用'
            break

        real_servers = listener.get('RealServer', [])

        for rs in real_servers:
            real_server_ids.append(rs.get('InstanceId', ''))
            # 业务上，监听器与真实服务器端口一致。
            if str(rs.get('RealServerPort', 0)) != real_listener_port:
                result = False
                # 用户创建了SLB，监听器监听资源端口不符：监听器监听集群资源端口异常，请检查您的监听器
                msg = '监听器监听集群资源端口异常，请检查您的监听器'
                # 放行到下一验证，这里不能break
                continue

        real_server_ids.sort()
        instance_ids.sort()
        if real_server_ids != instance_ids:
            result = False
            # 用户创建了SLB，监听器监听资源缺少或者多：监听器监听资源异常（缺少集群资源或多集群外资源），请检查您的监听器
            msg = '监听器监听资源异常（缺少集群资源或多集群外资源），请检查您的监听器'
            break

    return result, msg


async def get_bind_eip(cluster_id) -> EIPModel:
    query = model_query(EIPModel)
    query.filter(EIPModel.cluster_id == cluster_id)
    query.filter(EIPModel.load_balancer_type == 0)
    query.filter(EIPModel.status == 'Binded')
    return await query.query_one()


async def get_unbind_eip(cluster_id) -> EIPModel:
    query = model_query(EIPModel)
    query.filter(EIPModel.cluster_id == cluster_id)
    query.filter(EIPModel.load_balancer_type == 0)
    query.filter(EIPModel.status == 'Unbinded')
    return await query.query_one()


async def enable_cluster_scale_notification(cluster_id, url, token):
    noti = NotificationModel()
    noti.cluster_id = cluster_id
    noti.url = url
    noti.token = token
    await noti.save()


async def disable_cluster_scale_notification(noti):
    await noti.delete()


async def op_list_users_from_db(filters, offset, limit, *, created_after=None, created_before=None,
                                company_alias=None, account_id=None):
    query = model_query(UserModel)

    if created_after:
        query = query.filter(UserModel.created_at >= created_after)
        # outer_createtime_filter_exist = True
    if created_before:
        query = query.filter(UserModel.created_at <= created_before)
        # outer_createtime_filter_exist = True

    if company_alias:
        query = query.filter(UserModel.company_alias == company_alias)

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

        if prop == 'Id':
            id_contains = []
            id_matches = []
            if len(contain_arr) != 0:
                id_contains = contain_arr
            if len(match_arr) != 0:
                id_matches = match_arr
            if len(id_contains) > 0 or len(id_matches) > 0:
                query = query.filter(or_(or_(UserModel.id.like(id_contain) for id_contain in id_contains),
                                         or_(UserModel.id == id_match for id_match in id_matches)))

        elif prop == 'CompanyAlias':
            company_alias_matches = []
            company_alias_contains = []
            if len(contain_arr) != 0:
                company_alias_contains = contain_arr
            if len(match_arr) != 0:
                company_alias_matches = match_arr
            if len(company_alias_contains) > 0 or len(company_alias_matches) > 0:
                query = query.filter(or_(or_(
                    UserModel.company_alias.like(company_alias_contain) for company_alias_contain in
                    company_alias_contains),
                    or_(UserModel.company_alias == company_alias_match for company_alias_match in
                        company_alias_matches)))

        elif prop == 'UserLevel':
            user_level_matches = []
            user_level_contains = []
            if len(contain_arr) != 0:
                user_level_contains = contain_arr
            if len(match_arr) != 0:
                user_level_matches = match_arr
            if len(user_level_contains) > 0 or len(user_level_matches) > 0:
                query = query.filter(or_(or_(
                    UserModel.user_level.like(user_level_contain) for user_level_contain in
                    user_level_contains),
                    or_(UserModel.user_level == user_level_match for user_level_match in
                        user_level_matches)))

        elif prop == 'Fuzzy':
            fuzzy_contains = []
            fuzzy_matches = []
            if len(contain_arr) != 0:
                fuzzy_contains = contain_arr
            if len(match_arr) != 0:
                fuzzy_matches = match_arr
            if len(fuzzy_contains) > 0 or len(fuzzy_matches) > 0:
                query = query.filter(or_(or_(UserModel.id.like(fuzzy_contain) for fuzzy_contain in fuzzy_contains),
                                         or_(UserModel.id == fuzzy_match for fuzzy_match in fuzzy_matches)),
                                     or_(UserModel.role.like(fuzzy_contain) for fuzzy_contain in fuzzy_contains),
                                     or_(UserModel.role == fuzzy_match for fuzzy_match in fuzzy_matches))

    query = query.order_by(UserModel.created_at.desc())
    count, users = await query.query_all(count=True, offset=offset, limit=limit)
    users = [user.to_dict() for user in users]

    return count, users


async def op_list_jobs_from_db(filters, offset, limit, cluster_id, *, created_after=None, created_before=None,
                               account_id=None):
    query = model_query(JobModel)
    query = query.filter(JobModel.cluster_id == cluster_id)

    if created_after:
        query = query.filter(JobModel.created_at >= created_after)
        # outer_createtime_filter_exist = True
    if created_before:
        query = query.filter(JobModel.created_at <= created_before)
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

        if prop == 'Id':
            id_contains = []
            id_matches = []
            if len(contain_arr) != 0:
                id_contains = contain_arr
            if len(match_arr) != 0:
                id_matches = match_arr
            if len(id_contains) > 0 or len(id_matches) > 0:
                query = query.filter(or_(or_(JobModel.id.like(id_contain) for id_contain in id_contains),
                                         or_(JobModel.id == id_match for id_match in id_matches)))
        elif prop == 'Fuzzy':
            fuzzy_contains = []
            fuzzy_matches = []
            if len(contain_arr) != 0:
                fuzzy_contains = contain_arr
            if len(match_arr) != 0:
                fuzzy_matches = match_arr
            if len(fuzzy_contains) > 0 or len(fuzzy_matches) > 0:
                query = query.filter(or_(or_(JobModel.id.like(fuzzy_contain) for fuzzy_contain in fuzzy_contains),
                                         or_(JobModel.id == fuzzy_match for fuzzy_match in fuzzy_matches),
                                         or_(JobModel.name.like(fuzzy_contain) for fuzzy_contain in fuzzy_contains),
                                         or_(JobModel.name == fuzzy_match for fuzzy_match in fuzzy_matches)))

    query = query.order_by(JobModel.updated_at.desc())
    # query = query.join(ClusterModel, ClusterModel.id == JobModel.cluster_id)
    count, jobs = await query.query_all(count=True, offset=offset, limit=limit)
    # jobs = [job.to_dict() for job in jobs]
    ret = []
    for job in jobs:
        tmp = await get_model_by_id(ClusterModel, job.cluster_id)
        job = job.to_dict()
        job['KscUserId'] = tmp.ksc_user_id if tmp.ksc_user_id else ""
        ret.append(job)

    return count, ret


async def op_list_clusters_from_db(filters, offset, limit, *,
                                   cluster_status=None, cluster_type=None, created_after=None, created_before=None,
                                   expired_after=None, expired_before=None, charge_type=None, account_id=None,
                                   company_alias=None,
                                   ):
    user_model = await get_model_by_id(UserModel, account_id)
    account_role = user_model.role

    query = model_query(ClusterModel)
    if company_alias:
        query = query.outerjoin(UserModel, UserModel.id == ClusterModel.ksc_user_id)
        query = query.filter(UserModel.company_alias == company_alias)

    if cluster_type:
        query = query.filter(ClusterModel.cluster_type.in_(cluster_type))

    if charge_type:
        query = query.filter(ClusterModel.charge_type.in_(charge_type))

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

    if expired_after:
        query = query.filter(ClusterModel.expire_time >= expired_after)
        # outer_expiretime_filter_exist = True
    if expired_before:
        query = query.filter(ClusterModel.expire_time <= expired_before)
        # outer_expiretime_filter_exist = True

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

        elif prop == 'KscUserId':
            kid_contains = []
            kid_matches = []
            if len(contain_arr) != 0:
                kid_contains = contain_arr
            if len(match_arr) != 0:
                kid_matches = match_arr
            if len(kid_contains) > 0 or len(kid_matches) > 0:
                query = query.filter(
                    or_(or_(ClusterModel.ksc_user_id.like(kid_contain) for kid_contain in kid_contains),
                        or_(ClusterModel.ksc_user_id == kid_match for kid_match in kid_matches)))

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

    query = query.order_by(ClusterModel.created_at.desc())
    count, clusters = await query.query_all(count=True, offset=offset, limit=limit)

    f_clusters = []
    for cluster in clusters:
        company_alias = None
        tmp = await get_model_by_id(UserModel, cluster.ksc_user_id)
        if tmp:
            company_alias = tmp.company_alias
        summary_cluster = OpClusterSummary(cluster).__dict__
        if summary_cluster['EnableEip']:
            eip_info = await get_bind_eip(summary_cluster['ClusterId'])
            if eip_info:
                summary_cluster['Eip'] = eip_info.eip_address
                summary_cluster['SlbId'] = eip_info.load_balancer_id
            else:
                slb_info = await get_unbind_eip(summary_cluster['ClusterId'])
                if slb_info:
                    summary_cluster['SlbId'] = slb_info.load_balancer_id
        summary_cluster["company_alias"] = company_alias
        f_clusters.append(summary_cluster)

    return count, f_clusters
