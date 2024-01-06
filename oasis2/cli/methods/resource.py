import asyncio
import os

from sqlalchemy import or_
from termcolor import cprint

from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.gg_components import GgComponentsModel
from oasis.db.models.instance import InstanceModel
from oasis.db.models.instance_group import InstanceGroupModel
from oasis.utils import sdk
from oasis.utils.config import config


async def fuzzy_query_cluster(keyword=None, status=None, account_id=None):
    query = model_query(ClusterModel)
    if keyword:
        query = query.filter(or_(
            or_(ClusterModel.name.ilike(f'%{keyword}%')),
            or_(ClusterModel.id.ilike(f'%{keyword}%')),
        ))

    if status == 'NotDeleted':
        query = query.filter(ClusterModel.status != 'Deleted')
    else:
        query = query.filter(ClusterModel.status == status)

    if account_id:
        query = query.filter(ClusterModel.ksc_user_id == account_id)

    clusters = await query.query_all()
    return clusters


async def get_ssh_cmd(keyword):
    query = model_query(InstanceModel)
    query = query.filter(or_(or_(InstanceModel.instance_name == keyword),
                             or_(InstanceModel.inner_eip == keyword),
                             or_(InstanceModel.internal_ip == keyword),
                             or_(InstanceModel.instance_id == keyword),
                             or_(InstanceModel.id == keyword),
                             ))
    query = query.filter(InstanceModel.status != 'Deleted')
    instance = await query.query_one()
    if not instance:
        return None, None

    query = model_query(ClusterModel)
    query = query.join(InstanceGroupModel)
    query = query.filter(InstanceGroupModel.id == instance.instance_group_id)
    query = query.filter(InstanceGroupModel.status != 'Deleted')
    cluster = await query.query_one()
    if not cluster:
        return None, None

    cprint(cluster.management_private_key, 'grey')
    pk_filename = f'/tmp/pk-oasis-{cluster.id}'
    with open(pk_filename, 'w+') as f:
        f.truncate()
        f.write(cluster.management_private_key)
    os.system(f'chmod 600 {pk_filename}')
    return instance, f'ssh root@{instance.inner_eip} -p 1505 -i {pk_filename}'


async def refresh_agent(cluster):
    async def __inner(instance, cluster_id, gringotts_env, gringotts_repo_url_prefix):
        instance_group_id = instance.instance_group_id
        instance_id = instance.id
        remote = await instance.remote()
        async with remote as conn:
            await conn.execute('sudo cp -f /etc/nodeinfo /etc/nodeinfo.old')

            _nodeinfo = []
            _nodeinfo.append(f"node_id\t{instance_id}")
            _nodeinfo.append(f"node_group_id\t{instance_group_id}")
            _nodeinfo.append(f"cluster_id\t{cluster_id}")
            _nodeinfo.append(f"env {gringotts_env}")

            await conn.write_file('etc-nodeinfo', "\n".join(_nodeinfo))
            await conn.execute('sudo mv -f etc-nodeinfo /etc/nodeinfo')

            await conn.execute('sudo mv /tmp/gringotts-agent.latest.rpm '
                               '/tmp/gringotts-agent.latest.rpm.old',
                               raise_when_error=False)
            await conn.execute(f'sudo wget -c -t 120 -T 20 {gringotts_repo_url_prefix}gringotts/'
                               f'gringotts-agent/latest/'
                               f'gringotts-agent.latest.rpm -P /tmp/')
            await conn.execute('sudo rpm -U --force /tmp/gringotts-agent.latest.rpm')

            await conn.execute('supervisorctl restart gringotts-agent', raise_when_error=True)

    gringotts_env = config.get('gringotts', 'gringotts_env')
    gringotts_repo_url_prefix = config.get('gringotts', 'gringotts_repo_url_prefix')
    tasks = []
    for ig in cluster.instance_groups:
        for instance in ig.instances:
            tasks.append(__inner(instance, cluster.id, gringotts_env, gringotts_repo_url_prefix))
    await asyncio.gather(*tasks)


async def restart_agent(cluster):
    async def __inner(instance):
        remote = await instance.remote()
        async with remote as conn:
            await conn.execute('supervisorctl restart gringotts-agent', raise_when_error=True)

    tasks = []
    for ig in cluster.instance_groups:
        for instance in ig.instances:
            tasks.append(__inner(instance))
    await asyncio.gather(*tasks)


async def restart_all_agent():
    async def __inner(instance):
        remote = await instance.remote()
        async with remote as conn:
            await conn.execute('supervisorctl restart gringotts-agent', raise_when_error=False)

    tasks = []

    query = model_query(ClusterModel)
    query = query.filter(ClusterModel.status != 'Deleted')
    clusters = await query.query_all()
    for cluster in clusters:
        for ig in cluster.instance_groups:
            for instance in ig.instances:
                tasks.append(__inner(instance))

    await asyncio.gather(*tasks)


async def check_exporter(cluster_id=None):
    async def __inner(instance, cluster_iid):
        rrr = None
        try:
            remote = await instance.remote()
            async with remote as conn:
                _, exp_count = await conn.execute('ps -ef | grep exporter_name | grep -v grep | wc -l')
                # _, exist_pid = await conn.execute(f'ls  /var/run/gringotts-collector.pid', raise_when_error=False)

                if int(exp_count) > 1:
                    rrr = f'Cluster {cluster_iid}, Instance {instance.instance_name}, ' \
                          f'exp count: {exp_count}.'
                    # f' pid exist: {exist_pid}.'
        except:
            cprint(f'login failed instance: {instance.instance_name}', 'red')
        return rrr

    query = model_query(ClusterModel)
    if cluster_id:
        query.filter(ClusterModel.id == cluster_id)
    clusters = await query.query_all()

    tasks = []
    for cluster in clusters:
        for ig in cluster.instance_groups:
            for instance in ig.instances:
                tasks.append(__inner(instance, cluster.id))
    results = await asyncio.gather(*tasks)
    all_results = [x for x in results if x is not None]

    for res in all_results:
        cprint(res, 'blue')


async def update_jvm_config(cluster_id=None):
    async def __inner(instance, cluster_iid):
        rrr = None
        try:
            remote = await instance.remote()
            async with remote as conn:
                _, multi_instances = await conn.execute('ls /etc/config/elasticsearch/')
                # _, exist_pid = await conn.execute(f'ls  /var/run/gringotts-collector.pid', raise_when_error=False)

                config_files = []
                for multi in multi_instances:
                    m = 0
                    try:
                        m = int(multi)
                    except:
                        pass
                    if not m:
                        continue

                    config_files.append(f'/etc/config/elasticsearch/{m}/jvm.options')

                for config_file in config_files:
                    await conn.execute(f"sed -i '$a-Dlog4j2.formatMsgNoLookups=true' {config_file}")
                    cprint(f'done {instance.instance_name}, {config_file}', 'red')
        except:
            cprint(f'login failed instance: {instance.instance_name}', 'red')
        return rrr

    query = model_query(ClusterModel)
    if cluster_id:
        query.filter(ClusterModel.id == cluster_id)
    clusters = await query.query_all()

    tasks = []
    for cluster in clusters:
        for ig in cluster.instance_groups:
            for instance in ig.instances:
                tasks.append(__inner(instance, cluster.id))
    results = await asyncio.gather(*tasks)


async def correct_components(cluster_id=None, user_id=None):
    async def update_collector(ins):
        try:
            remote = await ins.remote()
            async with remote as conn:
                await conn.execute('sudo /Application/gringotts-agent/data/script/EXPORTER/control.sh init',
                                   raise_when_error=False)
                await conn.execute("ps -ef | grep exporter | grep -v grep  | awk '{print $2}' | xargs kill",
                                   raise_when_error=False)
        except:
            pass

    qqq = model_query(ClusterModel)
    qqq.filter(ClusterModel.status != 'Deleted')
    qqq.filter(ClusterModel.cluster_type == 'KES')
    if cluster_id:
        qqq.filter(ClusterModel.id == cluster_id)
    if user_id:
        qqq.filter(ClusterModel.ksc_user_id == user_id)
    clusters = await qqq.query_all()

    if not clusters:
        cprint(f'cluster {cluster_id} not found', 'red')
        return 0

    update_tasks = []
    for cluster in clusters:
        for ig in cluster.instance_groups:
            for ins in ig.instances:
                node_id = ins.id
                name = ins.instance_name

                query = model_query(GgComponentsModel)
                query.filter(GgComponentsModel.node_id == node_id)
                components = await query.query_all()

                for component in components:
                    if component.name == '':
                        print(f'instance {name}, component: {component.name}, [redundant], delete')
                        # await component.delete(hard=True)
                    elif component.name == 'EXPORTER':
                        print(f'instance {name}, component: {component.name}, [exporter], make it right')
                        new_data = {
                            'component_key': f'{node_id}{component.name}{component.instance_id}{component.role}',
                            'scripts': [
                                {
                                    'name': 'control.sh', 'version': 'f465a44a6367bf2877fc0f4543d4f981',
                                    'path': '/Application/gringotts-agent/data/script/EXPORTER//control.sh'
                                }
                            ],
                            'scripts_version_status': 'different',
                            'recovery': {
                                'enabled': True,
                                'is_running': False,
                                'max_count': 10,
                                'window_in_minutes': 20,
                                'retry_interval_seconds': 10,
                                'recovery_script': 'control.sh',
                                'recovery_args': f'start ELASTICSEARCH {ig.multi_instance_count - 1}',
                                'recovery_user': 'root',
                                'last_exec_time': '2021-05-19T14:55:57.568613924+08:00',
                                'current_hour_retry_count': 0
                            }, 'is_maintain_mode': False
                        }
                        await component.save({'data': new_data})
                    elif component.name == 'KIBANA':
                        print(f'instance {name}, component: {component.name}, '
                              f'data:{component.data}, [kibana], make it right, pass now')
                        new_data = {
                            "component_key": f'{node_id}{component.name}{component.instance_id}{component.role}',
                            "scripts": [
                                {"name": "control.sh", "version": "9166089cad09fcb05f569cb1163eeb6e",
                                 "path": "/Application/gringotts-agent/data/script/KIBANA//control.sh"},
                                {"name": "install.sh", "version": "a2efbe6a910182a30a4041a1564e30a0",
                                 "path": "/Application/gringotts-agent/data/script/KIBANA//install.sh"}],
                            "scripts_version_status": "normal",
                            "recovery": {
                                "enabled": True,
                                "is_running": False,
                                "max_count": 10,
                                "window_in_minutes": 20,
                                "retry_interval_seconds": 10,
                                "recovery_script": "control.sh",
                                "recovery_args": "start_without_confirm",
                                "recovery_user": "kibana",
                                "last_exec_time": "2021-05-18T19:56:20.433065691+08:00",
                                "current_hour_retry_count": 0
                            }, "is_maintain_mode": False
                        }
                        await component.save({'data': new_data})
                    else:
                        print(f'instance {name}, component: {component.name}, [normal], make it right')
                        data = component.data
                        data['component_key'] = f'{node_id}{component.name}{component.instance_id}{component.role}'
                        await component.save({'data': data})

                update_tasks.append(update_collector(ins))

    await asyncio.gather(*update_tasks)


async def check_gg_cpu(cluster_id=None):
    async def __inner(instance, cluster_iid):
        ress = [instance.instance_name, cluster_iid]
        try:
            remote = await instance.remote()
            async with remote as conn:
                _, processes = await conn.execute("ps -ef | grep gring | grep -v grep | "
                                                  "grep -v bash | awk '{print $2, $8}'",
                                                  raise_when_error=False)
                for proc in processes.split('\n'):
                    if not proc:
                        continue
                    pid, _ = proc.split(' ')
                    _, pid_res = await conn.execute(f"ps -o pcpu,pmem -p {pid}", raise_when_error=False)
                    cpu, mem = [float(x) > 5.0 for x in pid_res.split('\n')[1].split(' ') if x != '']

                    if cpu > 5.0 or mem > 15.0:
                        ress.append(proc)
                        ress.append(cpu)
                        ress.append(mem)
        except Exception as e:
            cprint(f'login failed instance: {instance.instance_name}, except: {e}', 'red')
        return ress

    query = model_query(ClusterModel)
    if cluster_id:
        query.filter(ClusterModel.id == cluster_id)
    clusters = await query.query_all()

    tasks = []
    for cluster in clusters:
        for ig in cluster.instance_groups:
            for instance in ig.instances:
                tasks.append(__inner(instance, cluster.id))
    results = await asyncio.gather(*tasks)
    all_results = [{
        'cluster_id': x[1],
        'instance_name': x[0],
        'proc': x[2],
        'cpu': x[3],
        'mem': x[4],
    } for x in results if len(x) > 2]

    sort_ress = sorted(all_results, key=lambda x: x.get('cpu'))
    for res in sort_ress:
        cprint(res, 'green')


async def describe_kec_info(instance_id, product='kes'):
    if not instance_id:
        cprint('Please specify instance id', 'red')
        return

    query = model_query(ClusterModel)
    query = query.join(InstanceGroupModel) \
        .join(InstanceModel) \
        .filter(InstanceModel.instance_group_id == InstanceGroupModel.id) \
        .filter(InstanceGroupModel.cluster_id == ClusterModel.id) \
        .filter(InstanceModel.instance_id == instance_id)
    cluster = await query.query_one()

    if not cluster:
        cprint(f'Instance {instance_id} not found', 'red')
        return

    kec_client = getattr(sdk, f'kec_client_{product.lower()}')
    res = await kec_client.describe_instances(instance_ids=[instance_id],
                                              account_id=cluster.ksc_user_id)

    return res


async def reboot_kec(instance_id):
    if not instance_id:
        cprint('Please specify instance id', 'red')
        return

    query = model_query(ClusterModel)
    query = query.join(InstanceGroupModel) \
        .join(InstanceModel) \
        .filter(InstanceModel.instance_group_id == InstanceGroupModel.id) \
        .filter(InstanceGroupModel.cluster_id == ClusterModel.id) \
        .filter(InstanceModel.instance_id == instance_id)
    cluster = await query.query_one()

    if not cluster:
        cprint(f'Instance {instance_id} not found', 'red')
        return

    kec_client = getattr(sdk, f'kec_client_{cluster.cluster_type.lower()}')
    await kec_client.reboot_instances(instance_ids=[instance_id], account_id=cluster.ksc_user_id)


async def stop_kec(instance_id):
    if not instance_id:
        cprint('Please specify instance id', 'red')
        return

    query = model_query(ClusterModel)
    query = query.join(InstanceGroupModel) \
        .join(InstanceModel) \
        .filter(InstanceModel.instance_group_id == InstanceGroupModel.id) \
        .filter(InstanceGroupModel.cluster_id == ClusterModel.id) \
        .filter(InstanceModel.instance_id == instance_id)
    cluster = await query.query_one()

    if not cluster:
        cprint(f'Instance {instance_id} not found', 'red')
        return

    kec_client = getattr(sdk, f'kec_client_{cluster.cluster_type.lower()}')
    await kec_client.stop_instances(instance_ids=[instance_id], account_id=cluster.ksc_user_id)


async def start_kec(instance_id):
    if not instance_id:
        cprint('Please specify instance id', 'red')
        return

    query = model_query(ClusterModel)
    query = query.join(InstanceGroupModel) \
        .join(InstanceModel) \
        .filter(InstanceModel.instance_group_id == InstanceGroupModel.id) \
        .filter(InstanceGroupModel.cluster_id == ClusterModel.id) \
        .filter(InstanceModel.instance_id == instance_id)
    cluster = await query.query_one()

    if not cluster:
        cprint(f'Instance {instance_id} not found', 'red')
        return

    kec_client = getattr(sdk, f'kec_client_{cluster.cluster_type.lower()}')
    await kec_client.start_instances(instance_ids=[instance_id], account_id=cluster.ksc_user_id)
