import asyncio
from uuid import uuid4

from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.utils.config import base_nginx_conf, base_gringotts_repo
from oasis.utils.config import config
from oasis.utils.generator import generate_instance_hosts
from oasis.utils.logger import logger
from oasis.worker.tasks import BaseTask
from oasis.worker.tasks import check_rollback
from oasis.worker.tasks import check_task


class TaskInstallGringottsAgent(BaseTask):
    @check_task
    async def run(self):
        cluster_id = self.args.get('cluster_id', None)
        new_instance_ids = self.args.get('new_instance_ids', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        gringotts_repo_url_prefix = config.get('gringotts', 'gringotts_repo_url_prefix')
        tasks = []
        for ig in cluster.instance_groups:
            for instance in ig.instances:
                if new_instance_ids and instance.instance_id not in new_instance_ids:
                    continue
                logger.info(self, f'Remote install gringotts on new instance {instance.id}, start.')
                tasks.append(self.remote_install_gringotts_agent(instance, gringotts_repo_url_prefix))

        await asyncio.gather(*tasks)

    @check_rollback
    async def rollback(self):
        return True

    async def remote_install_gringotts_agent(self, instance, gringotts_repo_url_prefix):
        logger.info(self, f'Remote install gringotts on new instance {instance.id}, start.')
        remote = await instance.remote()

        async with remote as conn:
            # install nginx
            await conn.execute(f'rm -rf /tmp/add_kes_nginx.sh;sudo wget -N -t 120 -T 20 {gringotts_repo_url_prefix}'
                               f'third-software/nginx/add_kes_nginx.sh -P /tmp/')
            await conn.execute(f'sudo sh /tmp/add_kes_nginx.sh {gringotts_repo_url_prefix}third-software/nginx/nginx.tar.gz >> /tmp/install_nginx.log', raise_when_error=False)

            # install gg agent/collector & supervisor
            await conn.execute('sudo mv /usr/local/src/gringotts-agent.latest.rpm '
                               '/usr/local/src/gringotts-agent.latest.rpm.old',
                               raise_when_error=False)
            await conn.execute(f'sudo wget -c -t 120 -T 20 {gringotts_repo_url_prefix}gringotts/'
                               f'gringotts-agent/latest/'
                               f'gringotts-agent.latest.rpm -P /tmp/')
            await conn.execute(f'sudo wget -c -t 120 -T 20 {gringotts_repo_url_prefix}gringotts/'
                               f'gringotts-agent/latest/'
                               f'gringotts-collector.latest.rpm -P /tmp/')
            await conn.execute('sudo touch /etc/nodeinfo')
            await conn.execute('sudo rpm -U --force /tmp/gringotts-agent.latest.rpm')
            await conn.execute('sudo rpm -U --force /tmp/gringotts-collector.latest.rpm')
            await conn.execute('sudo yum clean all')
            await conn.execute('sudo yum makecache')
            await conn.execute(f'rm -rf /tmp/supervisor.sh;sudo wget -N -t 120 -T 20 {gringotts_repo_url_prefix}'
                               f'third-software/supervisor/supervisor-kes.sh  -P /tmp/')
            await conn.execute(f'sudo sh /tmp/supervisor-kes.sh {gringotts_repo_url_prefix} >> /tmp/install_supervisor.log', raise_when_error=False)
            
            # force agent restart again, guarantee nodeinfo is loaded
            await conn.execute('supervisorctl restart gringotts-agent', raise_when_error=True)
            await conn.execute('supervisorctl restart gringotts-collector', raise_when_error=False)

            # install jdk
            await conn.execute(f'rm -rf /tmp/jdk.tar.gz;sudo wget -N -t 120 -T 20 {gringotts_repo_url_prefix}'
                               f'third-software/jdk/jdk.tar.gz  -P /tmp/')
            await conn.execute('tar -zxf /tmp/jdk.tar.gz -C /mnt/')
            await conn.execute(f'sed -i "/export JAVA_HOME/d" /etc/profile')
            await conn.execute(f'echo "export JAVA_HOME=/mnt/jdk" >> /etc/profile')
            await conn.execute(f'sed -i "/export CLASSPATH/d" /etc/profile')
            await conn.execute(f'echo "export CLASSPATH=.:\$JAVA_HOME/lib/dt.jar:\$JAVA_HOME/lib/tools.jar:\$JAVA_HOME/jre/lib/rt.jar" >> /etc/profile')
            await conn.execute(f'sed -i "/export PATH/d" /etc/profile')
            await conn.execute(f'echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> /etc/profile')
            await conn.execute(f'source /etc/profile')

        logger.info(self, f'Remote install gringotts on new instance {instance.id}, finished.')


class TaskConfigHostname(BaseTask):
    @check_task
    async def run(self):
        cluster_id = self.args.get('cluster_id', None)
        new_instance_ids = self.args.get('new_instance_ids', None)
        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        old_instances = []
        new_instances = []
        for ig in cluster.instance_groups:
            # New Cluster
            if not new_instance_ids:
                new_instances.extend(ig.instances)
                continue
            # Old Cluster, Scale in
            for instance in ig.instances:
                if instance.instance_id in new_instance_ids:
                    new_instances.append(instance)
                else:
                    old_instances.append(instance)

        gringotts_env = config.get('gringotts', 'gringotts_env')

        test_env = config.getboolean('oasis', 'test_env', fallback=True)
        total_hosts_file = generate_instance_hosts(old_instances + new_instances, test_env=test_env)
        new_hosts_file = generate_instance_hosts(new_instances)

        new_instance_tasks = []
        old_instance_tasks = []

        # For all new instance
        for instance in new_instances:
            new_instance_tasks.append(self.remote_config_new_instance(instance, total_hosts_file,
                                                                      cluster_id, gringotts_env))

        # For all old instances
        for instance in old_instances:
            old_instance_tasks.append(self.remote_config_old_instance(instance, new_hosts_file,
                                                                      cluster_id, gringotts_env))

        await asyncio.gather(*new_instance_tasks, *old_instance_tasks)

    @check_rollback
    async def rollback(self):
        return True

    async def remote_config_new_instance(self, instance, total_hosts_file, cluster_id, gringotts_env):
        instance_group_id = instance.instance_group_id
        instance_id = instance.id
        instance_fqdn = f'{instance.instance_name}.ksc.com'
        logger.info(self, f'Start remote config hostname on new instance {instance_id}, {instance_fqdn}.')

        remote = await instance.remote()
        async with remote as conn:
            # region 这是一个补丁，用来修复KES端口被通信端口占用的问题
            # 在合并入KMR之后，这个补丁应当被去除。
            # 目前银河KES也用上了userdata脚本，所以下面代码合并入userdata脚本，可以去掉了...
            # await conn.execute(f'echo -e "\n# KMR jmx_exporter'
            #                    '\nnet.ipv4.ip_local_reserved_ports = 1320,8633,9000-9500\n"'
            #                    ' >> /etc/sysctl.conf && sysctl -p;')

            # endregion
            await conn.write_file('etc-hosts', total_hosts_file)
            await conn.execute(f'sudo hostname {instance_fqdn}')
            await conn.execute(f'sudo echo {instance_fqdn} > /etc/hostname ')
            await conn.execute('sudo mv etc-hosts /etc/hosts')

            await conn.execute('sudo usermod -s /bin/bash $USER')

            _nodeinfo = []
            _nodeinfo.append(f"node_id\t{instance_id}")
            _nodeinfo.append(f"node_group_id\t{instance_group_id}")
            _nodeinfo.append(f"cluster_id\t{cluster_id}")
            _nodeinfo.append(f"env {gringotts_env}")

            await conn.write_file('etc-nodeinfo', "\n".join(_nodeinfo))
            await conn.execute('sudo mv -f etc-nodeinfo /etc/nodeinfo')

            # 替换为公有云镜像，KEC预装不在提供supervisorctl。需要先执行TaskInstallGringottsAgent
            # await asyncio.sleep(10)
            # await conn.execute('supervisorctl restart gringotts-agent', raise_when_error=True)

            # # update nginx conf
            # await conn.write_file('nginx-conf', base_nginx_conf)
            # await conn.execute('sudo mv nginx-conf /etc/nginx/nginx.conf')
            # await conn.execute('sudo nginx -s reload')

            # update gringotts.repo for yum repos
            await conn.write_file('gringotts-repo', base_gringotts_repo)
            await conn.execute('sudo mv gringotts-repo /etc/yum.repos.d/gringotts.repo')

        logger.info(self, f'Finish remote config hostname on new instance {instance_id}, {instance_fqdn}.')

    async def remote_config_old_instance(self, instance, new_hosts_file, cluster_id, gringotts_env):
        instance_group_id = instance.instance_group_id
        instance_id = instance.id
        instance_fqdn = f'{instance.instance_name}.ksc.com'
        logger.info(self, f'Start remote config hostname on old instance {instance_id}, {instance_fqdn}.')
        remote = await instance.remote()
        async with remote as conn:
            await conn.write_file('etc-hosts1', new_hosts_file)
            await conn.execute(
                'sudo cat /etc/hosts >> etc-hosts1;sudo sort -k 2 -u etc-hosts1 > /etc/hosts')
            await conn.execute('sudo rm -f etc-hosts1')

            await conn.execute('sudo usermod -s /bin/bash $USER')

            _nodeinfo = []
            _nodeinfo.append(f"node_id\t{instance_id}")
            _nodeinfo.append(f"node_group_id\t{instance_group_id}")
            _nodeinfo.append(f"cluster_id\t{cluster_id}")
            _nodeinfo.append(f"env {gringotts_env}")

            await conn.write_file('etc-nodeinfo', "\n".join(_nodeinfo))
            await conn.execute('sudo mv -f etc-nodeinfo /etc/nodeinfo')
        logger.info(self, f'Finish remote config hostname on old instance {instance_id}, {instance_fqdn}.')


class TaskConfigNic(BaseTask):
    @check_task
    async def run(self):
        cluster_id = self.args.get('cluster_id', None)
        instance_ids = self.args.get('instance_ids', [])
        nic = self.args.get('nic', None)
        routes = self.args.get('routes', [])

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        new_instances = []
        for ig in cluster.instance_groups:
            for instance in ig.instances:
                if instance.instance_id in instance_ids:
                    new_instances.append(instance)

        new_instance_tasks = []

        for instance in new_instances:
            new_instance_tasks.append(self.remote_config_nic(instance, nic, routes))

        await asyncio.gather(*new_instance_tasks)

    @check_rollback
    async def rollback(self):
        return True

    async def remote_config_nic(self, instance, nic, routes):
        instance_id = instance.instance_id
        logger.info(self, f'Start remote config nic on instance {instance_id}, '
                          f'nic: {nic}, routes: {routes}.')
        remote = await instance.remote()
        async with remote as conn:
            # TODO
            _, exist_nic = await conn.execute(f'ls  /etc/sysconfig/network-scripts/ifcfg-{nic}', raise_when_error=False)
            if not exist_nic:
                nic_conf = f"""DEVICE={nic}
BOOTPROTO=dhcp
ONBOOT=yes
TYPE=Ethernet 
"""

                await conn.write_file(f'ifcfg-{nic}', nic_conf)
                await conn.execute(f'sudo mv -f ifcfg-{nic} /etc/sysconfig/network-scripts/')

            route_conf = '\n'.join([f'{route.get("cidr", "None")} via {route.get("gateway", "None")} dev {nic}'
                                    for route in routes])
            await conn.write_file(f'route-{nic}', route_conf)
            await conn.execute(f'sudo mv -f route-{nic} /etc/sysconfig/network-scripts/')

            await conn.execute(f'ifdown {nic}')
            await conn.execute(f'ifup {nic}')

        logger.info(self, f'Finish remote config nic on instance {instance_id}, '
                          f'nic: {nic}, cidr: {cidr}, gateway: {gateway}.')


class TaskAddIptablesRules(BaseTask):
    @check_task
    async def run(self):
        cluster_id = self.args.get('cluster_id', None)
        instances = self.args.get('instances', [])

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        instance_rule_dict = {
            instance_rule.get('id', None): instance_rule.get('rules', [])
            for instance_rule in instances
        }

        new_instance_tasks = []
        for ig in cluster.instance_groups:
            for instance in ig.instances:
                instance_id = instance.instance_id
                if instance_id not in instance_rule_dict:
                    continue
                new_instance_tasks.append(self.remote_add_iptables_rule(instance, instance_rule_dict.get(instance_id)))

        await asyncio.gather(*new_instance_tasks)

    @check_rollback
    async def rollback(self):
        return True

    async def remote_add_iptables_rule(self, instance, rules: list):
        instance_id = instance.instance_id
        logger.info(self, f'Start remote add iptables rule on instance {instance_id}, rules: {rules}.')
        remote = await instance.remote()
        async with remote as conn:
            # Enable nat
            await conn.execute(f'sed -i "/net.ipv4.ip_forward = /d" /etc/sysctl.conf')
            await conn.execute(f'echo "net.ipv4.ip_forward = 1" >> /etc/sysctl.conf')
            await conn.execute(f'sysctl -p')

            _, ret = await conn.execute(f'iptables-save')
            if not ret:  # first time enable iptables, write default conf
                await conn.execute(f'service iptables start')
                iptables_conf = f'''
*nat
:PREROUTING ACCEPT [0:0]
:INPUT ACCEPT [0:0]
:OUTPUT ACCEPT [0:0]
:POSTROUTING ACCEPT [0:0]
COMMIT
*filter
:INPUT ACCEPT [0:0]
:FORWARD ACCEPT [0:0]
:OUTPUT ACCEPT [0:0]
-A INPUT -m state --state RELATED,ESTABLISHED -j ACCEPT
COMMIT
'''
                tmp_iptables_file = f'iptables-{uuid4()}'
                await conn.write_file(f'/tmp/{tmp_iptables_file}', iptables_conf)
                await conn.execute(f'iptables-restore < /tmp/{tmp_iptables_file}')

            for rule in rules:
                await conn.execute(f'iptables -t nat {rule}')

        logger.info(self, f'Finish remote add iptables rule on instance {instance_id}, rule: {rule}.')
