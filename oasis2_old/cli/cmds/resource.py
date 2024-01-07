import asyncio
import json
import os

from nubia import argument
from nubia import command
from prettytable import PrettyTable
from termcolor import cprint

from cli.methods.job import find_error_job_by_cluster
from cli.methods.job import find_running_job_by_cluster
from cli.methods.resource import check_exporter
from cli.methods.resource import check_gg_cpu
from cli.methods.resource import correct_components
from cli.methods.resource import describe_kec_info
from cli.methods.resource import fuzzy_query_cluster
from cli.methods.resource import get_ssh_cmd
from cli.methods.resource import refresh_agent
from cli.methods.resource import update_jvm_config
from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel


@command
class Cluster:
    """
        Cluster commands
    """

    @command()
    @argument('cluster_id', description='Cluster uuid', positional=True)
    def describe(self, cluster_id: str):
        """
            Describe cluster with uuid
        """
        cprint(f'Cluster Id: {cluster_id}', 'green')

        cluster = asyncio.run(get_model_by_id(ClusterModel, cluster_id))

        if not cluster:
            cprint(f'Cluster {cluster_id} not found', 'red')
            return 1

        job_id = asyncio.run(find_running_job_by_cluster(cluster_id))
        cprint(f'Running job : {job_id}', 'red')

        error_jobs = asyncio.run(find_error_job_by_cluster(cluster_id))
        error_job_table = PrettyTable(['Job Id', 'Name'])

        if error_jobs:
            for error_job in error_jobs:
                error_job_table.add_row([error_job.id, error_job.name])

        cluster_table = PrettyTable(['Type', 'ClusterId', 'Name', 'Status', 'AccountId'])
        cluster_table.add_row([cluster.cluster_type, cluster.id, cluster.name, cluster.status, cluster.ksc_user_id])

        ins_table = PrettyTable(['Instance Group Id', 'Type', 'Id', 'Instance Id',
                                 'Name', 'Resource Type', 'EIP', 'NIC'])
        for ig in cluster.instance_groups:
            for instance in ig.instances:
                ins_table.add_row([ig.id, ig.instance_group_type,
                                   instance.id, instance.instance_id, instance.instance_name,
                                   ig.resource_type, instance.inner_eip, instance.internal_ip])

        if error_jobs:
            cprint('Error Jobs: ', 'cyan')
            cprint(error_job_table, 'white')
        cprint('Cluster: ', 'cyan')
        cprint(cluster_table, 'white')
        cprint('Instances:', 'cyan')
        cprint(ins_table, 'white')

        return 0

    @command()
    @argument('keyword', description='Query keyword')
    @argument('status', description='Cluster status', choices=['NotDeleted', 'Active', 'Deleted', 'Error'])
    @argument('account_id', description='Account Id')
    def list_all(self, keyword: str = None, status: str = 'NotDeleted',
                 account_id: str = None, **kwargs):
        """
            List cluster with keyword
        """

        if keyword:
            kwargs.setdefault('keyword', keyword)

        kwargs.setdefault('status', status)

        if account_id:
            kwargs.setdefault('account_id', account_id)

        cprint(f'List clusters: {kwargs}', 'green')

        clusters = asyncio.run(fuzzy_query_cluster(**kwargs))
        cluster_table = PrettyTable(['No.', 'ClusterId', 'Type', 'Name', 'Status', 'AccountId'])
        for no, cluster in enumerate(clusters, 1):
            cluster_table.add_row([no, cluster.id, cluster.cluster_type, cluster.name,
                                   cluster.status, cluster.ksc_user_id])

        cprint(f'Clusters: ', 'white')
        cprint(cluster_table, 'white')

        return 0

    @command()
    @argument('cluster_id', description='Cluster uuid', positional=True)
    def refresh_agent(self, cluster_id: str):
        """
            Refresh cluster gg agent
        """
        cprint(f'Cluster Id: {cluster_id}', 'green')

        cluster = asyncio.run(get_model_by_id(ClusterModel, cluster_id))
        asyncio.run(refresh_agent(cluster))

    @command()
    @argument('cluster_id', description='Cluster uuid')
    def check_exporter(self, cluster_id: str = None):
        """
            Check exporter
        """
        cprint(f'Cluster Id: {cluster_id}', 'green')

        asyncio.run(check_exporter(cluster_id))

    @command()
    @argument('cluster_id', description='Cluster uuid')
    def check_gg_cpu(self, cluster_id: str = None, process_name: str = None):
        """
            Check gringotts cpu usage
        """
        cprint(f'Cluster Id: {cluster_id}', 'green')
        cprint(f'Process name: {process_name}', 'green')

        asyncio.run(check_gg_cpu(cluster_id))

    @command()
    @argument('cluster_id', description='Cluster uuid')
    @argument('user_id', description='User uuid')
    def correct_components(self, cluster_id: str = None, user_id: str = None):
        """
            Correct gg_components
        """
        cprint(f'Cluster Id: {cluster_id}', 'green')

        asyncio.run(correct_components(cluster_id, user_id))

    @command()
    @argument('cluster_id', description='Cluster uuid')
    def update_jvm_config(self, cluster_id: str = None):
        """
            Update JVM Config
        """
        cprint(f'Cluster Id: {cluster_id}', 'green')

        asyncio.run(update_jvm_config(cluster_id))


@command
class Instance:
    """
        Instance commands
    """

    @command()
    @argument('keyword', description='Query keyword', positional=True)
    def login(self, keyword: str):
        """
            List cluster with keyword
        """

        instance, ssh_cmd = asyncio.run(get_ssh_cmd(keyword))
        if not instance:
            cprint(f'Instance with keyword {keyword} not found.')
            return 0
        cprint(f'Instance Id: {instance.instance_id}, Name: {instance.instance_name}, '
               f'Nic: {instance.internal_ip}, Eip: {instance.inner_eip}', 'green')
        cprint(ssh_cmd, 'green')
        os.system(ssh_cmd)
        path = ssh_cmd.split(' ')[-1]
        os.remove(path)

    @command()
    @argument('instance_id', description='Instance Id', positional=True)
    @argument('product', description='Product', choices=['kes', 'khbase'])
    def kec_info(self, instance_id: str, product: str = 'kes'):
        """
            Describe instance kec info
        """

        instance_info = asyncio.run(describe_kec_info(instance_id, product=product))
        if not instance_info:
            cprint('Get instance kec info failed', 'red')
            return

        cprint(f'Instance kec info: {json.dumps(instance_info, indent=2)}', 'green')
