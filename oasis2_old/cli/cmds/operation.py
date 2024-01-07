import asyncio

from nubia import argument
from nubia import command
from termcolor import cprint

from cli.methods.operation import add_user
from cli.methods.order import upgrade_instance_group
from cli.methods.resource import reboot_kec
from cli.methods.resource import restart_agent
from cli.methods.resource import restart_all_agent
from cli.methods.resource import start_kec
from cli.methods.resource import stop_kec
from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel


@command
class Operation:
    """
        Operation commands
    """

    @command
    @argument('kuser_id', description='Ksc user id')
    @argument('company_alias', description='Alias')
    @argument('user_level', description='User level, P0 (High) - P999 (Low)')
    @argument('total_virtual_cpu', description='total_virtual_cpu')
    @argument('total_mem_mb', description='total_mem_mb')
    @argument('total_disk_gb', description='total_disk_gb')
    def add_user(self, kuser_id: str, company_alias: str = '', user_level: str = '',
                 total_virtual_cpu: int = 400, total_mem_mb: int = 1000000, total_disk_gb: int = 60000):
        """
            Add user
        """

        asyncio.run(add_user(kuser_id, company_alias=company_alias,
                             user_level=user_level, total_virtual_cpu=total_virtual_cpu,
                             total_mem_mb=total_mem_mb, total_disk_gb=total_disk_gb))

        return 0

    @command
    @argument('cluster_id', description='Cluster uuid')
    @argument('instance_group_id', description='Instance Group uuid')
    @argument('display_region', description='Display Region in order(北京6区(VPC))')
    @argument('display_az', description='Display Az in order(北京6区(VPC)-可用区C)')
    @argument('kec_order_type', description='S3.8B')
    @argument('kes_instance_type', description='ES.basic.8C16G')
    @argument('product_batch', description='Same with previous order')
    def upgrade_instance_group(self, cluster_id: str, instance_group_id: str, display_region: str, display_az: str,
                               kec_order_type: str, kes_instance_type: str, product_batch: str):
        """
            Upgrade instance group (Create upgrade product and create order)
        """
        asyncio.get_event_loop().run_until_complete(
            upgrade_instance_group(cluster_id, instance_group_id, display_region, display_az, kec_order_type,
                                   kes_instance_type,
                                   product_batch))

    @command
    @argument('instance_id', description='Instance uuid', positional=True)
    def reboot_kec(self, instance_id: str):
        """
            Reboot kec instance
        """
        asyncio.get_event_loop().run_until_complete(reboot_kec(instance_id))

    @command
    @argument('instance_id', description='Instance uuid', positional=True)
    def stop_kec(self, instance_id: str):
        """
            Stop kec instance
        """
        asyncio.get_event_loop().run_until_complete(stop_kec(instance_id))

    @command
    @argument('instance_id', description='Instance uuid', positional=True)
    def start_kec(self, instance_id: str):
        """
            Stop kec instance
        """
        asyncio.get_event_loop().run_until_complete(start_kec(instance_id))

    @command()
    @argument('cluster_id', description='Cluster uuid', positional=True)
    def restart_agent(self, cluster_id: str):
        """
            Restart cluster gg agent
        """
        if cluster_id == 'all':
            cprint(f'Cluster All!', 'green')
            asyncio.run(restart_all_agent())

        else:
            cprint(f'Cluster Id: {cluster_id}', 'green')
            cluster = asyncio.run(get_model_by_id(ClusterModel, cluster_id))
            asyncio.run(restart_agent(cluster))
