import asyncssh
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import String
from sqlalchemy import UniqueConstraint

from oasis.db.models import OasisBase
from oasis.db.models import get_model_by_id
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.instance_group import InstanceGroupModel
from oasis.utils.config import config
from oasis.utils.remote import Remote


class InstanceModel(OasisBase):
    """An OpenStack instance created for the cluster."""

    __tablename__ = 'instances'

    __table_args__ = (
        UniqueConstraint('instance_id', 'instance_group_id'),
    )

    class STATUS:
        ACTIVE = 'Active'
        STOPPED = 'Stopped'
        DELETING = 'Deleting'
        DELETED = 'Deleted'

    instance_id = Column(String(36), nullable=False)
    instance_name = Column(String(80), nullable=False)
    instance_group_id = Column(String(36), ForeignKey('instance_groups.id', ondelete='CASCADE'))
    internal_ip = Column(String(15))
    management_ip = Column(String(15))
    management_ip_line = Column(String(15), default='BGP')
    management_ip_type = Column(String(15))
    inner_eip = Column(String(15))
    volumes = Column(JSON)
    cpus = Column(Integer)
    ram = Column(Integer)
    host_name = Column(String(80))
    slb_register_id = Column(JSON)
    service_instance_id = Column(String(36))
    status = Column(String(20))
    inner_manager_ip = Column(String(15))
    data_guard_id = Column(String(36), default=None)
    allocate_address_id = Column(String(36))

    async def remote(self):
        instance_group_id = self.instance_group_id
        instance_group = await get_model_by_id(InstanceGroupModel, instance_group_id)
        cluster_id = instance_group.cluster_id
        cluster = await get_model_by_id(ClusterModel, cluster_id)
        pri_key = asyncssh.import_private_key(cluster.management_private_key)
        ssh_port = config.getint('vpc', 'ssh_port')
        return Remote(self.inner_eip, ssh_port, pri_key, instance_name=self.instance_name, cluster_id=cluster_id)
