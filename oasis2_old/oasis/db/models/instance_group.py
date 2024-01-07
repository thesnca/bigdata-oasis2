from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import relationship

from oasis.db.models import OasisBase


class InstanceGroupModel(OasisBase):
    class STATUS:
        ACTIVE = 'Active'
        DELETED = 'Deleted'

    __tablename__ = 'instance_groups'

    __table_args__ = (
        UniqueConstraint('name', 'cluster_id'),
    )

    name = Column(String(80), nullable=False)
    resource_type = Column(String(36))
    instance_type_code = Column(String(36))
    instance_group_type = Column(String(36))
    image_id = Column(String(36))
    resource_attr = Column(JSON)
    count = Column(Integer, nullable=False)
    dest_count = Column(Integer, nullable=False)
    cluster_id = Column(String(36), ForeignKey('clusters.id', ondelete='CASCADE'))
    availability_zone = Column(String(255))
    vpc_domain_id = Column(String(36))
    vpc_subnet_id = Column(String(36))
    system_volume_type = Column(String(36))
    system_volume_size = Column(Integer, default=0)
    volume_type = Column(String(36))
    volume_count = Column(Integer, default=0)
    volume_size = Column(Integer, default=0)
    order_id = Column(String(36))
    status = Column(String(80))
    multi_instance_count = Column(Integer, default=1)
    instances = relationship('InstanceModel',
                             cascade="all,delete",
                             backref='instance_group',
                             order_by='InstanceModel.instance_name',
                             lazy='joined',
                             primaryjoin='and_(InstanceGroupModel.id==InstanceModel.instance_group_id, '
                                         'InstanceModel.status!="Deleted")')
