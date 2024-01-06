from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy.orm import relationship

from oasis.db.models import OasisBase


class ClusterModel(OasisBase):
    class STATUS:
        SPAWNING = 'Spawning'
        WAITING = 'Waiting'
        CONFIGURING = 'Configuring'
        STARTING = 'Starting'
        ACTIVE = 'Active'
        SCALE_OUT_WAITING = 'ScaleOutWaiting'
        SCALE_OUT_PREPARING = 'ScaleOutPreparing'
        SCALE_OUT_CONFIGURING = 'ScaleOutConfiguring'
        UPGRADING = 'Upgrading'
        SCALE_IN_WAITING = 'ScaleInWaiting'
        SCALE_IN_PREPARING = 'ScaleInPreparing'
        PROGRESSING = 'Progressing'
        FREEZE = 'Freeze'
        FREEZING = 'Freezing'
        UNFREEZING = 'Unfreezing'
        DELETING = 'Deleting'
        DELETED = 'Deleted'
        ERROR = 'Error'

    __tablename__ = 'clusters'

    name = Column(String(80), nullable=False)
    description = Column(Text)
    cluster_type = Column(String(20))
    distribution_version = Column(String(80), nullable=False)
    main_version = Column(String(80), nullable=False)
    region = Column(String(80))
    availability_zone = Column(String(80))
    image_id = Column(String(36))
    anti_affinity = Column(JSON)
    management_private_key = Column(Text, nullable=False)
    management_public_key = Column(Text, nullable=False)
    management_keypair_id = Column(String(100), nullable=True)
    security_group_id = Column(String(100), nullable=True)
    status = Column(String(80))
    status_description = Column(Text)
    extra = Column(JSON)
    rollback_info = Column(JSON)
    instance_groups = relationship('InstanceGroupModel',
                                   cascade="all,delete",
                                   backref='cluster',
                                   lazy='joined',
                                   primaryjoin='and_(ClusterModel.id==InstanceGroupModel.cluster_id, '
                                               'InstanceGroupModel.status!="Deleted")')
    is_terminate_protection = Column(Boolean(), default=False)
    install_apps = Column(JSON)
    enable_eip = Column(Boolean, default=False)
    enable_private_slb = Column(Boolean, default=False)
    eip_line_id = Column(String(36))
    eip_bandwidth = Column(Integer())
    slb_id = Column(String(36))
    activated_at = Column(DateTime())
    terminated_at = Column(DateTime())
    ks3_credential = Column(String(256))
    vpc_domain_id = Column(String(36))
    vpc_subnet_id = Column(String(36))
    vpc_endpoint_id = Column(String(36))
    charge_type = Column(String(80))
    expire_time = Column(DateTime())
    purchase_time = Column(Integer())
    tenant_id = Column(String(36))
    ksc_user_id = Column(String(36))
    ksc_sub_user_id = Column(String(36))
    order_id = Column(String(36))
    es_plugins = relationship('EsPluginModel',
                              cascade="all,delete",
                              backref='cluster',
                              lazy='joined',
                              primaryjoin='and_(ClusterModel.id==EsPluginModel.cluster_id,'
                              'EsPluginModel.status!=4)')
    tags = Column(JSON)
    tag_keys = Column(Text)
