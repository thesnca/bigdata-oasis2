from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import UniqueConstraint

from oasis.db.models import OasisBase


class UserModel(OasisBase):
    """
    users includes admin and customer
    """
    class ROLE:
        ADMIN = 'quota-admin'
        NORMAL = 'normal_user'

    __tablename__ = 'users'

    __table_args__ = (
        UniqueConstraint('tenant_id'),
    )

    tenant_id = Column(String(36))
    total_virtual_cpu = Column(BigInteger, default=0)
    total_mem_mb = Column(BigInteger, default=0)
    total_disk_gb = Column(BigInteger, default=0)
    allocated_virtual_cpu = Column(BigInteger, default=0)
    allocated_mem_mb = Column(BigInteger, default=0)
    allocated_disk_gb = Column(BigInteger, default=0)
    lifespan = Column(BigInteger, default=0)
    role = Column(String(16), default='normal_user')
    allocator = Column(String(36))
    description = Column(Text())
    extra = Column(String(128))
    expire_time = Column(DateTime())
    company_alias = Column(String(256), default=None)
    user_level = Column(String(36), default=None)
