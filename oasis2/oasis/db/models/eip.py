from sqlalchemy import Column
from sqlalchemy import JSON
from sqlalchemy import String
from sqlalchemy.sql.sqltypes import Integer

from oasis.db.models import OasisBase


class EIPModel(OasisBase):
    """
    eip info of clusters
    """
    __tablename__ = 'eip_infos'

    class STATUS:
        BINDED = 'Binded'
        UNBINDED = 'Unbinded'
        DELETED = 'Deleted'

    cluster_id = Column(String(36), nullable=False, primary_key=True)
    load_balancer_id = Column(String(36))
    # 0：classic 1:private
    load_balancer_type = Column(Integer())
    listener_id = Column(JSON)
    health_check_id = Column(JSON)
    # 20211216 这是一个后加字段。从instance表迁过来的，
    # 适应多port+多instance，结构做了调整
    # 目前只做记录用，没有迁移历史数据
    # 释放/解绑时，通过listener by slb，直接取真值。
    # 后续instance.register_id 不在使用。
    register_id = Column(JSON)
    allocate_address_id = Column(String(36), nullable=False, primary_key=True)
    eip_address = Column(String(15))
    eip_line_id = Column(String(36))
    eip_charge_type = Column(String(36))
    eip_order_id = Column(String(36))
    status = Column(String(20))
