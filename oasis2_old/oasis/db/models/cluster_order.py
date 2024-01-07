from sqlalchemy import Column
from sqlalchemy import JSON
from sqlalchemy import String

from oasis.db.models import OasisBase


class ClusterOrderModel(OasisBase):
    """
    order infos of clusters
    """
    __tablename__ = 'cluster_orders'

    order_id = Column(String(108), nullable=False, primary_key=True)
    cluster_id = Column(String(36), nullable=False)
    instance_group_id = Column(String(36), nullable=False)
    instance_id = Column(String(36), nullable=False, primary_key=True)
    data = Column(JSON)



