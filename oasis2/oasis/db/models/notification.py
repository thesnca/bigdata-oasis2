from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import UniqueConstraint

from oasis.db.models import OasisBase


class NotificationModel(OasisBase):
    """
        Cubrick notification model
    """

    __tablename__ = 'notification'

    __table_args__ = (
        UniqueConstraint('cluster_id', 'url'),
    )

    cluster_id = Column(String(36))
    url = Column(String(256))
    token = Column(String(80))
