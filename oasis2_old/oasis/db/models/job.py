from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy.orm import relationship

from oasis.db.models import OasisBase


class JobModel(OasisBase):
    __tablename__ = 'job'

    class STATUS:
        Init = 'Init'
        Doing = 'Doing'
        Done = 'Done'
        Error = 'Error'
        Rolling = 'Rolling'
        Rolled = 'Rolled'

    name = Column(String(80))
    status = Column(String(10))
    cluster_id = Column(String(36))
    parent_job = Column(String(36), default=None)

    tasks = relationship('TaskModel',
                         cascade='all,delete',
                         backref='job',
                         lazy='joined')
