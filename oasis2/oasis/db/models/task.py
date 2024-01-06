from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import JSON
from sqlalchemy import String
from sqlalchemy import Text

from oasis.db.models import OasisBase


class TaskModel(OasisBase):
    __tablename__ = 'task'

    class STATUS:
        Init = 'Init'
        Doing = 'Doing'
        Done = 'Done'
        Failed = 'Failed'
        Rolling = 'Rolling'
        Rolled = 'Rolled'
        RollFailed = 'RollFailed'

    class TYPE:
        """
        For now, different type of tasks will not send to different workers
        This is prepared for the future
        wuhsh
        """

        ALL = 'all'
        POLY = 'poly'  # calling poly api
        INNER = 'inner'  # login user vm
        PUBLIC = 'public'  # internet

    name = Column(String(80))
    status = Column(String(15), default=STATUS.Init)
    worker = Column(String(20), default=None)
    type = Column(String(10), default=TYPE.ALL)
    job_id = Column(String(36), ForeignKey('job.id', ondelete='CASCADE'))
    next_tasks: list = Column(JSON)
    args: dict = Column(JSON, default={})
    results: dict = Column(JSON, default={})
    rollback_on_fail = Column(Boolean, default=0)
    info = Column(Text)
