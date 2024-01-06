from datetime import datetime

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy.orm import attributes
from sqlalchemy.sql import functions

from oasis.db.service import mysql_client
from oasis.utils.convert import datetime2str
from oasis.utils.generator import gen_uuid4


class TimestampMixin:
    id = Column(String(36), primary_key=True, default=gen_uuid4)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ModelBase(TimestampMixin):
    """Base class for models."""

    __table_initialized__ = False

    class STATE:
        CREATING = 'Creating'
        ACTIVE = 'Active'
        DELETING = 'Deleting'
        DELETED = 'Deleted'
        ERROR = 'Error'

    async def save(self, values=None):
        """Save this object."""
        db_value = None
        if self.id:
            db_value = await get_model_by_id(self.__class__, self.id)
        if db_value:
            if not values:
                values = self.to_dict()
            else:
                values = {k: v for k, v in values.items()
                          if hasattr(self.__table__.columns, k)}
            await mysql_client.update_one(self, values)
            res = await get_model_by_id(self.__class__, self.id)
        else:
            res = await mysql_client.insert_one(self)
        return res

    async def delete(self, hard=False):
        if hard or not hasattr(self.__table__.columns, 'status'):
            return await mysql_client.delete_one(self)
        values = {
            'status': self.STATE.DELETED,
        }
        await mysql_client.update_one(self, values)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __contains__(self, key):
        return hasattr(self, key)

    def get(self, key, default=None):
        return getattr(self, key, default)

    def update(self, values):
        """Make the model object behave like a dict."""
        for k, v in values.items():
            setattr(self, k, v)

    def iteritems(self):
        """Make the model object behave like a dict.

        Includes attributes from joins.
        """
        local = {k: v for k, v in self.__dict__.items()
                 if not k[0] == '_'}
        return local.items()

    def to_dict(self, **kwargs):
        """sqlalchemy based automatic to_dict method."""
        d = {}

        # if a column is unloaded at this point, it is
        # probably deferred. We do not want to access it
        # here and thereby cause it to load...
        unloaded = attributes.instance_state(self).unloaded

        for col in self.__table__.columns:
            if col.name not in unloaded:
                val = getattr(self, col.name)
                if isinstance(val, datetime):
                    val = datetime2str(val)
                d[col.name] = val

        return d


class Selection:
    def __init__(self, model):
        self.model = model
        self.select = select(model)
        self.count_select = select(functions.count(model.id))

    def __getattr__(self, func):
        def __inner(*args, **kwargs):
            self.select = getattr(self.select, func)(*args, **kwargs)
            self.count_select = getattr(self.count_select, func)(*args, **kwargs)
            return self

        async def __query(*args, **kwargs):
            # Aware of the default limit!
            count = kwargs.pop('count', False)
            limit = kwargs.pop('limit', 1000)
            offset = kwargs.pop('offset', 0)
            self.select = self.select.offset(offset).limit(limit)
            res = await getattr(mysql_client, func)(self.select,
                                                    *args, **kwargs)
            if count:
                total = await mysql_client.count(self.count_select)
                return total, res
            return res

        async def __count(*args, **kwargs):
            return await mysql_client.count(self.count_select)

        if func == 'count':
            return __count
        elif func in ['query_one', 'query_all']:
            return __query
        return __inner


def model_query(model) -> object:
    return Selection(model)


def in_filter(query, cls, search_opts):
    """Add 'in' filters for specified columns.

    Add a sqlalchemy 'in' filter to the query for any entry in the
    'search_opts' dict where the key is the name of a column in
    'cls' and the value is a tuple.

    This allows the value of a column to be matched
    against multiple possible values (OR).

    Return the modified query and any entries in search_opts
    whose keys do not match columns or whose values are not
    tuples.

    :param query: a non-null query object
    :param cls: the database model class that filters will apply to
    :param search_opts: a dictionary whose key/value entries are interpreted as
    column names and search values
    :returns: a tuple containing the modified query and a dictionary of
    unused search_opts
    """
    if not search_opts:
        return query, search_opts

    remaining = {}
    for k, v in search_opts.items():
        if type(v) == tuple and k in cls.__table__.columns:
            col = cls.__table__.columns[k]
            query = query.filter(col.in_(v))
        else:
            remaining[k] = v
    return query, remaining


async def get_model_by_id(model, model_id, account_id=None):
    if not model_id:
        return None

    query = model_query(model)
    query = query.filter(model.id == model_id)

    # For now, only cluster model check account id
    # TODO if other models use this, please add column ksc_user_id
    if account_id:
        from oasis.db.models.user import UserModel
        user_model = await model_query(UserModel).filter(UserModel.id == account_id).query_one()
        if not user_model:
            raise Exception(f'User not found when get_model_by_id, model: {model}, '
                            f'model_id: {model_id}, '
                            f'account_id {account_id}')
        account_role = user_model.role
        if account_role == UserModel.ROLE.NORMAL:
            query = query.filter(model.ksc_user_id == account_id)
        elif account_role != UserModel.ROLE.ADMIN:
            return None

    return await query.query_one()


OasisBase = declarative_base(cls=ModelBase)


def __load_all_models():
    from importlib import import_module
    from pkgutil import walk_packages
    for _, modname, _ in walk_packages(path=__path__):
        import_module(f'{__name__}.{modname}')


__load_all_models()
