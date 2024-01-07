import asyncio

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from oasis.utils.logger import logger


def get_conn(func):
    async def _inner(self, *args, **kwargs):
        pool = await self._get_pool()
        async with pool.begin() as conn:
            return await func(self, conn=conn, *args, **kwargs)

    return _inner


def get_session(func):
    async def _inner(self, *args, **kwargs):
        pool = await self._get_pool()
        try:
            async with AsyncSession(pool) as session:
                return await func(self, session=session, *args, **kwargs)
        except BrokenPipeError:
            logger.error('Mysql pool expired, reconnect...')
            self._pool = None
            pool = await self._get_pool()
            async with AsyncSession(pool) as session:
                return await func(self, session=session, *args, **kwargs)

    return _inner


class MysqlClient:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host', '127.0.0.1')
        self.port = kwargs.get('port', 3306)
        self.username = kwargs.get('username', None)
        self.password = kwargs.get('password', None)
        self.db = kwargs.get('db', None)
        self.maxsize = int(kwargs.get('max_pool_size', 10))
        self._pool = None
        self._semaphore = asyncio.Semaphore()

    async def _get_pool(self):
        if not self._pool:
            async with self._semaphore:
                if not self._pool:
                    self._pool = create_async_engine(
                        f'mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/'
                        f'{self.db}?charset=utf8mb4',
                        pool_pre_ping=True,
                        pool_recycle=3600
                    )
        return self._pool

    @get_session
    async def query_one(self, select, *, session):
        query = await session.execute(select)
        res = query.scalar()
        return res

    @get_session
    async def query_all(self, select, *, session):
        # Aware of default limit!
        query = await session.execute(select)
        res = [r for r in query.unique().scalars()]
        return res

    @get_session
    async def count(self, count, *, session):
        query = await session.execute(count)
        res = query.scalar()
        return res

    @get_conn
    async def insert_one(self, model, conn):
        res = await conn.execute(model.__table__.insert()
                                 .values(**model.to_dict()))
        model.id = res.inserted_primary_key[0]
        return model

    @get_conn
    async def update_one(self, model, values, conn):
        model_id = model.id
        await conn.execute(model.__table__.update()
                           .where(model.__table__.c.id == model_id)
                           .values(**values))
        return model

    @get_conn
    async def delete_one(self, model, conn):
        await conn.execute(model.__table__.delete()
                           .where(model.__table__.c.id == model.id))

    @get_conn
    async def create_all_table(self, base, conn):
        return await conn.run_sync(base.metadata.create_all)

    @get_conn
    async def drop_all_table(self, base, conn):
        return await conn.run_sync(base.metadata.drop_all)
