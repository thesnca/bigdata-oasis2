from asyncio import Semaphore

from aioredis import create_redis_pool


class RedisClient:
    def __init__(self, **kwargs):
        self.host = kwargs.pop('host', '127.0.0.1')
        self.port = kwargs.pop('port', 6379)
        self.maxsize = int(kwargs.pop('maxsize', 10))
        self._pool = None
        self._semaphore = Semaphore()
        password = kwargs.pop('password', None)
        if password:
            kwargs.setdefault('password', password)
        self.conf = kwargs

    async def _get_pool(self):
        if not self._pool:
            async with self._semaphore:
                if not self._pool:
                    self._pool = await create_redis_pool(f'redis://{self.host}:{self.port}',
                                                         maxsize=self.maxsize,
                                                         **self.conf)
        return self._pool

    def __getattr__(self, item):
        async def __inner(*args, **kwargs):
            if not self._pool:
                await self._get_pool()
            try:
                rslt = await self._pool.__getattribute__(item)(*args, **kwargs)
            except:
                rslt = None

            return rslt

        return __inner
