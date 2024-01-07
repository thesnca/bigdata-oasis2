class CMPTMQ:
    """
    cmpt = CMPTMQ(redis_pool)

    async def xadd():
        fields = OrderedDict((
            (b'field1', b'value1'),
            (b'field2', b'value2'),
        ))
        await cmpt.xadd('stream:1', fields)


    async def xread():
        async for msg in cmpt.xread('stream:1'):
            print(msg)


    async def xreadgroup():
        async for msg in cmpt.xread_group('stream:1', 'group1', 'consumer1'):
            for m in msg:
                msg_id = m[1]
                await cmpt.xack('stream:1', 'group1', msg_id)
            print('msg', msg)


    async def xgroup_create():
        await cmpt.xgroup_create('stream:1', 'group1')
    """

    def __init__(self, pool):
        self.pool = pool

    async def xread(self, stream_id):
        while True:
            yield await self.pool.xread([stream_id])

    async def xgroup_create(self, stream_id, group, last_id='$', mk_stream=False):
        return await self.pool.execute('XGROUP', 'create', stream_id, group, last_id,
                                       'MKSTREAM' if mk_stream else '')

    async def xgroup_get_consumers(self, stream_id, group):
        return await self.pool.execute('XINFO', 'CONSUMERS', stream_id, group)

    async def xgroup_del_consumer(self, stream_id, group, consumer):
        return await self.pool.execute('XGROUP', 'DELCONSUMER', stream_id, group, consumer)

    async def xread_group(self, *, stream_id, group, consumer, latest_ids='>',
                          count=None, timeout=None):
        # while True:
        messages = await self.pool.xread_group(group, consumer, [stream_id],
                                               latest_ids=[latest_ids], count=count,
                                               timeout=timeout)
        if not messages:
            return

        for m in messages:
            yield m

    async def xadd(self, stream_id, fields):
        return await self.pool.xadd(stream_id, fields)

    async def xack(self, stream_id, group, *ids):
        return await self.pool.xack(stream_id, group, *ids)

    async def xdel(self, stream_id, msg_id):
        return await self.pool.execute('XDEL', stream_id, msg_id)

    # TODO
    async def xdelall(self, stream_id):
        for msg in await self.pool.xrange(stream_id):
            await self.xdel('stream:1', msg[0])
