import asyncssh

from oasis.utils.logger import logger


class Remote:
    def __init__(self, ip, port, private_key, **kwargs):
        self.conn = None
        self.ip = ip
        self.port = port
        self.private_key = private_key
        self.kwargs = kwargs

    async def __aenter__(self):
        self.conn = await asyncssh.connect(self.ip, self.port, username='root',
                                           client_keys=[self.private_key],
                                           known_hosts=None).__aenter__()
        return self

    async def __aexit__(self, *exc_info):
        await self.conn.__aexit__(*exc_info)

    async def execute(self, *args, **kwargs):
        timeout = kwargs.pop('timeout', 180)
        raise_when_error = kwargs.pop('raise_when_error', True)
        logger.info(f'Remote execute cmd on {self.ip}:{self.port} ({self.kwargs}), '
                    f'Args:{args}, Kwargs:{kwargs}')
        res = await self.conn.run(timeout=timeout, *args, **kwargs)
        logger.info(f'Remote execute cmd on {self.ip}:{self.port} ({self.kwargs}), '
                    f'Args:{args}, Kwargs:{kwargs}, Res: {res}')
        if raise_when_error and res.exit_status:
            raise Exception(f'Remote execute cmd failed, {self.ip}:{self.port} ({self.kwargs}), '
                            f'Args:{args}, Kwargs:{kwargs}, '
                            f'Error: {res.stderr}')
        return res.exit_status, res.stdout

    async def write_file(self, dest_path, data):
        async with self.conn.start_sftp_client() as sftp:
            async with sftp.open(dest_path, 'w+') as file:
                await file.write(data)
