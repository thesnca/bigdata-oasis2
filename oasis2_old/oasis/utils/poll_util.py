def wait_until_complete(timeout=600, interval=10):
    def _inner(func):
        async def _wait(*args, **kwargs):
            from asyncio import sleep
            from datetime import datetime
            from oasis.utils.logger import logger
            start_time = datetime.now()

            duration = 0
            while duration < timeout:
                res = await func(*args, **kwargs)

                if res:
                    logger.info(f'Wait {func.__name__} to be done. '
                                f'Duration: {duration} / {timeout} s. '
                                f'Args: {args}, Kwargs: {kwargs}.')
                    return res

                logger.info(
                    f'Wait {func.__name__} until complete... Result: {res}, '
                    f'Duration: {duration} / {timeout} s.'
                    f'Args: {args}, Kwargs: {kwargs}.')
                await sleep(interval)
                duration = int((datetime.now() - start_time).total_seconds())

            raise Exception(f'Wait {func.__name__} timeout.'
                            f'Duration: {duration} / {timeout} s.'
                            f'Args: {args}, Kwargs: {kwargs}.')

        return _wait

    return _inner
