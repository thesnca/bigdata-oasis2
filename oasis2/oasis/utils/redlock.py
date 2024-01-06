import re

from oasis.db.service import redis_client
from oasis.utils.generator import gen_cluster_lock
from oasis.utils.generator import gen_request_lock
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger


def _prepare(func):
    async def __inner(self, *args, **kwargs):
        if not self._acquire_lock_sha1:
            script = self.SET_LOCK_SCRIPT
            script = re.sub(r'^\s+', '', script, flags=re.M).strip()
            self._acquire_lock_sha1 = await redis_client.script_load(script)

            script = self.UNSET_LOCK_SCRIPT
            script = re.sub(r'^\s+', '', script, flags=re.M).strip()
            self._release_lock_sha1 = await redis_client.script_load(script)

            script = self.GET_LOCK_TTL_SCRIPT
            script = re.sub(r'^\s+', '', script, flags=re.M).strip()
            self._get_lock_ttl_script_sha1 = await redis_client.script_load(script)

        res = await func(self, *args, **kwargs)
        return res

    return __inner


class RedLock:
    # KEYS[1] - lock resource key
    # ARGS[1] - lock unique identifier
    # ARGS[2] - expiration time in milliseconds
    SET_LOCK_SCRIPT = """
        local identifier = redis.call('get', KEYS[1])
        if not identifier or identifier == ARGV[1] then
            return redis.call("set", KEYS[1], ARGV[1], 'PX', ARGV[2])
        else
            return redis.error_reply('ERROR')
        end"""

    # KEYS[1] - lock resource key
    # ARGS[1] - lock unique identifier
    UNSET_LOCK_SCRIPT = """
    local identifier = redis.call('get', KEYS[1])
    if not identifier then
        return redis.status_reply('OK')
    elseif identifier == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return redis.error_reply('ERROR')
    end"""

    # KEYS[1] - lock resource key
    GET_LOCK_TTL_SCRIPT = """
    local identifier = redis.call('get', KEYS[1])
    if not identifier then
        return redis.error_reply('ERROR')
    elseif identifier == ARGV[1] then
        return redis.call("TTL", KEYS[1])
    else
        return redis.error_reply('ERROR')
    end"""

    def __init__(self, timeout=10):
        self.timeout = timeout
        self._acquire_lock_sha1 = None
        self._release_lock_sha1 = None
        self._get_lock_ttl_script_sha1 = None

    @_prepare
    async def is_locked(self, key):
        identifier = await redis_client.get(key)
        return identifier is not None

    @_prepare
    async def acquire_lock(self, key, timeout=None, identifier=None):
        identifier = identifier or gen_uuid4()
        timeout_ms = (timeout or self.timeout) * 1000
        res = await redis_client.evalsha(
            self._acquire_lock_sha1,
            keys=[key],
            args=[identifier, timeout_ms],
        )
        if res == 'OK':
            return identifier

    @_prepare
    async def release_lock(self, key, identifier):
        res = await redis_client.evalsha(
            self._release_lock_sha1,
            keys=[key],
            args=[identifier],
        )
        return res

    @_prepare
    async def get_lock_ttl(self, key, identifier):
        res = await redis_client.evalsha(
            self._get_lock_ttl_script_sha1,
            keys=[key],
            args=[identifier],
        )
        return res


async def lock_cluster(cluster_id, job_id, timeout=1800):
    res = None
    try:
        logger.info(f'Lock cluster {cluster_id}, job {job_id}')
        res = await redlock.acquire_lock(gen_cluster_lock(cluster_id), timeout=timeout, identifier=job_id)
    except Exception as e:
        logger.error(f'Failed to lock cluster {cluster_id}, job {job_id}, Exception: {e}..')
    return res


async def unlock_cluster(cluster_id, job_id):
    logger.info(f'Unlock cluster {cluster_id}, job {job_id}')
    await redlock.release_lock(gen_cluster_lock(cluster_id), job_id)


async def lock_request(request_id, action, timeout=600):
    res = None
    try:
        logger.info(f'Lock request {request_id}, action {action}.')
        res = await redlock.acquire_lock(gen_request_lock(request_id, action), timeout=timeout)
    except Exception as e:
        logger.error(f'Failed to lock request {request_id}, action {action}, Exception: {e}..')
    return res


async def unlock_request(request_id, action, iden):
    logger.info(f'Unlock request {request_id}, action {action}, identifier {iden}')
    await redlock.release_lock(gen_request_lock(request_id, action), iden)


redlock = RedLock()
