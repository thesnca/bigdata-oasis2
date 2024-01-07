from oasis.db.service.mq import CMPTMQ
from oasis.db.service.mysql import MysqlClient
from oasis.db.service.redis import RedisClient
from oasis.utils.config import config

_redis_conf = {k: v for k, v in config['redis'].items()}
redis_client = RedisClient(**_redis_conf)

_mysql_conf = {k: v for k, v in config['mysql'].items()}
mysql_client = MysqlClient(**_mysql_conf)

mq_client = RedisClient(**_redis_conf)
competition_mq = CMPTMQ(mq_client)
