import asyncio
from datetime import datetime

from oasis.db.models import OasisBase
from oasis.db.service import mysql_client
from oasis.utils.logger import logger


async def init_mysql():
    await mysql_client.create_all_table(OasisBase)


async def clean_mysql():
    await mysql_client.drop_all_table(OasisBase)


logger.init_logger('init_db', 'init_db')
start_time = datetime.utcnow()
# logger.info(f'Start clean db...')
# asyncio.run(clean_mysql())
# end_time = datetime.utcnow()
# time_diff = (end_time - start_time).total_seconds()
# logger.info(f'Finish clean db... cost {time_diff} s.')
logger.info(f'Start init db...')
asyncio.run(init_mysql())
end_time = datetime.utcnow()
time_diff = (end_time - start_time).total_seconds()
logger.info(f'Finish init db... cost {time_diff} s.')
