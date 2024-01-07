import traceback

from oasis.utils import http
from oasis.utils.logger import logger


async def send_scale_notification(noti, action, new_instance_ids, cluster_type, account_id, region):
    cluster_id = noti.cluster_id
    url = noti.url
    # token = noti.token

    headers = {
        'Content-Type': 'application/json',
    }

    data = {
        'AccountId': account_id,
        'ClusterId': cluster_id,
        'ClusterType': cluster_type,
        'Region': region,
        'InstanceIds': new_instance_ids,
        'Action': action,
    }
    try:
        await http.post(url, data=data, headers=headers)

    except Exception as e:
        logger.info(f'Send scale notification failed, url: {url}, data: {data}, '
                    f'Error: {e}\n{traceback.format_exc()}')
