import base64
from datetime import datetime
from datetime import timedelta
import hashlib
import hmac
from uuid import uuid4

from conf.instance_conf import INSTANCE_HOSTNAME_DEV
from oasis.utils.config import config
import re


def gen_uuid4():
    return str(uuid4())


def gen_name(pre, strategy):
    name = ''
    if strategy == 'timestamp':
        name = f'{pre}-{int(datetime.now().timestamp())}'
    return name


def gen_group_name(group_type):
    return f'gn-{gen_uuid4()[:8]}-{group_type}'


def gen_cluster_lock(cluster_id):
    return f'/oasis/lock/cluster/{cluster_id}'


def gen_request_lock(request_id, action):
    return f'/oasis/lock/request/{action}/{request_id}'


def sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()


def get_signature_key(key, date_stamp, region_name, service_name):
    k_date = sign(('AWS4' + key).encode('utf-8'), date_stamp)
    k_region = sign(k_date, region_name)
    k_service = sign(k_region, service_name)
    k_signing = sign(k_service, 'aws4_request')
    return k_signing


def gen_aws_auth_header(ak, sk, host, service, method, region, params):
    t = datetime.utcnow()
    amzdate = t.strftime('%Y%m%dT%H%M%SZ')
    datestamp = t.strftime('%Y%m%d')
    canonical_uri = '/'

    payload_hash = hashlib.sha256(''.encode('utf-8')).hexdigest()
    # canonical_querystring = '&'.join(sorted([f'{k}={v}' for k, v in params.items()]))
    canonical_querystring = '&'.join(sorted([f'{k.replace("[", "%5B").replace("]", "%5D")}={v.replace("/32", "%2F32").replace(",", "%2C")}' for k, v in params.items()]))
    canonical_headers = 'host:' + host + '\n' + 'x-amz-content-sha256:' + payload_hash + '\n' + 'x-amz-date:' + amzdate + '\n'
    signed_headers = 'host;x-amz-content-sha256;x-amz-date'

    canonical_request = f'{method}\n{canonical_uri}\n{canonical_querystring}\n' \
                        f'{canonical_headers}\n{signed_headers}\n{payload_hash}'
    algorithm = 'AWS4-HMAC-SHA256'
    credential_scope = f'{datestamp}/{region}/{service}/aws4_request'
    string_to_sign = f'{algorithm}\n{amzdate}\n{credential_scope}\n' + hashlib.sha256(
        canonical_request.encode('utf-8')).hexdigest()
    signing_key = get_signature_key(sk, datestamp, region, service)
    signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
    authorization_header = f'{algorithm} Credential={ak}/{credential_scope}, ' \
                           f'SignedHeaders={signed_headers}, Signature={signature}'
    headers = {
        'X-Amz-Date': amzdate,
        'x-amz-content-sha256': payload_hash,
        'Authorization': authorization_header,
    }
    return headers


def get_url_suffix(secret, product_type='kmr'):
    url_overdue_seconds = int(config.get('gringotts', 'url_overdue_seconds'))
    future = datetime.utcnow() + timedelta(seconds=url_overdue_seconds)
    expiry = int(future.timestamp())

    secure_link = f'{secret}{expiry}'
    token = hashlib.md5(secure_link.encode('utf8')).digest()
    encoded_token = base64.urlsafe_b64encode(token).decode('utf8').rstrip('=')
    suffix = f'{product_type}_token={encoded_token}&{product_type}_exp={expiry}'
    return suffix


def generate_instance_name(cluster_id, instance_group_name, index=None, prefix='kmr-'):
    # escape all invalid hostname characters
    cluster_name = cluster_id.split('-')[0]
    if index is not None:
        return f'{prefix}{cluster_name}-{instance_group_name}-{index:03}'.lower()
    else:
        return f'{prefix}{cluster_name}-{instance_group_name}'.lower()


def generate_instance_hosts(instances, test_env=False):
    hosts = [f'127.0.0.1 localhost\n']
    if test_env:
        hosts.append(INSTANCE_HOSTNAME_DEV)
    for instance in instances:
        instance_name = instance.instance_name
        hosts.append(f'{instance.internal_ip} {instance_name}.ksc.com {instance_name}\n')
    return ''.join(hosts)


def validate_instance_type_code(new_instance_type_code, origin_instance_type_code):
    '''
    1 升配
    0 未变
    -1 降配
    升配/降配同时出现算降配。
    '''
    oitc_cpu, oitc_mem = re.findall(r"\d+\.?\d*", origin_instance_type_code.split('.')[2])
    nitc_cpu, nitc_mem = re.findall(r"\d+\.?\d*", new_instance_type_code.split('.')[2])

    if (int(oitc_cpu) > int(nitc_cpu)) or (int(oitc_mem) > int(nitc_mem)):
        return -1
    elif (int(nitc_cpu) > int(oitc_cpu)) or (int(nitc_mem) > int(oitc_mem)):
        return 1

    return 0
