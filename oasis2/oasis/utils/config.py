from configparser import ConfigParser
import os

import conf

config = ConfigParser(allow_no_value=True)

env = os.environ.get('OASIS_ENV')
region = os.environ.get('OASIS_REGION')
if env and region:
    conf_file = f'oasis2_{region.lower()}_{env.lower()}.ini'
else:
    conf_file = 'my_conf.ini'
config.read(f'{os.path.dirname(conf.__file__)}/{conf_file}')

base_kec_userdata_conf = ""
if not base_kec_userdata_conf:
    with open(f'{os.path.dirname(conf.__file__)}/kec_userdata_conf.sh', 'r') as kec_userdata_file:
        base_kec_userdata_conf = kec_userdata_file.read()

base_nginx_conf = ""
if not base_nginx_conf:
    with open(f'{os.path.dirname(conf.__file__)}/nginx.conf', 'r') as base_nginx_file:
        base_nginx_conf = base_nginx_file.read()

base_gringotts_repo = ""
if not base_gringotts_repo:
    with open(f'{os.path.dirname(conf.__file__)}/gringotts.repo', 'r') as base_gringotts_file:
        base_gringotts_repo = base_gringotts_file.read()