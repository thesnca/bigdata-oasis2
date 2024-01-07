from oasis.utils.config import config

KEC_API_MAP = {
    'notify': 'NotifySubOrderStatus',
    'create_instance': 'CreateInstances',
    'describe_instance': 'DescribeInstances',
    'delete_instance': 'TerminateInstances',
    'start_instance': 'StartInstances',
    'stop_instance': 'StopInstances',
    'reboot_instance': 'RebootInstances',
    'modify_instance': 'ModifyInstanceType',
    'create_data_guard_group': 'CreateDataGuardGroup',
    'delete_data_guard_group': 'DeleteDataGuardGroups'
}

EPC_API_MAP = {
    'notify': 'NotifySubOrderStatus',
    'create_instance': 'CreateEpc',
    'describe_instances': 'DescribeEpcs',
    'delete_instances': 'DeleteEpc',
    'start_instance': 'StartEpc',
    'stop_instance': 'StopEpc',
    'reboot_instance': 'RebootEpc'
}

SKS_API_MAP = {
    'describe_keys': 'DescribeKeys',
    'create_key': 'CreateKey',
    'delete_key': 'DeleteKey',
}

VOLUME_TYPE_MAP = {
    'LOCAL_SSD': 'Local_SSD',
    'CLOUD_SSD': 'SSD3.0',
    'LOCAL_HDD': 'HDD',  # D4机型，不需要传VolumeType
    'CLOUD_EHDD': 'EHDD',  # 已弃用
    'CLOUD_ESSD1': 'ESSD_PL1',
    'CLOUD_ESSD2': 'ESSD_PL2',
    'CLOUD_ESSD3': 'ESSD_PL3',
}

NEUTRON_API = {
    'create_sg': 'vpc/vpc_securitygroups',
    'delete_sg': 'vpc/vpc_securitygroups/{sg_id}',
    'get_sg': 'vpc/vpc_securitygroups?name={sg_name}&domain_id={domain_id}',
    'get_sg_id': 'vpc/vpc_securitygroups/{sg_id}',

    'handle_vif_sg': 'vpc/vpc_securitygroups/{sg_id}/handle_vif_sg_bulk',
    'create_rules': 'vpc/vpc_securitygroups/{sg_id}',
    'get_igw': 'vpc/igws?domain_id={vpc_domain_id}',
    'create_igw': 'vpc/igws',
    'get_inner_networks': 'networks?type=private&shared=1',
    'create_inner_floating_ip': 'floatingips',
    'get_inner_floating_ip': 'floatingips?id={eip_id}',
    'delete_inner_floating_ip': 'floatingips/{eip_id}',
    'create_port_forward': 'vpc/portfwds',
    'get_port_forward': 'vpc/portfwds?vm_id={vm_id}',
    'delete_port_forward': 'vpc/portfwds/{pfwd_id}',
    'list_network': 'networks?type=public&shared=1&isp=bgp',
    'create_eip': 'floatingips',
    'bind_eip': 'floatingips/{eip_id}',
    'unbind_eip': 'floatingips/{eip_id}',
    'delete_eip': 'floatingips/{eip_id}',
    'get_eip_id_by_ip': 'floatingips?floating_ip_address={eip}',
    'search_eip_by_vm_id': 'floatingips?device_id={vm_id}',
    'create_pool': 'lb/pools',
    'get_pool': 'lb/pools?id={pool_id}',
    'delete_pool': 'lb/pools/{pool_id}',
    'create_vip': 'lb/vips',
    'list_vips_by_pool_id': 'lb/vips?pool_id={pool_id}',
    'get_vip': 'lb/vips/{vip_id}',
    'delete_vip': 'lb/vips/{vip_id}',
    'create_lb_member': 'lb/members',
    'get_lb_member': 'lb/members?vm_id={vm_id}'
}

DEFAULT_SEC_GROUP_RULE_KHBASE = [
    {
        'direction': 'in',
        'ip': '0.0.0.0',
        'mask': 0,
        'protocol': 'tcp',
        'port_start': 22,
        'port_end': 22
    },
    {
        'direction': 'in',
        'ip': '0.0.0.0',
        'mask': 0,
        'protocol': 'tcp',
        'port_start': config.getint('vpc', 'nginx_listen_ports'),
        'port_end': config.getint('vpc', 'nginx_listen_ports')
    },
    {
        'direction': 'out',
        'ip': '0.0.0.0',
        'mask': 0,
        'protocol': 'ip'
    }
]

DEFAULT_SEC_GROUP_RULE_KES = [
    {
        "direction": "in",
        "ip": "0.0.0.0",
        "mask": 0,
        "protocol": "tcp",
        "port_start": 22,
        "port_end": 22
    },
    {
        "direction": "in",
        "ip": "0.0.0.0",
        "mask": 0,
        "protocol": "tcp",
        "port_start": config.getint('vpc', 'nginx_listen_ports'),
        "port_end": config.getint('vpc', 'nginx_listen_ports')
    },
    {
        "direction": "in",
        "ip": "0.0.0.0",
        "mask": 0,
        "protocol": "tcp",
        "port_start": config.getint('vpc', 'hue_listen_ports'),
        "port_end": config.getint('vpc', 'hue_listen_ports')
    },
    {
        "direction": "out",
        "ip": "0.0.0.0",
        "mask": 0,
        "protocol": "ip"
    }
]

DEFAULT_SEC_GROUP_CONTROL_RULE = [
    {
        'direction': 'in',
        'ip': config.get('vpc', 'pub_zone_subnet'),
        'mask': config.get('vpc', 'pub_zone_mask'),
        'protocol': 'tcp',
        'port_start': config.getint('vpc', 'control_port_start'),
        'port_end': config.getint('vpc', 'control_port_end'),
    }
]

DEFAULT_LINK = {
    'kes': {
        'public': {
            'kibana': '28291',
            'cluster': '9200'
        },
        'private': {
            'kibana': '5601',
            'cluster': '9200'
        }
    },
    'khbase': {
        'public': {
            'master': '28291'
        }
    }
}

TAG_REP = {
    'KEC': 'instance',
    'EPC': 'epc-instance',
    'EBS': 'volume',
    'EIP': 'eip',
    'SLB': 'loadbalancer',
    'KMR': 'kmr',
    'KES': 'kes',
    'KHBASE': 'khbase'
}
