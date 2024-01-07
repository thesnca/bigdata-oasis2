PRODUCT_GROUP_ID_MAP = {
    'KES': 206,
    'KHBASE': 216
}

ORDER_USE_MAP = {
    'BUY': 1,
    'RENEW': 2,
    'SCALE': 3,
    'FREETRIAL': 4,
    'REGULARIZATION': 5,
    'DELETE': 7,
    'POSTPONE': 8,
}

APP_ID_MAP = {
    'KMR': '107.console',
    'KES': '206.console',
    'KHBASE': '216.console',
}

CHARGE_API_MAP = {
    'create_order': '/trade/orders',
    'find_suborders_by_order_id': '/trade/querySubOrdersByOrderId',
    'find_suborders_by_instance_id': '/trade/querySubOrdersByInstanceIds',
    'update_suborder_status': '/trade/notifySubOrderStatus',
    'find_sub_order': '/trade/querySubOrders',
    'batch_delete_instance': '/trade/batchRefundInstances',
}

PRODUCT_TYPE_MAP = {
    'EBS': 132,
    'KEC': 41,
    'EPC': 47,
    'EIP': 9,
    'IEIP': 10,
    'SLB': 5,
    'KES_EBS': 368,
    'KES_SSD': 369,
    'KES_EPC': 370,
    'KES_HDD': 578,
    'KHBase_EBS': 408,
    'KHBase_SSD': 409,
    'KHBase_EPC': 410,
}

PRODUCT_USE_MAP = {
    'BUY': 1,
    'RENEW': 2,
    'SCALE': 3,
    'REGULARIZATION': 4,
    'POSTPONE': 5,
    'DELETE': 7,
}

PRODUCT_WHAT_MAP = {
    'Monthly': 1,
    'Minutely': 1,
    'Daily': 1,
    'FreeTrial': 2,
}

CHARGE_BILL_MAP = {
    'Minutely': 87,
    'Monthly': 1,
    'Daily': 5,
    'FreeTrial': 5,
}

EIP_CHARGE_BILL_MAP = {
    'PrePaidByMonth': 1,
    'PrePaidByHour': 2,
    'PostPaidByPeak': 3,
    'PostPaidByDay': 5,
    'FreeTrial': 5,
    'DailyPeak': 6,
    'BandwidthHourly': 80,
    'PostPaidByRegionPeak': 83,
    'PostPaidByHour': 84,
    'DailyPaidByTransfer': 86,
    'HourlyInstantSettlement': 87,
    'PostPaidByTransfer': 704,
    'PrepaidByTime': 801,
    'PostpaidByTime': 805,
    'PostPaidByAdvanced95Peak': 807,
}

MACHINE_ROOM_MAP = {
    'cn-beijing-6a': 'TJWQRegion',
    'cn-beijing-6b': 'TJWQRegion',
    'cn-beijing-6c': 'TJWQRegion',
    'cn-beijing-6d': 'TJWQRegion',
    'cn-shanghai-2': 'SHPBSRegionOne',
    'cn-shanghai-2a': 'SHPBSRegionOne',
    'cn-shanghai-2b': 'SHPBSRegionOne',
    'cn-shanghai-3a': 'SHPBSVpctestRegionOne',
    'cn-shanghai-3b': 'SHPBSVpctestRegionOne',
    'cn-guangzhou-1a': 'GZVPCRegion',
    'cn-guangzhou-1b': 'GZVPCRegion',
    'ap-singapore-1a': 'SGPRegionOne',
    'ap-singapore-1b': 'SGPRegionOne',
    'eu-east-1a': 'RUSSRegionOne',
    'eu-east-1b': 'RUSSRegionOne',
    'cn-taipei-1a': 'TAIPEIVPCRegion',
    'cn-beijing-fin-a': 'BJFINRegion',
}

# MACHINE_ROOM_MAP = {
#     'cn-beijing-6a': 'cn-beijing-6',
#     'cn-beijing-6b': 'cn-beijing-6',
#     'cn-beijing-6c': 'cn-beijing-6',
#     'cn-shanghai-2a': 'cn-shanghai-2',
#     'cn-shanghai-2b': 'Scn-shanghai-2',
#     'cn-shanghai-3a': 'cn-shanghai-3',
#     'cn-shanghai-3b': 'cn-shanghai-3',
#     'cn-guangzhou-1a': 'cn-guangzhou-1',
#     'ap-singapore-1a': 'ap-singapore-1',
#     'eu-east-1a': 'eu-east-1',
#     'eu-east-1b': 'eu-east-1',
#     'cn-taipei-1a': 'cn-taipei-1'
# }

PRODUCT_GROUP_MAP = {
    'KES': 206,
    'KHBASE': 216,
    'KMR': 107,
    'EIP': 102,
    'EBS': 101,
    'KEC': 100,
    'EPC': 111,
    'GEPC': 133,
    'SLB': 105,
}

product_code_map = {v: k
                    for k, v in PRODUCT_GROUP_MAP.items()}

CN_REGIONG_MAP = {
    'cn-beijing-6a': '华北1（北京）',
    'cn-beijing-6b': '华北1（北京）',
    'cn-beijing-6c': '华北1（北京）',
    'cn-beijing-6d': '华北1（北京）',
    'cn-shanghai-2a': '华北1',
    'cn-shanghai-2b': '华东1（上海）',
    'cn-shanghai-3a': '华东2（上海)',
    'cn-shanghai-3b': '华东2（上海)',
    'cn-guangzhou-1a': '华南1（广州）',
    'cn-guangzhou-1b': '华南1（广州）',
    'ap-singapore-1a': '新加坡',
    'ap-singapore-1b': '新加坡',
    'eu-east-1a': '俄罗斯（莫斯科）',
    'eu-east-1b': '俄罗斯（莫斯科）',
    'cn-taipei-1a': '台北',
    'cn-beijing-fin-a': '华北金融1（北京）',
}

CN_ROOM_MAP = {
    'cn-beijing-6a': '可用区A',
    'cn-beijing-6b': '可用区B',
    'cn-beijing-6c': '可用区C',
    'cn-beijing-6d': '可用区D',
    'cn-shanghai-2a': '可用区A',
    'cn-shanghai-2b': '可用区B',
    'cn-shanghai-3a': '可用区A',
    'cn-shanghai-3b': '可用区B',
    'cn-guangzhou-1a': '可用区A',
    'cn-guangzhou-1b': '可用区B',
    'ap-singapore-1a': '可用区A',
    'ap-singapore-1b': '可用区B',
    'eu-east-1a': '可用区A',
    'eu-east-1b': '可用区B',
    'cn-taipei-1a': '可用区A',
    'cn-beijing-fin-a': '可用区A',
}

MAIN_INSTANCE_POLICY = {
    "KES": {
        "INSTANCE_GROUP_TYPE": "DATA",
        "INSTANCE_NAME": "data-1",
        "EPC_INSTANCE_NAME": "data-001"
    },
    "KHBASE": {
        "INSTANCE_GROUP_TYPE": "MASTER",
        "INSTANCE_NAME": "ster-1",
        "EPC_INSTANCE_NAME": "ster-001"
    }
}
