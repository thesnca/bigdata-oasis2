from functools import partial

from oasis.utils.validation.cheking import CLUSTER_NAME_REGEX
from oasis.utils.validation.cheking import INSTANCE_TYPE_REGEX
from oasis.utils.validation.cheking import MARKER_REGEX
from oasis.utils.validation.cheking import NoNeed
from oasis.utils.validation.cheking import check_num_range
from oasis.utils.validation.cheking import check_str_regex
from oasis.utils.validation.cheking import check_uuid
from oasis.utils.validation.cheking import check_within_enum

LIST_CLUSTERS_SYNTAX = {
    'marker': (str, partial(check_str_regex, pattern=MARKER_REGEX)),
}

DESCRIBE_CLUSTER_SYNTAX = {
    'cluster_id': (str, check_uuid),
}

LAUNCH_CLUSTER_SYNTAX = {
    'charge_type': (str, partial(check_within_enum,
                                 enum=[
                                     'Monthly',
                                     'Daily',
                                     'HourlyInstantSettlement',
                                 ],
                                 )),
    'purchase_time': ((int, NoNeed), partial(check_num_range, low=1, high=36)),
    'cluster_name': ((str, NoNeed), partial(check_str_regex, pattern=CLUSTER_NAME_REGEX)),
    'availability_zone': str,
    'main_version': (str, partial(check_within_enum,
                                  enum={
                                      'kes': [
                                          '7.4.2',
                                          '6.8.4',
                                          '5.6.16',
                                      ],
                                      'khbase': [
                                          '2.2.3',
                                          '1.4.12',
                                      ],
                                  }, )
                     ),
    'instance_groups': [
        {
            'instance_group_type': (str, partial(check_within_enum,
                                                 enum={
                                                     'kes': [
                                                         'MASTER',  # KES
                                                         'DATA',  # KES
                                                         'COORDINATOR',  # KES
                                                         'WARM',  # KES
                                                     ],
                                                     'khbase': [
                                                         'MASTER',  # KHBASE
                                                         'CORE',  # KHBASE
                                                     ],
                                                 }, )
                                    ),
            'instance_type': (str, partial(check_str_regex, pattern=INSTANCE_TYPE_REGEX)),
            'instance_count': (int, partial(check_num_range, low=2, high=255)),
            'resource_type': ((str, NoNeed), partial(check_within_enum,
                                                     enum=[
                                                         'KEC',
                                                         'EPC',
                                                     ])),
            'raid_type': ((str, NoNeed), partial(check_within_enum,
                                                 enum=[
                                                     'Raid0',
                                                     'Raid1',
                                                     'Raid5',
                                                     'Raid50',
                                                     'Raid10',
                                                     'SRaid0',
                                                 ])),
            'multi_instance_count': ((int, NoNeed), partial(check_num_range, low=1)),
            'volume_type': ((str, NoNeed), partial(check_within_enum,
                                                   enum={
                                                       'kes': [
                                                           'SSD3.0',
                                                           'Local_SSD',
                                                           'ESSD_PL1',
                                                           'ESSD_PL2',
                                                           'ESSD_PL3',
                                                       ],
                                                       'khbase': [
                                                           'SSD3.0',
                                                           'Local_SSD',
                                                       ],
                                                   })
                            ),
            'volume_size': ((int, NoNeed), partial(check_num_range, low=20, high=16000)),
        },
    ],
    'enable_eip': (bool, NoNeed),
    'eip_id': ((str, NoNeed), check_uuid),
    'eip_charge_type': ((str, NoNeed), partial(check_within_enum,
                                               enum=[
                                                   'Monthly',
                                                   'Peak',
                                                   'Daily',
                                                   'TrafficMonthly',
                                                   'DailyPaidByTransfer',
                                                   'HourlyInstantSettlement',
                                               ])),
    'eip_purchase_time': ((int, NoNeed), partial(check_num_range, low=1, high=36)),
    'eip_line_id': ((str, NoNeed), check_uuid),
    'eip_band_width': ((int, NoNeed), partial(check_num_range, low=1, high=15000)),
    'vpc_domain_id': (str, check_uuid),
    'vpc_subnet_id': ((str, NoNeed), check_uuid),
    'vpc_epc_subnet_id': ((str, NoNeed), check_uuid),
    'security_group_id': ((str, NoNeed), check_uuid),
    'project_id': (int, NoNeed),
}

SCALE_OUT_INSTANCE_GROUPS_SYNTAX = {
    'cluster_id': (str, check_uuid),
    'instance_groups': [
        {
            'instance_group_type': (str, partial(check_within_enum,
                                                 enum={
                                                     'kes': [
                                                         # 'MASTER',  # KES currently have bug scaling master node
                                                         'DATA',  # KES
                                                         'COORDINATOR',  # KES
                                                         'WARM',  # KES
                                                     ],
                                                     'khbase': [
                                                         'CORE',  # KHBASE
                                                     ],
                                                 }
                                                 )),
            # 'instance_status': bool,
            'instance_type': ((str, NoNeed), partial(check_str_regex, pattern=INSTANCE_TYPE_REGEX)),
            'instance_count': (int, partial(check_num_range, low=1, high=255)),
            'volume_type': ((str, NoNeed), partial(check_within_enum,
                                                   enum={
                                                       'kes': [
                                                           'SSD3.0',
                                                           'Local_SSD',
                                                           'ESSD_PL1',
                                                           'ESSD_PL2',
                                                           'ESSD_PL3',
                                                       ],
                                                       'khbase': [
                                                           'SSD3.0',
                                                           'Local_SSD',
                                                       ],
                                                   })
                            ),
            'volume_size': ((int, NoNeed), partial(check_num_range, low=20, high=16000)),
        },
    ],
    'project_id': (int, NoNeed),
}

RESTART_CLUSTER_SYNTAX = {
    'cluster_id': (str, check_uuid),
    'rolling': (bool, NoNeed),
}
