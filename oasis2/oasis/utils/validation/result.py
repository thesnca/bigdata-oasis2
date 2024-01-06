from functools import partial

from oasis.utils.convert import convert_volume_type

LIST_CLUSTERS_RESULT = {
    'Clusters': [
        {
            'ClusterId': str,
            ('ClusterName', 'Name'): str,
            'MainVersion': str,
            'InstanceGroups': [
                {
                    'Id': str,
                    'InstanceGroupType': str,
                    'ResourceType': str,
                    'InstanceType': str,
                    'InstanceCount': str,
                }
            ],
            'EnableEip': bool,
            'EipLineId': str,
            'Region': str,
            'AvailabilityZone': str,
            'VpcDomainId': str,
            'VpcSubnetId': str,
            'ChargeType': str,
            'PurchaseTime': int,
            'ExpireTime': str,
            'ClusterStatus': str,
            'CreateTime': str,
            'UpdateTime': str,
            'ServingMinutes': int,
        }
    ],
    'Total': int,
}

DESCRIBE_CLUSTER_RESULT = {
    'ClusterId': str,
    ('ClusterName', 'Name'): str,
    'ClusterType': str,
    'MainVersion': str,
    'InstanceGroups': [
        {
            'Id': str,
            'InstanceGroupType': str,
            'ResourceType': str,
            'InstanceType': str,
            'InstanceCount': str,
            'VolumeSize': int,
            'VolumeType': (str, partial(convert_volume_type, revert=False)),
        }
    ],
    'EnableEip': bool,
    'Eip': str,
    'Region': str,
    'AvailabilityZone': str,
    'VpcDomainId': str,
    'VpcSubnetId': str,
    'ChargeType': str,
    'PurchaseTime': int,
    'ExpireTime': str,
    'ClusterStatus': str,
    'CreateTime': str,
    'UpdateTime': str,
    'ServingMinutes': int,
    'ProxyPort': int,
    'ProxyUrlSuffix': str,
    'SlbId': str,
}

LAUNCH_CLUSTER_RESULT = {
    'ClusterId': str,
}

SCALE_OUT_INSTANCE_GROUPS_RESULT = {
    'ClusterId': str,
}

RESTART_CLUSTER_RESULT = {
    'ClusterId': str,
}
