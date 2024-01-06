from datetime import datetime
import math

from oasis.db.models.cluster import ClusterModel
from oasis.utils.chaos import CLUSTER_STATUS_CONVERT_MAP
from oasis.utils.convert import convert_status
from oasis.utils.convert import datetime2str


class KhbaseCluster:
    def __init__(self, cluster: ClusterModel = None):
        self.ClusterId = None
        self.Name = None
        self.ClusterType = None
        self.Distribution = None
        self.MainVersion = None
        self.TerminationProtected = None
        self.InstanceGroups = []
        self.EnableEip = None
        self.Region = None
        self.AvailabilityZone = None
        self.VpcDomainId = None
        self.VpcSubnetId = None
        self.VpcEpcSubnetId = None
        self.VpcEndPointId = None
        self.ChargeType = None
        self.PurchaseTime = None
        self.ExpireTime = None
        self.ClusterStatus = None
        self.CreateTime = None
        self.UpdateTime = None
        self.ServingMinutes = None
        self.MainInstanceId = None
        self.BindEipStatus = None
        self.SecurityGroupId = None

        if cluster is not None:
            self.convert_from(cluster)

    def convert_from(self, cluster: ClusterModel):
        self.ClusterId = cluster.id
        self.Name = cluster.name
        self.ClusterType = cluster.cluster_type
        self.Distribution = cluster.distribution_version
        self.MainVersion = cluster.main_version
        self.TerminationProtected = cluster.is_terminate_protection
        self.EnableEip = cluster.enable_eip
        self.Region = cluster.region
        self.AvailabilityZone = cluster.availability_zone
        self.VpcDomainId = cluster.vpc_domain_id
        self.VpcSubnetId = cluster.vpc_subnet_id
        for ig in cluster.instance_groups:
            if ig.resource_type == 'EPC':
                self.VpcEpcSubnetId = ig.vpc_subnet_id
        self.VpcEndPointId = cluster.vpc_endpoint_id
        self.ChargeType = cluster.charge_type
        self.PurchaseTime = cluster.purchase_time
        self.SecurityGroupId = cluster.security_group_id
        self.ClusterStatus = convert_status(
            CLUSTER_STATUS_CONVERT_MAP, cluster.status)

        created_at = datetime2str(cluster.created_at)
        updated_at = datetime2str(cluster.updated_at)
        expire_time = datetime2str(cluster.expire_time)
        if created_at:
            self.CreateTime = created_at.replace('T', ' ')
        else:
            self.CreateTime = created_at
        if updated_at:
            self.UpdateTime = updated_at.replace('T', ' ')
        else:
            self.UpdateTime = updated_at
        if expire_time and isinstance(expire_time, str):
            self.ExpireTime = expire_time.replace('T', ' ')
        else:
            self.ExpireTime = expire_time

        self.ServingMinutes = math.floor((datetime.utcnow() - cluster.created_at).total_seconds() / 60)

        for instance_group in cluster.instance_groups:
            ig = {
                'Id': instance_group['id'],
                'InstanceGroupType': instance_group['instance_group_type'],
                'ResourceType': instance_group['resource_type'],
                'InstanceType': instance_group['instance_type_code'],
                'InstanceCount': instance_group['count'],
                'VolumeSize': int(instance_group['volume_size']),
                'VolumeType': instance_group['volume_type'],
            }
            self.InstanceGroups.append(ig)

        extra = cluster.extra
        self.MainInstanceId = extra.get(
            'main_instance_id', "") if extra else ""
        self.BindEipStatus = extra.get(
            'bind_eip_status', "") if extra else ""
