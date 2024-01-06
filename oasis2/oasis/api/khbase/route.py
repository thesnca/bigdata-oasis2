from oasis.api.khbase.views.cluster import ClusterView
from oasis.api.khbase.views.gringotts import GringottsView
from oasis.api.khbase.views.gringotts_monitor import GringottsMonitorView
from oasis.api.khbase.views.instance_group import InstanceGroupView
from oasis.api.khbase.views.query import QueryView


# from oasis.api.khbase.views.test import TestView


def __app_routes():
    routes = list()

    # Cluster
    routes.append(('/DescribeCluster', ClusterView))
    routes.append(('/ListClusters', ClusterView))
    routes.append(('/ModifyClusterName', ClusterView))
    routes.append(('/LaunchCluster', ClusterView))
    routes.append(('/DeleteClusterProtection', ClusterView))
    routes.append(('/DeleteCluster', ClusterView))
    routes.append(('/FreezeCluster', ClusterView))
    routes.append(('/UnfreezeCluster', ClusterView))

    # InstanceGroup
    routes.append(('/ListInstanceGroups', InstanceGroupView))
    routes.append(('/ScaleInInstanceGroups', InstanceGroupView))
    routes.append(('/ScaleOutInstanceGroups', InstanceGroupView))
    routes.append(('/UpgradeInstanceGroups', InstanceGroupView))

    # Gringotts
    routes.append(('/ClusterServiceRestartCheck', GringottsView))
    routes.append(('/ServiceControl', GringottsView))
    routes.append(('/RestartCluster', GringottsView))
    routes.append(('/UpdateServiceConfiguration', GringottsView))
    routes.append(('/ListServiceConfigurations', GringottsView))
    routes.append(('/ListServiceConfigurationHistory', GringottsView))
    routes.append(('/DescribeClusterOperation', GringottsView))
    routes.append(('/ListClusterOperations', GringottsView))
    routes.append(('/CheckClusterIdle', GringottsView))
    routes.append(('/UpdateComponentConfiguration', GringottsView))

    # Gringotts for KHBASE
    routes.append(('/ComponentControl', GringottsView))
    routes.append(('/ListServicesIdle', GringottsView))
    routes.append(('/ListConnections', GringottsView))

    # Gringotts Monitor
    routes.append(('/ListMetrics', GringottsMonitorView))
    routes.append(('/ListClusterStatus', GringottsMonitorView))
    routes.append(('/GetClusterStatus', GringottsMonitorView))

    # Others
    routes.append(('/ListDistributions', QueryView))
    routes.append(('/ListComprehensiveStatus', QueryView))
    routes.append(('/CheckSecurityGroup', QueryView))

    # routes.append(('/TestError', TestView))

    return routes


API_ROUTES = __app_routes()
