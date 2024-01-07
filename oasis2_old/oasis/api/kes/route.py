from oasis.api.kes.views.cluster import ClusterView
from oasis.api.kes.views.gringotts import GringottsView
from oasis.api.kes.views.gringotts_monitor import GringottsMonitorView
from oasis.api.kes.views.instance_group import InstanceGroupView
from oasis.api.kes.views.query import QueryView
from oasis.api.kes.views.plugin import PluginView


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
    routes.append(('/RollingRestartInstanceGroups', InstanceGroupView))

    # Gringotts
    routes.append(('/ClusterServiceRestartCheck', GringottsView))
    routes.append(('/ServiceControl', GringottsView))
    routes.append(('/RestartCluster', GringottsView))
    routes.append(('/UpdateServiceConfiguration', GringottsView))
    routes.append(('/ListServiceConfigurations', GringottsView))
    routes.append(('/ListServiceConfigurationHistory', GringottsView))
    routes.append(('/UpdateComponentConfiguration', GringottsView))
    routes.append(('/ListComponentConfigurations', GringottsView))
    routes.append(('/ListComponentConfigurationHistory', GringottsView))

    routes.append(('/DescribeClusterOperation', GringottsView))
    routes.append(('/ListClusterOperations', GringottsView))
    routes.append(('/CheckClusterIdle', GringottsView))

    # Gringotts for KES
    routes.append(('/SnapshotOn', GringottsView))
    routes.append(('/SnapshotOff', GringottsView))
    routes.append(('/SnapshotStatus', GringottsView))
    routes.append(('/SnapshotHistory', GringottsView))
    routes.append(('/QueryLogFirstTime', GringottsView))
    routes.append(('/GetNodesIpAddressStr', GringottsView))
    routes.append(('/EnableXpack', GringottsView))
    routes.append(('/DisableXpack', GringottsView))
    routes.append(('/ResetXpackPassword', GringottsView))
    routes.append(('/CheckXpackStatus', GringottsView))
    routes.append(('/ClusterHealthStatistic', GringottsView))
    routes.append(('/CheckClusterHealth', GringottsView))
    routes.append(('/ListPlugin', GringottsView))
    routes.append(('/InstallPlugin', GringottsView))
    routes.append(('/UninstallPlugin', GringottsView))
    routes.append(('/RestartCheck', GringottsView))
    routes.append(('/GetWebServiceInfo', GringottsView))
    routes.append(('/FetchIpAddressStr', GringottsView))
    routes.append(('/EnableLogCollection', GringottsView))
    routes.append(('/DisableLogCollection', GringottsView))
    routes.append(('/ScrollLogForce', GringottsView))
    routes.append(('/GetEsFreeNodes', GringottsView))

    # Gringotts Monitor
    routes.append(('/ListMetrics', GringottsMonitorView))
    routes.append(('/ListClusterStatus', GringottsMonitorView))
    routes.append(('/GetClusterStatus', GringottsMonitorView))

    # Others
    routes.append(('/ListDistributions', QueryView))
    routes.append(('/ListComprehensiveStatus', QueryView))
    routes.append(('/CheckSecurityGroup', QueryView))

    # User Define Plugins
    routes.append(('/GetKs3PresignedUrl', PluginView))
    routes.append(('/ListPlugins', PluginView))  # system default + user define
    routes.append(('/AddUserPlugin', PluginView))
    routes.append(('/InstallUserPlugin', PluginView))
    routes.append(('/UninstallUserPlugin', PluginView))
    routes.append(('/DeleteUserPlugin', PluginView))

    return routes


API_ROUTES = __app_routes()
