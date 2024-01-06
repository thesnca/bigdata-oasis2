from oasis.api.kmr.views.cluster import ClusterView
from oasis.api.kmr.views.instance_group import InstanceGroupView


def __app_routes():
    routes = list()
    routes.append(('/DescribeCluster', ClusterView))
    routes.append(('/ModifyCluster', ClusterView))
    routes.append(('/ListInstanceGroups', InstanceGroupView))
    routes.append(('/ModifyInstanceGroups', InstanceGroupView))
    return routes


API_ROUTES = __app_routes()
