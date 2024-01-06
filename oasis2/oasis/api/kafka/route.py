from oasis.api.kafka.views.cluster import ClusterView


def __app_routes():
    routes = list()
    routes.append(('/DescribeCluster', ClusterView, 'describe_cluster'))
    routes.append(('/ModifyCluster', ClusterView, 'modify_cluster'))
    routes.append(('/', ClusterView, 'launch_cluster'))
    routes.append(('/', ClusterView, 'launch_cluster'))
    return routes


API_ROUTES = __app_routes()
