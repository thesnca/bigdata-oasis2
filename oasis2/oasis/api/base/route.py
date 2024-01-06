from oasis.api.base.views.action import ActionView
from oasis.api.base.views.charge import ChargeView
from oasis.api.base.views.cube import CubeView
from oasis.api.base.views.iam import IamView
from oasis.api.base.views.job import JobView
from oasis.api.base.views.operation import OperationView
from oasis.api.base.views.platform import PlatformView
from oasis.api.base.views.query import QueryView


def __app_routes():
    routes = list()

    # Charge
    routes.append(('/CreateProducts', ChargeView))
    routes.append(('/UpgradeProducts', ChargeView))

    # Job
    routes.append(('/DescribeJob', JobView))
    routes.append(('/RetryJob', JobView))
    routes.append(('/RollbackJob', JobView))

    # IAM
    routes.append(('/CreateSlbProduct', IamView))
    routes.append(('/CreateInternalEip', IamView))
    routes.append(('/BindInternalEip', IamView))
    routes.append(('/ListKS3Buckets', IamView))
    routes.append(('/BindEip', IamView))
    routes.append(('/UnbindEip', IamView))
    routes.append(('/BindPrivateSlb', IamView))
    routes.append(('/UnbindPrivateSlb', IamView))
    routes.append(('/CheckConnectivityStatus', IamView))
    routes.append(('/GetLinkInfos', IamView))

    # Common Action
    routes.append(('/ModifyClusterBillingInfo', ActionView))

    # Common Query
    routes.append(('/VerifyUserPermissions', QueryView))
    routes.append(('/DescribeUser', QueryView))
    routes.append(('/DescribeVpcs', QueryView))
    routes.append(('/DescribeSubnets', QueryView))
    routes.append(('/DescribeEips', QueryView))
    routes.append(('/DescribeEipAddresses', QueryView))
    routes.append(('/DescribeSecurityGroups', QueryView))
    routes.append(('/ListClusterOrderInstances', QueryView))
    routes.append(('/GetEsAddress', QueryView))
    routes.append(('/FetchClusterByExtraInstance', QueryView))
    routes.append(('/CheckInstanceItems', QueryView))

    # Cubricks v1.1
    routes.append(('/ScaleNotification', CubeView))
    routes.append(('/ConfigNic', CubeView))
    routes.append(('/AddIptablesRules', CubeView))

    # Operation
    routes.append(('/OpCreateUser', OperationView))
    routes.append(('/OpDeleteUser', OperationView))
    routes.append(('/OpUpdateUser', OperationView))
    routes.append(('/OpDescribeUser', OperationView))
    routes.append(('/OpListUsers', OperationView))
    routes.append(('/OpVerifyUserPermissions', OperationView))
    routes.append(('/OpListClusters', OperationView))
    routes.append(('/OpListClusterJobs', OperationView))
    routes.append(('/OpDescribeCluster', OperationView))
    routes.append(('/OpListClusterNodes', OperationView))
    routes.append(('/OpQueryOrder', OperationView))
    routes.append(('/OpNotifyOrder', OperationView))
    routes.append(('/OpControlHttpReferer', OperationView))
    routes.append(('/OpModifyCluster', OperationView))
    routes.append(('/OpStartInstance', OperationView))

    # Operation
    routes.append(('/BindTags', PlatformView))
    routes.append(('/CheckTags', PlatformView))
    routes.append(('/ListTagValues', PlatformView))
    routes.append(('/ListTagKeys', PlatformView))
    routes.append(('/CreateTag', PlatformView))
    return routes


BASE_ROUTES = __app_routes()
