from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.generator import gen_uuid4


def _prepare(func):
    async def __inner(self, *args, **kwargs):
        account_id = kwargs.pop('account_id', None)
        if not account_id:
            raise Exception(f'Please specify account_id, got {account_id}')
        request_id = kwargs.pop('request_id', gen_uuid4())

        headers = {
            'Accept': 'application/json',
            'X-Ksc-Region': self.region,
            'X-Ksc-Request-Id': request_id,
            'X-Ksc-Account-Id': account_id,
            'X-KSC-SOURCE': self.product,
            'X-KSC-SK': self.ksc_sk,
        }
        aws_headers = {
            'ak': self.ak,
            'sk': self.sk,
            'region': self.region,
            'host': self.endpoint.split('/')[2],
            'service': 'slb',
        }
        params = {
            'Version': self.version,
            'AccountId': account_id,
        }

        return await func(self, params=params, headers=headers, aws_headers=aws_headers, *args, **kwargs)

    return __inner


class SLBClient(object):
    def __init__(self, product):
        self.product = product
        self.endpoint = config.get('infra', 'slb_endpoint')
        self.ksc_sk = config.get('ksc', product)
        self.region = config.get('infra', 'region')
        self.version = config.get('infra', 'slb_version')
        self.ak = config.get('iam', f'{product}_ak')
        self.sk = config.get('iam', f'{product}_sk')

    # return load_balancer_id
    @_prepare
    async def create_load_balancer(self, suborder_id, vpc_id, load_balancer_type='public',
                                   slb_name='kes_slb', subnet_id=None, private_ip_address=None,
                                   account_id=None, params: dict = None,
                                   headers: dict = None, aws_headers=None):
        params.setdefault('Action', 'CreateLoadBalancer')
        params.setdefault('SubOrderId', suborder_id)
        params.setdefault('VpcId', vpc_id)
        params.setdefault('Type', load_balancer_type)
        params.setdefault('LoadBalancerName', slb_name)
        if load_balancer_type == 'internal' and subnet_id and private_ip_address:
            params.setdefault('SubnetId', subnet_id)
            params.setdefault('PrivateIpAddress', private_ip_address)

        code, ret = await http.post(self.endpoint, params=params,
                                    headers=headers, aws_headers=aws_headers)
        # code, ret = await mock_request_fail('http://10.69.72.79:28257/slb', {'SubOrderId': suborder_id})

        if 199 < code < 300:
            return ret.get('LoadBalancerId', None)
        raise Exception(f'slb create_load_balancer Error, return {ret}')

    @_prepare
    async def modify_load_balancer(self, load_balancer_id, load_balancer_state,
                                   account_id=None, params: dict = None,
                                   headers: dict = None, aws_headers=None):
        params.setdefault('Action', 'ModifyLoadBalancer')
        params.setdefault('LoadBalancerId', load_balancer_id)
        params.setdefault('LoadBalancerState', load_balancer_state)

        code, ret = await http.post(self.endpoint, params=params,
                                    headers=headers, aws_headers=aws_headers)
        if 199 < code < 300:
            return ret
        raise Exception(f'slb modify_load_balancer Error, return {ret}')

    # return listener_id
    @_prepare
    async def create_listeners(self, load_balancer_id, listener_port, *, listener_protocol='TCP',
                               listener_state='start', method='RoundRobin', session_state='start',
                               cookie_type='ImplantCookie',
                               account_id=None, params: dict = None,
                               headers: dict = None, aws_headers=None):
        params.setdefault('Action', 'CreateListeners')
        params.setdefault('LoadBalancerId', load_balancer_id)
        params.setdefault('ListenerProtocol', listener_protocol)
        params.setdefault('ListenerState', listener_state)
        params.setdefault('ListenerPort', listener_port)
        params.setdefault('Method', method)
        params.setdefault('SessionState', session_state)
        params.setdefault('CookieType', cookie_type)

        code, ret = await http.post(self.endpoint, params=params, headers=headers, aws_headers=aws_headers)
        if 199 < code < 300:
            return ret.get('ListenerId', None)
        raise Exception(f'slb create_listeners Error, return {ret}')

    @_prepare
    async def describe_listeners(self, load_balancer_id, *,
                                 account_id=None, params: dict = None,
                                 headers: dict = None, aws_headers=None):
        params.setdefault('Action', 'DescribeListeners')
        params.setdefault('Filter.1.Name', 'load-balancer-id')
        params.setdefault('Filter.1.Value.1', load_balancer_id)

        code, ret = await http.post(self.endpoint, params=params, headers=headers, aws_headers=aws_headers)
        if 199 < code < 300:
            return [r.get('ListenerId') for r in ret.get('ListenerSet', [])]
        raise Exception(f'slb describe_listeners Error, return {ret}')

    @_prepare
    async def describe_listeners_all(self, load_balancer_id, *,
                                     account_id=None, params: dict = None,
                                     headers: dict = None, aws_headers=None):
        '''
        {
            "ListenerSet": [
                {
                    "ListenerId": "72efd77e-46d5-4871-bbb1-8cc0df3a4523",
                    "ListenerName": "vip-tcp",
                    "Method": "RoundRobin",
                    "ListenerProtocol": "TCP",
                    "ListenerState": "start",
                    "CreateTime": "2021-12-13 15:27:53",
                    "LoadBalancerId": "bf8c51b6-8963-4397-b905-1ac1f4073330",
                    "ListenerPort": 1505,
                    "HealthCheck": {},
                    "Session": {
                        "SessionPersistencePeriod": 3600,
                        "SessionState": "stop"
                    },
                    "RealServer": [
                        {
                            "RegisterId": "f78cd2fc-b46c-4588-a7d8-ca7713860059",
                            "RealServerState": "unavailable",
                            "RealServerType": "host",
                            "ListenerId": "72efd77e-46d5-4871-bbb1-8cc0df3a4523",
                            "Weight": 100,
                            "RealServerIp": "172.1.0.55",
                            "RealServerPort": 1505,
                            "InstanceId": "14034a2e-d845-4f2e-9fad-d74982d2d8be",
                            "NetworkInterfaceId": "e63ce384-b7e4-4d82-9754-0f316c567463"
                        }
                    ],
                    "EnableHttp2": false,
                    "IpVersion": "ipv4",
                    "AsPrivateLinkServer": false,
                    "AsPrivateLink": false
                }
            ],
            "RequestId": "c609e9ac-c7f1-4be8-b9f7-e6a6259a236c"
        }
        '''
        params.setdefault('Action', 'DescribeListeners')
        params.setdefault('Filter.1.Name', 'load-balancer-id')
        params.setdefault('Filter.1.Value.1', load_balancer_id)

        code, ret = await http.post(self.endpoint, params=params, headers=headers, aws_headers=aws_headers)
        if 199 < code < 300:
            return ret.get('ListenerSet', [])
        return []

    # return health_check_id
    @_prepare
    async def configure_health_check(self, listener_id, *, health_check_state='start',
                                     healthy_threshold=5, interval=5, time_out=4, unhealthy_threshold=4,
                                     account_id=None, params: dict = None, headers: dict = None,
                                     aws_headers=None):
        params.setdefault('Action', 'ConfigureHealthCheck')
        params.setdefault('ListenerId', listener_id)
        params.setdefault('HealthCheckState', health_check_state)
        params.setdefault('HealthyThreshold', str(healthy_threshold))
        params.setdefault('Interval', str(interval))
        params.setdefault('Timeout', str(time_out))
        params.setdefault('UnhealthyThreshold', str(unhealthy_threshold))

        code, ret = await http.post(self.endpoint, params=params, headers=headers, aws_headers=aws_headers)
        if 199 < code < 300:
            return ret.get('HealthCheckId', None)
        raise Exception(f'slb configure_health_check Error, return {ret}')

    # return register_id
    @_prepare
    async def register_instances_with_listener(self, listener_id, real_server_ip, real_server_port, *,
                                               weight=1, real_server_type='host',
                                               account_id=None, params: dict = None, headers: dict = None,
                                               aws_headers=None):
        params.setdefault('Action', 'RegisterInstancesWithListener')
        params.setdefault('ListenerId', listener_id)
        params.setdefault('RealServerType', real_server_type)
        params.setdefault('RealServerIp', real_server_ip)
        params.setdefault('RealServerPort', real_server_port)
        params.setdefault('Weight', str(weight))

        code, ret = await http.post(self.endpoint, params=params, headers=headers, aws_headers=aws_headers)
        if 199 < code < 300:
            return ret.get('RegisterId', None)
        raise Exception(f'slb register_instances_with_listener Error, return {ret}')

    @_prepare
    async def deregister_instances_from_listener(self, register_id,
                                                 account_id=None, params: dict = None,
                                                 headers: dict = None, aws_headers=None):
        params.setdefault('Action', 'DeregisterInstancesFromListener')
        params.setdefault('RegisterId', register_id)

        code, ret = await http.post(self.endpoint, params=params,
                                    headers=headers, aws_headers=aws_headers)
        if 199 < code < 300 or code == 404:
            return ret
        raise Exception(f'slb deregister_instances_with_listener Error, return {ret}')

    @_prepare
    async def delete_health_check(self, health_check_id,
                                  account_id=None, params: dict = None,
                                  headers: dict = None, aws_headers=None):
        params.setdefault('Action', 'DeleteHealthCheck')
        params.setdefault('HealthCheckId', health_check_id)
        code, ret = await http.post(self.endpoint, params=params,
                                    headers=headers, aws_headers=aws_headers)
        if 199 < code < 300:
            return ret
        raise Exception(f'slb delete_health_check Error, return {ret}')

    @_prepare
    async def delete_listeners(self, listener_id,
                               account_id=None, params: dict = None,
                               headers: dict = None, aws_headers=None):
        params.setdefault('Action', 'DeleteListeners')
        params.setdefault('ListenerId', listener_id)

        code, ret = await http.post(self.endpoint, params=params,
                                    headers=headers, aws_headers=aws_headers)
        if 199 < code < 300 or code == 404:
            return ret
        raise Exception(f'slb delete_listeners Error, return {ret}')

    @_prepare
    async def delete_load_balancer(self, load_balancer_id,
                                   account_id=None, params: dict = None,
                                   headers: dict = None, aws_headers=None):
        params.setdefault('Action', 'DeleteLoadBalancer')
        params.setdefault('LoadBalancerId', load_balancer_id)

        code, ret = await http.post(self.endpoint, params=params,
                                    headers=headers, aws_headers=aws_headers)
        if 199 < code < 300 or code == 404:
            return ret

    @_prepare
    async def describe_load_balancers(self, load_balancer_ids,
                                      account_id=None, params: dict = None,
                                      headers: dict = None, aws_headers=None):
        '''
        {
            "RequestId": "a055018f-c8c7-4b83-a048-6cde7f7f7017",
            "LoadBalancerDescriptions": [
                {
                    "LoadBalancerId": "bf8c51b6-8963-4397-b905-1ac1f4073330",
                    "LoadBalancerName": "kmr-pool",
                    "IsWaf": false,
                    "Type": "public",
                    "VpcId": "20bf8aaf-38aa-4688-ac9d-b6dc99fee598",
                    "PublicIp": "198.18.0.164",
                    "LoadBalancerState": "start",
                    "CreateTime": "2021-12-13 15:27:53",
                    "ListenersCount": 1,
                    "ProjectId": "0",
                    "State": "associate",
                    "IpVersion": "ipv4",
                    "UserTag": "console",
                    "LbType": "classic",
                    "LbStatus": "active"
                }
            ],
            "TotalCount": 1
        }
        '''
        params.setdefault('Action', 'DescribeLoadBalancers')

        for i, lb_id in enumerate(load_balancer_ids, 1):
            params.setdefault(f'LoadBalancerId.{i}', lb_id)

        code, ret = await http.post(self.endpoint, params=params,
                                    headers=headers, aws_headers=aws_headers)
        if 199 < code < 300:
            lines = ret.get('LoadBalancerDescriptions', [])
            return lines
        return []
