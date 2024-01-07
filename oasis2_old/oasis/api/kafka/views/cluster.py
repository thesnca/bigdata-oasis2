from oasis.api import BaseView
from oasis.api.response import console_response


class ClusterView(BaseView):
    async def launch_cluster(self, *args, **kwargs):
        print(self.body.get('cluster_id'))
        return console_response({'heihei': 'pppp'})
