from oasis.api import BaseView
from oasis.api.response import console_response


class ClusterView(BaseView):
    async def describe_cluster(self, *args, **kwargs):
        # TODO query db
        clusters = [
            {
                'name': 'cluster1',
            },
            {
                'name': 'cluster2',
            },
            {
                'name': 'cluster3',
            },
        ]
        return console_response({'clusters': clusters})
