from aiohttp import web

from oasis.api.base.route import BASE_ROUTES


class WebService:
    def __init__(self, app_name, routes, conf):
        self.app = web.Application()
        self.app_name = app_name
        self.conf = conf
        self.version = self.conf.pop('version', 'v1')
        self.init_route(routes)

    def init_route(self, routes):
        # POST Mode
        base_routes = BASE_ROUTES
        routes.extend(base_routes)
        web_routes = [getattr(web, 'post')(f'/{self.app_name}/{self.version}{route}', handler)
                      for route, handler in routes]
        # for route, handler in routes:
        #     print(f'/{self.app_name}/{self.version}{route}', handler)
        self.app.add_routes(web_routes)

    def run(self, port=None):
        port = port or int(self.conf.pop('port', 18080))
        web.run_app(self.app,
                    port=port,
                    )
