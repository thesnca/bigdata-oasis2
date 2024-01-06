import abc
from functools import partial
import traceback

from aiohttp import web
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp.web_response import Response

from oasis.api.response import console_response
from oasis.api.response import error_response
from oasis.db.models import OasisBase
from oasis.db.models import get_model_by_id
from oasis.db.models.user import UserModel
from oasis.utils.config import config
from oasis.utils.convert import camel2snack
from oasis.utils.convert import dict_camel2snake
from oasis.utils.convert import dict_snake2camel
from oasis.utils.convert import dict_upper2lower
from oasis.utils.exceptions import ResourceCheckError
from oasis.utils.exceptions import UserPermissionError
from oasis.utils.exceptions import ValidationError
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger
from oasis.utils.redlock import lock_request
from oasis.utils.redlock import unlock_request
from oasis.utils.sdk.iam import get_user_by_id
from oasis.utils.validation import complete_params
from oasis.utils.validation import validate_params
from oasis.utils.validation import validate_results


class BaseView(web.View, metaclass=abc.ABCMeta):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.response = console_response

    def __getattr__(self, http_method):
        def __inner(*args, **kwargs):
            async def __pre_check():
                # TODO some precheck works
                # Action Mode
                try:
                    body = await self.request.json()
                    self.body = dict_camel2snake(body)
                except:
                    self.body = {}

                test_env = config.getboolean('oasis', 'test_env', fallback=True)

                self.headers = dict_upper2lower(dict(self.request.headers))
                self.request_id = self.headers.get('x-ksc-request-id', gen_uuid4())
                self.action = self.headers.get('x-action', None)
                self.account_id = self.headers.get('x-ksc-account-id', None)
                self.region = self.headers.get('x-ksc-region', None)

                # kes\khbase for console, user for openapi
                self.source = self.headers.get('x-ksc-source', None)

                # 预发环境适应前端，强制转换
                if self.region == 'pre-online':
                    self.region = 'cn-guangzhou-1'

                self.error_response = partial(error_response, request_id=self.request_id)
                if test_env:
                    self.region = 'cn-shanghai-3'
                self.version = self.headers.get('x-ksc-version', '')

                if not self.account_id:
                    return self.error_response(msg='Please specify account id', status=402)

                account = await get_model_by_id(UserModel, self.account_id)
                if not account:
                    return self.error_response(msg=f'Account {self.account_id} not found.', status=401)

                # POST Mode
                if http_method != 'post':
                    return self.error_response(msg=f'Only support POST method, got {http_method.upper()}',
                                               status=402)

                # product: kmr kes khbase kafka
                self.product = self.request.url.parts[1]

                # check user from iam
                res = await get_user_by_id(self.account_id, self.product)
                if not res:
                    return self.error_response(msg=f'Can not find account {self.account_id}',
                                               status=401)
                self.tenant_id = res.get('tenant_id', None)
                # logger.info(self, f'Got tenant id {self.tenant_id}')
                self.user_token = res.get('user_token', None)
                # logger.info(self, f'Got token {self.user_token}')
                if not self.tenant_id:
                    return self.error_response(msg=f'Can not find tenant of account {self.account_id}',
                                               status=401)

                # API: DescribeClusters --> describe_clusters
                self.request_api = camel2snack(self.request.url.name)

                lock_res = await lock_request(self.request_id, self.action)
                if not lock_res:
                    return self.error_response(msg=f'Duplicate request id {self.request_id} '
                                                   f'and action {self.request.url.name}, '
                                                   f'please retry...',
                                               status=402)

                # All kwargs are received from body
                try:
                    logger.info(self, f'Request received, request_id: {self.request_id}, url: {self.request.url}, '
                                      f'body: {self.body}, headers:{self.headers}')
                    res = await getattr(self, self.request_api)(*args, **self.body)

                    if not res:
                        res = {'Return': res}
                    elif type(res) in (Response, HTTPBadRequest):
                        return res
                    elif isinstance(res, OasisBase):
                        res = res.to_dict()
                    elif type(res) not in [dict]:
                        res = {'Return': res}

                    res = res or {}
                    res.setdefault('request_id', self.request_id)
                    res.setdefault('status_code', 200)

                    # All response to camel
                    res = dict_snake2camel(res)
                    logger.info(self, f'Request succeed, request_id: {self.request_id}, url: {self.request.url}, '
                                      f'body: {self.body}, headers:{self.headers}, result: {res}')
                    return self.response(res)

                except UserPermissionError as e:
                    logger.error(self, f'Request failed, request_id: {self.request_id}, url: {self.request.url}, '
                                       f'body: {self.body}, headers:{self.headers}, Exception: {e}\n'
                                       f'{traceback.format_exc()}')
                    return self.error_response(msg=str(e), status=403)

                except ValidationError as e:
                    logger.error(self, f'Request failed, request_id: {self.request_id}, url: {self.request.url}, '
                                       f'body: {self.body}, headers:{self.headers}, Exception: {e}\n'
                                       f'{traceback.format_exc()}')
                    return self.error_response(msg=str(e), status=402)

                except ResourceCheckError as e:
                    logger.error(self, f'Request failed, request_id: {self.request_id}, url: {self.request.url}, '
                                       f'body: {self.body}, headers:{self.headers}, Exception: {e}\n'
                                       f'{traceback.format_exc()}')
                    return self.error_response(msg=str(e), status=405)

                except Exception as e:

                    logger.error(self, f'Request failed, request_id: {self.request_id}, url: {self.request.url}, '
                                       f'body: {self.body}, headers:{self.headers}, Exception: {e}\n'
                                       f'{traceback.format_exc()}')
                    return self.error_response(str(e))
                finally:
                    await unlock_request(self.request_id, self.action, lock_res)

            return __pre_check()

        return __inner


def openapi(func):
    async def _inner(self, *args, **kwargs):
        func_name = func.__name__
        product = self.product
        if self.source in ['kes', 'khbase']:
            if self.source != product:
                raise ValidationError(f'X-Ksc-Source: {self.source} does not match product.')
            logger.info(self, f'inner api of {self.source}: {func_name}')
        elif self.source == 'user':
            # logger.info(self, f'open api: {func_name}')

            # # Validate params
            # kwargs = validate_params(func_name, product, kwargs)
            # logger.info(self, f'params after validation: {kwargs}')

            # # Complete params
            # kwargs = await complete_params(func_name, product, kwargs)
            # logger.info(self, f'params after complete: {kwargs}')
            self.source = 'kes'
            # res = await func(self, *args, **kwargs)
            # return res
        else:
            logger.info(self, f'invalid source {self.source}: {func_name}')
            raise UserPermissionError(f'Invalid api source {self.source}, please check.')

        # res = await func(self, *args, **kwargs)

        # # open api results
        # if self.source == 'user':
        #     res = dict_snake2camel(res)

        #     res = validate_results(func_name, res)

        # return res

        res = await func(self, *args, **kwargs)
        return res

    return _inner
