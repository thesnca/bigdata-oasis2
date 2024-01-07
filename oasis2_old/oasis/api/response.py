from datetime import datetime

from aiohttp import web

from oasis.utils.convert import datetime2str


def check_data(func):
    def _check(data, **kwargs):
        def __validate(d):
            if isinstance(d, dict):
                return {k: __validate(v) for k, v in d.items()}
            elif isinstance(d, list):
                return [__validate(v) for v in d]
            elif isinstance(d, datetime):
                return datetime2str(d)
            else:
                return d

        data = __validate(data)
        return func(data, **kwargs)

    return _check


@check_data
def console_response(data, status=200, reason=None):
    headers = {

    }
    return web.json_response(data, status=status, reason=reason, headers=headers)


def text_response(text, status=200):
    return web.Response(text=text, status=status)


def error_response(msg, request_id=None, status=400, reason=None):
    headers = {
    }
    data = {
        'RequestId': request_id,
        'Error': {
            'Code': status,
            'Message': msg,
        }
    }
    return web.json_response(data, status=status, reason=msg, headers=headers)

# TODO add more response
