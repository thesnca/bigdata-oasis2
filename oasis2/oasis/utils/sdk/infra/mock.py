from oasis.utils import http


async def mock_request_fail(url, data=None):
    return await http.post(url, data=data)
