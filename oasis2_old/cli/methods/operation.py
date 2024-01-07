from termcolor import cprint

from oasis.db.models import get_model_by_id
from oasis.db.models.user import UserModel
from oasis.utils.sdk.iam import get_user_by_id


async def add_user(kuser_id, **kwargs):
    user = await get_model_by_id(UserModel, kuser_id)
    if user:
        cprint(f'Add user failed, User {kuser_id} already exist, alias {user.company_alias}', 'red')
        return False

    product = kwargs.get('product', 'kes')
    res = await get_user_by_id(kuser_id, product)
    tenant_id = res.get('tenant_id', None)
    if not tenant_id:
        cprint(f'Add user failed, User {kuser_id} does not have tenant id.', 'red')
        return False

    user = UserModel()
    user.id = kuser_id
    user.tenant_id = tenant_id
    user.role = 'normal_user'
    user.allocator = '4337ad7994864dc189410771efcd31f2'
    user.update(kwargs)
    await user.save()

    return True
