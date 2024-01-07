import re

from oasis.utils.convert import snake2camel
from oasis.utils.exceptions import ValidationError

NoNeed = type(None)
CLUSTER_NAME_REGEX = r'^[A-Za-z][A-Za-z0-9-_]{0,24}$'
INSTANCE_TYPE_REGEX = r'^[A-Z-]+.\w+.\d+C\d+G$'
MARKER_REGEX = r'^limit=\d+&offset=\d+$'


def check_value(key, val, valid_type, product=None, *valid_funcs):
    if isinstance(valid_type, tuple):
        valid_type = valid_type[0]

    if valid_type is bool:
        if isinstance(val, str):
            if val.lower() in ['false', '0']:
                val = False
            elif val.lower() in ['true', '1']:
                val = True
        if not isinstance(val, bool) and val not in [0, 1]:
            raise ValidationError(f'\'{snake2camel(key)}\' invalid, '
                                  f'got {type(val).__name__} ({val}), '
                                  f'expect {valid_type.__name__}.')
        val = bool(val)

    if not isinstance(val, valid_type):
        raise ValidationError(f'\'{snake2camel(key)}\' invalid, '
                              f'got {type(val).__name__} ({val}), '
                              f'expect {valid_type.__name__}.')
    if valid_funcs:
        for valid_func in valid_funcs:
            if callable(valid_func):
                valid_func(key, val, product=product)
            else:
                raise Exception(f'Wrong valid_func, please contact admin.')
    return val


def check_str_length(key, val, low: int, high: int, product=None):
    if len(val) < low or len(val) > high:
        raise ValidationError(f'\'{snake2camel(key)}\' length should between {low} and {high}, '
                              f'got {len(val)} ({val}).')
    return True


def check_str_regex(key, val, pattern: str, product=None):
    if not re.match(pattern, val):
        raise ValidationError(f'\'{snake2camel(key)}\' pattern invalid, got {val}.')
    return True


def check_uuid(key, val, product=None):
    from uuid import UUID
    try:
        UUID(val)
    except:
        raise ValidationError(f'\'{snake2camel(key)}\' should be UUID, got {val}.')
    return True


def check_num_range(key, val, low: int = None, high: int = None, product=None):
    if low is not None and val < low:
        raise ValidationError(f'\'{snake2camel(key)}\' should greater than {low}, '
                              f'got {val}.')

    if high is not None and val > high:
        raise ValidationError(f'\'{snake2camel(key)}\' should less than {high}, '
                              f'got {val}.')
    return True


def check_within_enum(key, val, enum, product=None):
    if isinstance(enum, dict) and product:
        enum = enum.get(product, [])

    if not isinstance(enum, list):
        raise ValidationError(f'\'{snake2camel(key)}\' check failed, please contact admin.')
    if val not in enum:
        raise ValidationError(f'\'{snake2camel(key)}\' invalid, got {val}.')
    return True
