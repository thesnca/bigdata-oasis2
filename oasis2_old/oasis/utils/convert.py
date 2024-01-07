from conf.infra_conf import VOLUME_TYPE_MAP


def camel2snack(camel_str):
    import re
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', camel_str).lower()


def snake2camel(snake_str):
    if not snake_str:
        return snake_str
    if '_' in snake_str:
        return ''.join([s.title() for s in snake_str.split('_')])
    return snake_str[0].upper() + snake_str[1:]


def dict_camel2snake(camel_dict):
    if type(camel_dict) == dict:
        return {camel2snack(k): dict_camel2snake(v) for k, v in camel_dict.items()}
    elif type(camel_dict) == list:
        return [dict_camel2snake(v) for v in camel_dict]
    else:
        return camel_dict


def dict_snake2camel(snake_dict):
    if type(snake_dict) == dict:
        return {snake2camel(k): dict_snake2camel(v) for k, v in snake_dict.items()}
    elif type(snake_dict) == list:
        return [dict_snake2camel(v) for v in snake_dict]
    else:
        return snake_dict


def dict_upper2lower(upper_dict):
    if type(upper_dict) == dict:
        return {k.lower(): dict_upper2lower(v) for k, v in upper_dict.items()}
    elif type(upper_dict) == list:
        return [dict_upper2lower(v) for v in upper_dict]
    else:
        return upper_dict


def datetime2str(date):
    from datetime import datetime
    if not isinstance(date, datetime):
        return date
    value = date.isoformat('T')
    ms_delimiter = value.find(".")
    if ms_delimiter != -1:
        # Removing ms from time
        value = value[:ms_delimiter]
    return value


def str2datetime(date_str):
    from datetime import datetime
    if not date_str or type(date_str) != str:
        return None
    date = datetime.strptime(date_str.replace('T', ' '), "%Y-%m-%d %H:%M:%S")
    return date


def list2dict(str1, attrs=None):
    if not attrs:
        return {}
    dic1 = {f'{str1}.{index}': instance for index, instance in enumerate(attrs, 1)}
    return dic1


def translate_marker_str(marker_str):
    marker = {'offset': 0, 'limit': 100}
    marker_list = marker_str.lower().replace(' ', '').split('&')
    try:
        for item in marker_list:
            key, valstr = item.split("=")
            val = int(valstr)
            marker[key] = val
    except Exception:
        pass
    finally:
        return marker


def replace_wildcards(sql_str):
    wildcards = ['%', '_', '[', ']', '^', '!']
    for wildcard in wildcards:
        sql_str = sql_str.replace(wildcard, '\\' + wildcard)
    return sql_str


def convert_status(status_map, state):
    for key, values in status_map.items():
        if state in values:
            return key
        else:
            for value in values:
                if state == value or state.startswith(value + ":"):
                    return key
    return "UnknownStatus"


def convert_volume_type(volume_type, revert=True):
    type_map = VOLUME_TYPE_MAP
    if revert:
        type_map = {v: k for k, v in type_map.items()}

    return type_map.get(volume_type)
