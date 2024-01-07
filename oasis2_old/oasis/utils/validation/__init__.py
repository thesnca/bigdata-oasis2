from oasis.utils.convert import snake2camel
from oasis.utils.exceptions import ValidationError
from oasis.utils.validation import completion
from oasis.utils.validation import result
from oasis.utils.validation import syntax
from oasis.utils.validation.cheking import NoNeed
from oasis.utils.validation.cheking import check_value


def validate_params(func, product, params):
    def __check_params_by_syn(param, syn):
        new_params = {}
        for syn_k, syn_v in syn.items():
            val = param.get(syn_k, None)
            is_list = False

            if isinstance(syn_v, list):
                syn_v = syn_v[0]
                is_list = True

            # Simply check type
            syn_type = syn_v
            syn_funcs = []
            if isinstance(syn_v, tuple) and NoNeed not in syn_type:  # Check type and format
                syn_type = syn_v[0]
                syn_funcs = syn_v[1:] if len(syn_v) > 1 else []
            elif isinstance(syn_v, dict):  # Check dict
                if is_list:
                    if not isinstance(val, list):
                        raise ValidationError(f'\'{snake2camel(syn_k)}\' should be list of dict, '
                                              f'got {val}.')

                    validation_list = []
                    for v in val:
                        if not isinstance(v, dict):
                            raise ValidationError(f'\'{snake2camel(syn_k)}\' should be list of dict, '
                                                  f'got {val}.')
                        valid = __check_params_by_syn(v, syn_v)
                        validation_list.append(valid)
                    new_params.setdefault(syn_k, validation_list)
                else:
                    if not isinstance(val, dict):
                        raise ValidationError(f'\'{snake2camel(syn_k)}\' should be dictionary, '
                                              f'got {val}.')
                    validation_dict = __check_params_by_syn(val, syn_v)
                    new_params.setdefault(syn_k, validation_dict)
                continue

            if val is None:
                if isinstance(syn_type, tuple) and NoNeed in syn_type:
                    new_params.setdefault(syn_k, val)
                    continue

                raise ValidationError(f'\'{snake2camel(syn_k)}\' should be {syn_type}, '
                                      f'got {val}.')

            if is_list:
                if not isinstance(val, list):
                    raise ValidationError(f'\'{snake2camel(syn_k)}\' should be list, '
                                          f'got {val}.')
                val = [check_value(syn_k, v, syn_type, product, *syn_funcs) for v in val]
            else:
                val = check_value(syn_k, val, syn_type, product, *syn_funcs)

            new_params.setdefault(syn_k, val)

        return new_params

    param_syntax = getattr(syntax, f'{func}_syntax'.upper())
    return __check_params_by_syn(params, param_syntax)


def validate_results(func, res):
    def __valid_res(res_dict, temp):
        new_res_dict = {}
        for tem_k, tem_v in temp.items():
            # Convert result key
            if isinstance(tem_k, tuple):
                val = res_dict.get(tem_k[1], None)
                tem_k = tem_k[0]
            else:
                val = res_dict.get(tem_k, None)

            # Convert result value
            if isinstance(tem_v, tuple):
                convert_func = tem_v[1]
                val = convert_func(val)
                tem_v = tem_v[0]

            is_list = False
            if isinstance(tem_v, list):
                tem_v = tem_v[0]
                is_list = True

            # There is no list in list

            # If value is None, don't return to openapi user
            if val is None:
                continue

            if isinstance(tem_v, type):
                if not isinstance(val, tem_v):
                    continue

            elif isinstance(tem_v, dict):
                if is_list:
                    val = [__valid_res(v, tem_v) for v in val]

                elif not isinstance(val, dict):
                    continue

                else:
                    val = __valid_res(val, tem_v)

            new_res_dict.setdefault(tem_k, val)

        return new_res_dict

    res_template = getattr(result, f'{func}_result'.upper())
    return __valid_res(res, res_template)


async def complete_params(func, product, params):
    new_params = await getattr(completion, f'{func}_completion')(product, params)
    return new_params
