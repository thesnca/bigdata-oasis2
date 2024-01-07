from oasis.utils import http
from oasis.utils.config import config


class PriceClient:
    def __init__(self):
        self.endpoint = config.get('charge', 'price_uri')

    async def get_product_details(self, account_id, product_group_id, operate_type):
        """

        :param account_id:
        :param product_group_id:
        :param operate_type:
        :return: {"KES_EPC":{}, "KES_EBS":{"ES.basic.4C4G": {}}}
        """
        product_details = {}
        params = {
            'userId': account_id,
            'productGroupId': str(product_group_id),
            'operateType': str(operate_type),
        }
        code, ret = await http.get(f'{self.endpoint}/product/details', params=params)

        if 199 < code < 300:
            data = ret.get('data', {})
            product_type_list = data.get('productTypeList', [])
            for product_type in product_type_list:
                product_type_code = product_type['productTypeCode']

                pk_dict = {}
                package_list = product_type.get('packageList', [])
                for package in package_list:
                    package_code = package['packageCode']
                    package_infos = package.get('packageInfo', [])

                    pk_info = {}
                    for pi in package_infos:
                        if pi['propCode'] == 'KEC_type':
                            pk_info['kec_type'] = pi['propValue'][0]['value']
                        elif pi['propCode'] == 'KEC_ebs':
                            pk_info['kec_ebs'] = pi['propValue'][0]['value']
                        elif pi['propCode'] == 'ProductType_code':
                            pk_info['product_type'] = pi['propValue'][0]['value']
                        elif pi['propCode'] == 'KEC_SSD':
                            pk_info['kec_ssd'] = pi['propValue'][0]['max']
                        elif pi['propCode'] == 'disk_num':  # 直连盘
                            pk_info['local_disk'] = pi['propValue'][0]['value']

                    pk_dict.setdefault(package_code, pk_info)
                product_details.setdefault(product_type_code, pk_dict)
        return product_details

    async def get_eip_instance_type(self, user_id, region, value=9):
        params = {
            'userId': user_id,
            'productGroupId': 102,
            'operateType': 1,
            'productTypeId': value,
            'region': region,
        }

        eip_instance_type = None
        max_net = 0
        code, ret = await http.get(f'{self.endpoint}/product/details', params=params)

        if 199 < code < 300 and ret is not None:
            if 'data' in ret and 'productTypeList' in ret['data']:
                product_type = ret['data']['productTypeList'][0]
                package_list = product_type['packageList']
                for package in package_list:
                    package_code = package['packageCode']
                    for pi in package['packageInfo']:
                        if pi['propCode'] == 'net':
                            t_net = pi['propValue'][0]['max']
                            if t_net > max_net:
                                max_net = t_net
                                eip_instance_type = package_code
        # logger.warn('===============================get_eip_instance_type: %s', eip_instance_type)
        return eip_instance_type

    async def get_slb_bill_type(self, user_id):
        params = {
            'userId': user_id,
            'productGroupId': 105,
            'operateType': 1,
            'productTypeId': 5,
        }
        bill_type = 801
        code, ret = await http.get(f'{self.endpoint}/product/details', params=params)

        if 199 < code < 300 and ret is not None:
            if 'data' in ret and 'productTypeList' in ret['data']:
                product_type = ret['data']['productTypeList'][0]
                bill_type_list = product_type['billTypeList']
                for bt in bill_type_list:
                    if bt['billTypeId'] == 805:
                        bill_type = 805
        return bill_type

    async def calculate_product(self, items):
        payload = items
        code, ret = await http.post(f'{self.endpoint}/product/calculateProduct', data=payload)

        if 'data' in ret:
            return ret['data']['price']
        else:
            return None
