# kec instance need order_id, so cluster create
from oasis.db.models import get_model_by_id
from oasis.db.models.instance import InstanceModel
from oasis.db.models.instance_group import InstanceGroupModel
from oasis.utils import sdk
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger
from oasis.utils.sdk import charge_client


async def create_kec_instance_cluster(kec_param_list, order_id,
                                      account_id, kec_client):
    instance_results = await kec_client.create_instances(kec_param_list, order_id, account_id=account_id)

    instance_dict = {
        res.get('InstanceId', ''): {
            'instance_name': res.get('InstanceName', ''),
            'service_instance_id': gen_uuid4(),  # Generate service instance id
        }
        for ir in instance_results for res in ir
    }

    # Wait KEC generate instance id and notify order
    instance_suborder_dict = await charge_client.wait_suborders_by_instance_id_list(
        instance_id_list=list(instance_dict.keys()))

    logger.info(f'==instance_dict: {instance_dict}')
    logger.info(f'==instance_suborder_dict: {instance_suborder_dict}')

    if not instance_suborder_dict:
        raise Exception(f'==instance_suborder_dict is {instance_suborder_dict}')

    return instance_dict, instance_suborder_dict


async def instance_add(instance_group_id, instance_info):
    instance_model = InstanceModel()
    instance_model.update(instance_info)
    instance_model.instance_group_id = instance_group_id
    instance_model.status = InstanceModel.STATUS.ACTIVE
    await instance_model.save()

    instance_group_model = await get_model_by_id(InstanceGroupModel, instance_group_id)
    await instance_group_model.save({'count': instance_group_model.count + 1})

    return instance_model.instance_id


async def create_eip(cluster, eip_order_id, eip_line_id, eip_charge_type, project_id, eip_purchase_time,
                     account_id=None, band_width=None):
    eip_client = getattr(sdk, f'eip_client_{cluster.cluster_type.lower()}')

    allocate_address_id, ip_addr = await eip_client.get_allocate_address_id(eip_charge_type,
                                                                            eip_order_id,
                                                                            eip_line_id,
                                                                            project_id,
                                                                            eip_purchase_time,
                                                                            account_id=account_id,
                                                                            band_width=band_width)
    logger.info(f'==create_eip==> allocate_address_id: {allocate_address_id}, ip_addr: {ip_addr}')

    return ip_addr, allocate_address_id
