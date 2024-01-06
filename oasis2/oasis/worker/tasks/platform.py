import asyncio
import traceback
from conf.infra_conf import TAG_REP

from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.eip import EIPModel
from oasis.utils import sdk
from oasis.utils.logger import logger
from oasis.utils.sdk.platform.tag import TagResource
from oasis.worker.tasks import BaseTask
from oasis.worker.tasks import check_rollback
from oasis.worker.tasks import check_task


class TaskReplaceResourcesTags(BaseTask):
    @check_task
    async def run(self):
        '''
        业务梳理:
        tag->绑定集群->绑定各业务实例
        '''
        try:
            account_id = self.args.get('account_id', '')
            cluster_id = self.args.get('cluster_id', None)
            new_instance_ids = self.args.get('new_instance_ids', None)

            # all 手动修改
            # new 新建集群
            # scale 扩容
            # bing_eip 绑定eip或绑定slb
            exec_mode = self.args.get('exec_mode', None)

            # [{"TagKey":"你好","TagValue":"再见","TagId":"123"}]
            tags = self.args.get('tags', [])

            # TODO tag这个需求初衷，并不希望以复杂的设计来完成。
            # 我使用了一个参数来区分，绑定/解绑（绑定空）操作。
            # 这是一个不好的设计，会导致一些分支逻辑，需要增加对tags为空的判断，
            # 后续如果tag需求发生变化，需要增加一些功能，建议拆分出解绑
            # 扩容/绑定EIP时，如果值为空，认定为无需操作
            # 绑定tags，如果值为空，认定为置空操作。
            if exec_mode not in [TagResource.EXEC.ALL, TagResource.EXEC.NEW] and not tags:
                # 无需绑定任何资源
                return True

            tag_ids = [str(tag.get('tag_id', '')) for tag in tags]

            if tags and not tag_ids:
                logger.warn(self, f'Replace Resources Warn,tags param is none, cluster_id:{cluster_id} ,tags:{tags}')
                return True

            if not cluster_id:
                raise Exception('Please specify cluster_id')

            cluster = await get_model_by_id(ClusterModel, cluster_id)
            if not cluster:
                raise Exception(f'Cluster not found, id {cluster_id}')

            # 存在kec+epc 混合模式。这里存储实例ID
            temp_check_dict = {}
            temp_check_dict[cluster.cluster_type.upper()] = []
            temp_check_dict['KEC'] = []
            temp_check_dict['EPC'] = []
            temp_check_dict['EBS'] = []
            temp_check_dict['EIP'] = []
            temp_check_dict['SLB'] = []

            # region KEC/EPC/EBS//KMR/KES/KHBASE
            # 创建集群/扩容
            if exec_mode != TagResource.EXEC.BIND:
                for instance_group in cluster.instance_groups:
                    tag_ec_key = instance_group.resource_type.upper()
                    for instance in instance_group.instances:
                        # create or scale_out
                        if new_instance_ids and instance.instance_id not in new_instance_ids:
                            continue
                        if tag_ec_key == 'KEC':
                            temp_check_dict['KEC'].append(instance.instance_id)
                        elif tag_ec_key == 'EPC':
                            temp_check_dict['EPC'].append(instance.instance_id)

                        if instance.volumes:
                            temp_check_dict['EBS'].extend(instance.volumes)

                        if instance.service_instance_id:
                            temp_check_dict[cluster.cluster_type.upper()].append(instance.service_instance_id)
            # endregion

            # region EIP/SLB
            # 创建集群/绑定EIP/SLB
            if exec_mode != TagResource.EXEC.SCALE:
                eip_info_query = model_query(EIPModel).filter(
                    EIPModel.cluster_id == cluster_id,
                    EIPModel.status == EIPModel.STATUS.BINDED)
                eip_infos = await eip_info_query.query_all()

                for eip_info in eip_infos:
                    if eip_info.allocate_address_id:
                        temp_check_dict['EIP'].append(eip_info.allocate_address_id)
                    if eip_info.load_balancer_id:
                        temp_check_dict['SLB'].append(eip_info.load_balancer_id)
            # endregion

            tag_client = getattr(sdk, f'tag_client_{cluster.cluster_type.lower()}')

            if tags:
                # 在“确认”进行绑定时，验证tag是否存在。
                tag_ids = await tag_client.validate_tag_ids(tags, account_id=account_id)
                # else 说明 是置空操作

            if exec_mode == TagResource.EXEC.NEW:
                # 新建集群时，系统tag必定需要判断生成一次。
                new_tags, new_tag_ids = await tag_client.get_cluster_default_key(cluster.id, cluster.cluster_type, account_id=account_id)
                if new_tags:
                    tags.extend(new_tags)
                if new_tag_ids:
                    tag_ids.extend(new_tag_ids)
                logger.info(self, f'New Cluster tags : {tags}')

            # tag定义：强制要求根据type分别调用
            # 我方需求规定，该操作禁止阻断主流程。
            for service_key, replace_instance in temp_check_dict.items():
                if replace_instance:
                    try:
                        await tag_client.replace_resources_tags(TAG_REP.get(service_key, ''), replace_instance, tag_ids, account_id=account_id)
                    except Exception as e:
                        logger.warn(self, f'Replace Resources Error, Type:{service_key} ,Error:{traceback.format_exc()}')
            # 关联集群
            tagkeys = ','.join([tag.get('tag_key', '') for tag in tags])
            await cluster.save({'tags': tags, 'tag_keys': tagkeys})
        except Exception as e:
            logger.warn(self, f'Replace Resources Final Error ,{traceback.format_exc()}')
        return True

    @check_rollback
    async def rollback(self):
        return True


class TaskDeleteClusterDefaultTags(BaseTask):
    @check_task
    async def run(self):
        account_id = self.args.get('account_id', '')
        cluster_id = self.args.get('cluster_id', None)

        if not cluster_id:
            raise Exception('Please specify cluster_id')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if not cluster:
            raise Exception(f'Cluster not found, id {cluster_id}')

        try:
            tags = [{
                "Key": TagResource.SYS_CLUSTER_TAG.CLUSTER_ID,
                "Value": cluster_id,
            }]
            tag_client = getattr(sdk, f'tag_client_{cluster.cluster_type.lower()}')
            await tag_client.delete_tag(tags, account_id=account_id)
        except Exception as e:
            logger.warn(self, f'Delete Cluster Tags Error ,{traceback.format_exc()}')
        return True

    @check_rollback
    async def rollback(self):
        return True
