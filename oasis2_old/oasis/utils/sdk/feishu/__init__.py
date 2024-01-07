from datetime import datetime
import traceback

from oasis.db.models import get_model_by_id
from oasis.db.models import model_query
from oasis.db.models.cluster import ClusterModel
from oasis.db.models.job import JobModel
from oasis.db.models.task import TaskModel
from oasis.db.models.user import UserModel
from oasis.utils import http
from oasis.utils.config import config
from oasis.utils.generator import gen_uuid4
from oasis.utils.logger import logger


def _prepare(func):
    async def __inner(self, *args, **kwargs):
        headers = {
            'X-Ksc-Request-Id': gen_uuid4(),
            'Content-Type': 'application/json',
            'cache-control': 'no-cache',
            'Connection': 'keep-alive',
        }

        try:
            return await func(self, headers=headers, *args, **kwargs)
        except Exception as e:
            logger.info(f'Send feishu msg failed, msg: {args} , {kwargs}, Error: {e}\n{traceback.format_exc()}')
            pass

    return __inner


ACTION_DICT = {
    'launch_cluster': '创建集群',
    'scale_out': '扩容集群',
    'scale_in': '缩容集群',
    'freeze_cluster': '冻结集群',
    'unfreeze_cluster': '解冻集群',
    'delete_cluster': '删除集群',
    'restart_cluster': '重启集群',
    'bind_eip': '绑定EIP',
    'unbind_eip': '解绑EIP',
    'bind_private_slb': '绑定私网SLB',
    'unbind_private_slb': '解绑私网SLB',
    'bind_internal_eip': '绑定内部EIP',
    'enable_xpack': '启用XPack',
    'disable_xpack': '禁用XPack',
    'install_user_plugin': '安装用户自定义插件',
    'uninstall_user_plugin': '卸载用户自定义插件',
    'delete_user_plugin': '删除用户自定义插件',
    'replace_resources_tags': '绑定tag',
    'upgrade_instances': '升配集群',
    'rolling_restart_instances': '滚动重启Instances'
}


class FeishuClient:
    class STATE:
        INIT = '开始'
        DONE = '完成'
        ERROR = '错误'
        RETRY = '重试'
        ROLLED = '回滚完成'

    class MSGGROUP:
        # 不告警
        MESSAGE = 4
        # 告警
        ALERT = 2

    class MSGENV:
        # 线上
        PROD = "khadoop"
        # 测试
        TEST = "kkk"

    class MSGDEAL:
        # 不需要认领
        FALSE = 1
        # 需要认领
        TRUE = 0

    def __init__(self):
        self.feishu_url = config.get('feishu', 'feishu_url')
        self.enable = config.getboolean('feishu', 'enable', fallback=True)
        self.test_env = config.getboolean('oasis', 'test_env', fallback=True)

    @_prepare
    async def send_cluster_action(self, state, cluster_id, job_id,
                                  *args, **kwargs):
        if not self.enable:
            return
        test_env = self.test_env
        headers = kwargs.pop('headers', {})
        job = await get_model_by_id(JobModel, job_id)
        action = ACTION_DICT.get(job.name, 'Unknown')

        cluster = await get_model_by_id(ClusterModel, cluster_id)
        if cluster:
            account_id = cluster.ksc_user_id
            user = await get_model_by_id(UserModel, account_id)
            if user.user_level == 'P999':
                test_env = True
        else:
            user = None

        content, content_detail = await self._form_content(state, action, cluster, job_id, user, cluster_id=cluster_id,
                                                           *args, **kwargs)

        data = {
            'name': f'{action}-{state}',
            'group': self.MSGGROUP.ALERT if state == self.STATE.ERROR else self.MSGGROUP.MESSAGE,
            'product': self.MSGENV.TEST if test_env else self.MSGENV.PROD,
            # and not self.test_env
            'priority': 2,
            'content': content,
            'html_content': content_detail,
            'no_deal': self.MSGDEAL.TRUE if state == self.STATE.ERROR else self.MSGDEAL.FALSE,
        }

        res = await http.post(self.feishu_url, data=data, headers=headers)
        logger.info(f'feishu send, res {res}')

    async def _form_content(self, state, action, cluster, job_id, user, *args, **kwargs):
        cluster_id = kwargs.get('cluster_id', None)

        region = config.get('infra', 'region')
        az = 'Unknown'
        cluster_name = 'Unknown'
        cluster_type = 'Unknown'
        company_alias = 'Unknown'
        user_level = ''
        user_id = 'Unknown'
        instances_count = 0

        if cluster:
            region = cluster.region
            az = cluster.availability_zone
            cluster_name = cluster.name
            cluster_type = cluster.cluster_type

        if user:
            company_alias = user.company_alias
            user_level = user.user_level
            user_id = user.id
            instances_count = sum([ig.count if action != '创建集群' else ig.dest_count for ig in cluster.instance_groups])

        content_dict = dict()

        if not self.test_env:
            content_dict.update({
                '公司别名': company_alias,
                '客户等级': user_level,
            })

        content_dict.update({
            '用户ID': user_id,
            '集群名称': cluster_name,
            '集群ID': cluster_id,
            '集群类型': cluster_type,
            '地区': region,
            '可用区': az,
            '接口': action,
            '状态': state,
            '时间': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            '集群当前实例总数': instances_count,
        })

        if action == '扩容集群':
            scale_out_sum = kwargs.get('scale_out_sum', [])
            content_dict.setdefault('扩容数量', scale_out_sum)
        elif action == '缩容集群':
            scale_in_sum = kwargs.get('scale_in_sum', [])
            content_dict.setdefault('缩容数量', scale_in_sum)
        elif action == '升配集群':
            is_upgrade_kec = kwargs.get('is_upgrade_kec', False)
            is_upgrade_ebs = kwargs.get('is_upgrade_ebs', False)
            is_upgrade_service = kwargs.get('is_upgrade_service', False)
            is_upgrade_local = kwargs.get('is_upgrade_local', False)
            origin_instance_group_type_code = kwargs.get('origin_instance_group_type_code', '')
            origin_instance_group_volume_size = kwargs.get('origin_instance_group_volume_size', 0)

            content_dict.setdefault('\n是否升配KEC', is_upgrade_kec)
            content_dict.setdefault('\n是否升配EBS', is_upgrade_ebs)
            content_dict.setdefault('\n是否升配本地盘', is_upgrade_local)
            content_dict.setdefault(f'\n是否升配{cluster.cluster_type}', is_upgrade_service)

            upgrade_instance_group = kwargs.get('upgrade_instance_group', {})

            upgrade_volume_size = upgrade_instance_group.get('volume_size', '20')
            if is_upgrade_kec:
                content_dict.setdefault('\n原始KES-KEC套餐', origin_instance_group_type_code)
                content_dict.setdefault('\n目标KES-KEC套餐', upgrade_instance_group['instance_type_code'])
                upgrade_volume_type = upgrade_instance_group.get('volume_type', 'LOCAL_SSD')
                if upgrade_volume_type.startswith('LOCAL_'):
                    content_dict.setdefault('\n原始本地盘大小', str(origin_instance_group_volume_size))
                    content_dict.setdefault('\n目标本地盘大小', str(upgrade_volume_size))
            if is_upgrade_ebs:
                content_dict.setdefault('\n原始EBS大小', str(origin_instance_group_volume_size))
                content_dict.setdefault('\n目标EBS大小', str(upgrade_volume_size))

        content_list = [f'{k}:{v}' for k, v in content_dict.items()]
        content = ';'.join(content_list)
        content_list.insert(0, '')
        if job_id:
            content_list.append(f'任务ID: {job_id}')
            if state == self.STATE.ERROR:
                query = model_query(TaskModel)
                query.filter(TaskModel.job_id == job_id, TaskModel.status == TaskModel.STATUS.Failed)
                error_tasks = await query.query_all()
                error_info = '; <br />'.join([f'子任务: {et.name}:  <br />'
                                              f'子任务ID: {et.id},  <br />'
                                              f'Worker: {et.worker},  <br />'
                                              f'Error: {et.info}' for et in error_tasks])
                content_list.append(f'错误信息: <br />{error_info}')
        content_list.append('')

        content_detail = ' <br />'.join(content_list)

        return content, content_detail
