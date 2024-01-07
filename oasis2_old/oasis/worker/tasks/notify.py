from oasis.db.models import model_query
from oasis.db.models.notification import NotificationModel
from oasis.utils.config import config
from oasis.utils.logger import logger
from oasis.utils.sdk import feishu_client
from oasis.utils.sdk.cube import send_scale_notification
from oasis.worker.tasks import BaseTask
from oasis.worker.tasks import check_rollback
from oasis.worker.tasks import check_task


class TaskSendFeishu(BaseTask):
    @check_task
    async def run(self):
        try:
            cluster_id = self.args.pop('cluster_id', None)
            state = self.args.pop('state', None)
            job_id = self.job_id
            # to avoid multiple values exception
            self.args.pop('job_id', None)

            await feishu_client.send_cluster_action(state, cluster_id, job_id,
                                                    **self.args)
        except:
            logger.warn(self, f'Send Feishu Error, Args:{self.args}')
        return True

    @check_rollback
    async def rollback(self):
        return True


class TaskSendScaleNotification(BaseTask):
    # https://wiki.op.ksyun.com/pages/viewpage.action?pageId=151474732
    @check_task
    async def run(self):
        try:
            enable_notification = config.getboolean('cubrick', 'enable_notification', fallback=False)
            if not enable_notification:
                logger.info(self, f'Skip task.')
                return True

            cluster_id = self.args.pop('cluster_id', None)
            action = self.args.get('action', None)
            new_instance_ids = self.args.get('new_instance_ids', None)
            cluster_type = self.context.get('cluster_type', None)
            account_id = self.context.get('account_id', None)
            region = self.context.get('region', None)

            query = model_query(NotificationModel)
            query.filter(NotificationModel.cluster_id == cluster_id)
            notifications = await query.query_all()

            if not notifications:
                logger.info(self, f'No notifications, skip task')
                return True

            for noti in notifications:
                await send_scale_notification(noti, action, new_instance_ids, cluster_type, account_id, region)

        except:
            logger.warn(self, f'Send Scale Notification Error, Args:{self.args}')
        return True

    @check_rollback
    async def rollback(self):
        return True
