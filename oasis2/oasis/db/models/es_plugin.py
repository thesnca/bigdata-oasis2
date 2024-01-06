from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import UniqueConstraint

from oasis.db.models import OasisBase


class EsPluginModel(OasisBase):
    __tablename__ = 'es_plugins'

    __table_args__ = (
        UniqueConstraint('name', 'cluster_id'),
    )

    '''
        CREATE TABLE `es_plugins` (
            `id`                   varchar(36)  NOT NULL,
            `name`                 varchar(128) NOT NULL       COMMENT '插件名称',
            `cluster_id`           varchar(36)  DEFAULT NULL,
            `plugin_type`          int          NOT NULL       COMMENT '插件类型（0系统默认/1用户自定义）',
            `upload_type`          int          NOT NULL       COMMENT '插件上传类型（0本地上传/1KS3上传）',
            `status`               int          NOT NULL       COMMENT '状态（0未安装/1已安装/2卸载中/3安装中/4已删除/5删除中）',
            `description`          text         DEFAULT NULL   COMMENT '描述',
            `ks3_address`          text         NOT NULL       COMMENT '插件存放到ks3的地址（系统临时/用户）',
            `created_at`           datetime     DEFAULT NULL,
            `updated_at`           datetime     DEFAULT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

        # insert_system_default_es_plugins sql
        INSERT INTO `es_plugins` VALUES (1,'analysis-ik','1',0,1,'Elasticsearch IK 分析插件，默认不能卸载。','','2021-12-07 18:54:35','2021-12-07 18:54:35');
        INSERT INTO `es_plugins` VALUES (2,'sql','1',0,1,'SQL查询插件，默认不能卸载。','','2021-12-07 18:54:35','2021-12-07 18:54:35');
        INSERT INTO `es_plugins` VALUES (3,'repository-s3','1',0,1,'KS3插件，默认不能卸载。','','2021-12-07 18:54:35','2021-12-07 18:54:35');
    '''

    class SOURCE:
        SYSTEM_DEFAULT_PLUGIN = 0
        USER_DEFINE_PLUGIN = 1

    class TYPE:
        DEFAULT_UPLOAD_TYPE = 0
        KS3_UPLOAD_TYPE = 1

    class STATUS:
        UNINSTALL_STATUS = 0
        INSTALL_STATUS = 1
        UNINSTALLING_STATUS = 2
        INSTALLING_STATUS = 3
        DELETE_STATUS = 4
        DELETING_STATUS = 5

    name = Column(String(128), nullable=False)
    cluster_id = Column(String(36), ForeignKey('clusters.id', ondelete='CASCADE'))
    plugin_type = Column(Integer, nullable=False)
    upload_type = Column(Integer, nullable=False)
    status = Column(Integer, nullable=False)
    description = Column(Text)
    ks3_address = Column(Text, nullable=False)
