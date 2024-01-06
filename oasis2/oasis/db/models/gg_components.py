from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import String
from sqlalchemy import Text

from oasis.db.models import OasisBase


class GgComponentsModel(OasisBase):
    __tablename__ = 'gg_components'

    '''
       create table gg_components
   (
       id            int auto_increment
           primary key,
       name          varchar(45) null comment 'component',
       service_name  varchar(45) null comment 'KES',
       instance_id   int         null comment 'component id多实例的id',
       role          varchar(45) null comment 'role 比如es的MASTER/DATA， 如果没有可以不写',
       cluster_id    varchar(36) null,
       node_group_id varchar(36) null,
       node_id       varchar(45) null comment 'instance',
       register_info text        null comment '注册信息，register发送的信息，提供给agent心跳时使用',
       created_at    datetime    null,
       updated_at    datetime    null,
       status        int         null comment 'agent上报的状态信息，组件运行状态',
       scripts       text        null comment '组件脚本',
       script_status varchar(45) null comment '组件脚本状态',
       data          text        null comment '组件autostart信息等'
   )
       charset = utf8;


       '''
    name = Column(String(45))
    service_name = Column(String(45))
    instance_id = Column(Integer)
    role = Column(String(45))
    cluster_id = Column(String(36))
    node_group_id = Column(String(36))
    node_id = Column(String(45))
    register_info = Column(Text)
    status = Column(Integer)
    scripts = Column(Text)
    script_status = Column(String(45))
    data = Column(JSON)
