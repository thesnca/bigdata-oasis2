# --------- cluster_orders -----------

insert into oasis_new.cluster_orders (id,
                                  created_at,
                                  updated_at,
                                  order_id,
                                  cluster_id,
                                  instance_group_id,
                                  instance_id,
                                  data)
select @new_id := UUID() id,
       old.created_at,
       old.updated_at,
       old.order_id,
       old.cluster_id,
       old.node_group_id,
       old.instance_id,
       json_object("kec_instance_id", old.instance_id,
                   "service_order_id", old.kes_order_id,
                   "kec_order_id", old.kec_order_id,
                   "ebs_order_id", json_extract(old.ebs_order_id, '$'),
                   "service_instance_id", old.kes_instance_id,
                   "ebs_instance_id", json_extract(old.extra, '$.ebs_instances'))
from (select eco.created_at,
    eco.updated_at,
    eco.order_id,
    eco.kes_order_id,
    eco.kec_order_id,
    eco.epc_order_id,
    eco.ebs_order_id,
    eco.eip_order_id,
    eco.slb_order_id,
    eco.kes_instance_id,
    eco.instance_id,
    eco.node_group_id,
    eco.extra,
    ng.cluster_id
    from sahara_migration.es_cluster_orders eco
    left join sahara_migration.node_groups ng
    on eco.node_group_id = ng.id
    where ng.cluster_id in (
    select id
    from sahara_migration.clusters
    where cluster_type = 'KES'
    and status!='Deleted'
--     and id in ('2539b796-0f4b-4f45-b1e2-190b29ce3442', '997d7763-a8fa-45f1-8b45-5e8189b95f8a')
    )
    ) old;

# --------- clusters -----------

insert into oasis_new.clusters (id,
                            created_at,
                            updated_at,
                            name,
                            description,
                            cluster_type,
                            distribution_version,
                            main_version,
                            region,
                            availability_zone,
                            image_id,
                            anti_affinity,
                            management_private_key,
                            management_public_key,
                            management_keypair_id,
                            security_group_id,
                            status,
                            status_description,
                            extra,
                            rollback_info,
                            is_terminate_protection,
                            install_apps,
                            enable_eip,
                            eip_line_id,
                            eip_bandwidth,
                            slb_id,
                            activated_at,
                            terminated_at,
                            ks3_credential,
                            vpc_domain_id,
                            vpc_subnet_id,
                            vpc_endpoint_id,
                            charge_type,
                            expire_time,
                            purchase_time,
                            tenant_id,
                            ksc_user_id,
                            ksc_sub_user_id,
                            order_id)
select old.id,
       old.created_at,
       old.updated_at,
       old.name,
       old.description,
       old.cluster_type,
       old.distribution_version,
       old.hadoop_version,
       old.region,
       old.availability_zone,
       old.default_image_id,
       null,
       old.management_private_key,
       old.management_public_key,
       old.management_key_id,
       old.security_group_id,
       old.status,
       old.status_description,
       json_object(
               "main_instance_id", trim(both '"' from json_extract(old.extra, '$.main_kes_instance_id'))
           ),
       old.rollback_info,
       1,
       old.install_apps,
       old.enable_eip,
       old.eip_line_id,
       null,
       null,
       old.activated_at,
       old.terminated_at,
       old.ks3_credential,
       old.vpc_domain_id,
       old.vpc_subnet_id,
       old.vpc_endpoint_id,
       old.charge_type,
       old.expire_time,
       old.purchase_time,
       old.tenant_id,
       old.ksc_user_id,
       null,
       trim(both '"' from json_extract(old.extra, '$.order_id'))
from (select id,
             created_at,
             updated_at,
             name,
             description,
             cluster_type,
             distribution_version,
             hadoop_version,
             region,
             availability_zone,
             default_image_id,
             anti_affinity,
             management_private_key,
             management_public_key,
             management_key_id,
             security_group_id,
             status,
             status_description,
             extra,
             rollback_info,
             is_terminate_protection,
             install_apps,
             enable_eip,
             eip_line_id,
             activated_at,
             terminated_at,
             ks3_credential,
             vpc_domain_id,
             vpc_subnet_id,
             vpc_endpoint_id,
             charge_type,
             expire_time,
             purchase_time,
             tenant_id,
             ksc_user_id
      from sahara_migration.clusters
      where cluster_type = 'KES'
        and status!='Deleted'
--     and id in ('2539b796-0f4b-4f45-b1e2-190b29ce3442', '997d7763-a8fa-45f1-8b45-5e8189b95f8a')
     ) old;

# --------- eip_infos -----------

insert into oasis_new.eip_infos (id,
                             created_at,
                             updated_at,
                             cluster_id,
                             load_balancer_id,
                             listener_id,
                             health_check_id,
                             allocate_address_id,
                             eip_address,
                             eip_line_id,
                             eip_charge_type,
                             eip_order_id,
                             status)
select @new_id := UUID() id,
       old.created_at,
       old.updated_at,
       old.cluster_id,
       old.load_balancer_id,
       old.listener_id,
       old.health_check_id,
       old.allocate_address_id,
       old.eip_address,
       old.eip_line_id,
       old.eip_charge_type,
       old.eip_order_id,
       old.status
from (select created_at,
    updated_at,
    cluster_id,
    load_balancer_id,
    listener_id,
    health_check_id,
    allocate_address_id,
    eip_address,
    eip_line_id,
    eip_charge_type,
    eip_order_id,
    status
    from sahara_migration.eip_infos
    where cluster_id in (
    select id
    from sahara_migration.clusters
    where cluster_type = 'KES'
    and status!='Deleted'
--     and id in ('2539b796-0f4b-4f45-b1e2-190b29ce3442', '997d7763-a8fa-45f1-8b45-5e8189b95f8a')
    )
    ) old;

# --------- instance_groups -----------

insert into oasis_new.instance_groups (id,
                                   created_at,
                                   updated_at,
                                   name,
                                   resource_type,
                                   instance_type_code,
                                   instance_group_type,
                                   image_id,
                                   resource_attr,
                                   count,
                                   dest_count,
                                   cluster_id,
                                   availability_zone,
                                   vpc_domain_id,
                                   vpc_subnet_id,
                                   system_volume_type,
                                   system_volume_size,
                                   volume_type,
                                   volume_count,
                                   volume_size,
                                   order_id,
                                   status)
select old.id,
       old.created_at,
       old.updated_at,
       old.name,
       if(old.product_type = 'KES_EBS', 'KEC', if(old.product_type = 'KES_EPC', 'EPC', null)),
       old.extra_flavor_name,
       old.type,
       cl.default_image_id,
       if(old.product_type = 'KES_EPC',
          json_array(json_object("Name", "raid_type", "Value", old.raid),
                     json_object("Name", "bond_type", "Value", old.bond)),
          '[]'),
       old.count,
       old.dest_count,
       old.cluster_id,
       old.availability_zone,
       cl.vpc_domain_id,
       if(old.product_type = 'KES_EPC', cl.vpc_epc_subnet_id, cl.vpc_subnet_id),
       null,
       20,
       if(ebs_storage = '[]', 'LOCAL_SSD',
          if(json_extract(ebs_storage, '$[0].VolumeType') = 'EHDD', 'CLOUD_EHDD', 'CLOUD_SSD')),
       1,
       ifnull(json_extract(ebs_storage, '$[0].VolumeSize'), fl.ephemeral),
       eco.order_id,
       'Active'
from (select id,
             created_at,
             updated_at,
             name,
             product_type,
             extra_flavor_name,
             type,
             image_id,
             count,
             dest_count,
             cluster_id,
             availability_zone,
             ebs_storage,
             flavor_id,
             status,
             bond,
             raid
      from sahara_migration.node_groups) old
         left join sahara_migration.flavors fl
                   on fl.flavor_id = old.flavor_id
         left join sahara_migration.es_cluster_orders eco
                   on eco.node_group_id = old.id
         left join sahara_migration.clusters cl
                   on cl.id = old.cluster_id
where old.cluster_id in (
    select id
    from sahara_migration.clusters
    where cluster_type = 'KES'
      and status
    !='Deleted'
--   and id in ('2539b796-0f4b-4f45-b1e2-190b29ce3442', '997d7763-a8fa-45f1-8b45-5e8189b95f8a')
    )
group by old.id;

# --------- instances -----------

insert into oasis_new.instances (id,
                             created_at,
                             updated_at,
                             instance_id,
                             instance_name,
                             instance_group_id,
                             internal_ip,
                             management_ip,
                             management_ip_line,
                             management_ip_type,
                             inner_eip,
                             volumes,
                             cpus,
                             ram,
                             host_name,
                             data_guard_id,
                             slb_register_id,
                             service_instance_id,
                             status,
                             inner_manager_ip,
                             allocate_address_id)
select old.id,
       old.created_at,
       old.updated_at,
       old.instance_id,
       old.instance_name,
       old.node_group_id,
       old.internal_ip,
       old.management_ip,
       'BGP',
       null,
       old.inner_eip,
       old.volumes,
       null,
       null,
       null,
       old.data_guard_id,
       old.slb_register_id,
       old.extra_instance_id,
       if(c.status in ('Deleted', 'Error'), 'Deleted', 'Active'),
       old.inner_manager_ip,
       old.allocate_address_id
from (select id,
             created_at,
             updated_at,
             instance_id,
             instance_name,
             node_group_id,
             internal_ip,
             management_ip,
             inner_eip,
             volumes,
             data_guard_id,
             slb_register_id,
             extra_instance_id,
             inner_manager_ip,
             allocate_address_id
      from sahara_migration.instances) old
         left join sahara_migration.node_groups ng on ng.id = old.node_group_id
         left join sahara_migration.clusters c on ng.cluster_id = c.id
where ng.id in (select id from oasis_new.instance_groups);


#
--------- users -----------
--
-- insert ignore into oasis_new.users (id,
--                          created_at,
--                          updated_at,
--                          tenant_id,
--                          total_virtual_cpu,
--                          total_mem_mb,
--                          total_disk_gb,
--                          allocated_virtual_cpu,
--                          allocated_mem_mb,
--                          allocated_disk_gb,
--                          lifespan,
--                          role,
--                          allocator,
--                          description,
--                          extra,
--                          expire_time,
--                          company_alias,
--                          user_level)
-- select old.user_id,
--        old.created_at,
--        old.updated_at,
--        old.tenant_id,
--        old.total_virtual_cpu,
--        old.total_mem_mb,
--        old.total_disk_gb,
--        old.allocated_virtual_cpu,
--        old.allocated_mem_mb,
--        old.allocated_disk_gb,
--        old.lifespan,
--        old.role,
--        old.allocator,
--        old.description,
--        old.extra,
--        old.expire_time,
--        company_alias,
--        user_level
-- from (select created_at,
--              updated_at,
--              user_id,
--              tenant_id,
--              total_virtual_cpu,
--              total_mem_mb,
--              total_disk_gb,
--              allocated_virtual_cpu,
--              allocated_mem_mb,
--              allocated_disk_gb,
--              lifespan,
--              role,
--              allocator,
--              description,
--              extra,
--              expire_time,
--              company_alias,
--              user_level
--       from sahara_migration.users) old;


insert
ignore into oasis_new.gg_node_config
select *
from sahara_migration.gg_node_config old
where old.node_id in (select id from oasis_new.instances)
;

insert
ignore into oasis_new.gg_node_config_his
select *
from sahara_migration.gg_node_config_his old
where old.node_config_id in (select id from oasis_new.gg_node_config);

insert
ignore into oasis_new.gg_node_group_config
select *
from sahara_migration.gg_node_group_config old
where old.node_group_id in (select id from oasis_new.instance_groups)
;

insert
ignore into oasis_new.gg_node_group_config_his
select *
from sahara_migration.gg_node_group_config_his old
where old.node_group_config_id in (select id from oasis_new.gg_node_group_config)
;


insert
ignore into oasis_new.gg_cluster_infos
select *
from sahara_migration.gg_cluster_infos old
where old.id in (select id from oasis_new.clusters)
;

-- insert
-- ignore into oasis_new.gg_operation
-- select *
-- from sahara_migration.gg_operation;
--
-- insert
-- ignore into oasis_new.gg_task_audit
-- select *
-- from sahara_migration.gg_task_audit;


insert into oasis_new.gg_components (name,
                                     service_name,
                                     instance_id,
                                     role,
                                     cluster_id,
                                     node_group_id,
                                     node_id,
                                     register_info,
                                     created_at,
                                     updated_at,
                                     status,
                                     scripts,
                                     script_status,
                                     data)
select old.name,
       old.service_name,
       old.instance_id,
       old.role,
       old.cluster_id,
       old.node_group_id,
       old.node_id,
       old.register_info,
       old.created_at,
       old.updated_at,
       old.status,
       old.scripts,
       old.script_status,
       old.data
from (select name,
             service_name,
             instance_id,
             role,
             cluster_id,
             node_group_id,
             node_id,
             register_info,
             created_at,
             updated_at,
             status,
             scripts,
             script_status,
             data
      from sahara_migration.gg_components) old
where old.node_id in (select id from oasis_new.instances);


update oasis_new.gg_components
set data=json_object("component_key", concat(node_id, name, instance_id, role),
                     "scripts", json_array(
                             json_object("name", "control.sh",
                                         "version", "f465a44a6367bf2877fc0f4543d4f981",
                                         "path", "/Application/gringotts-agent/data/script/EXPORTER//control.sh")),
                     "scripts_version_status", "different",
                     "recovery", json_object(
                             "enabled", true,
                             "is_running", false,
                             "max_count", 10,
                             "window_in_minutes", 20,
                             "retry_interval_seconds", 10,
                             "recovery_script", "control.sh",
                             "recovery_args", concat("start ELASTICSEARCH ", instance_id),
                             "recovery_user", "root",
                             "last_exec_time", "2021-04-02T15:39:05.731337545+08:00",
                             "current_hour_retry_count", 0
                         ),
                     "is_maintain_mode", false
    )
where name = "EXPORTER";