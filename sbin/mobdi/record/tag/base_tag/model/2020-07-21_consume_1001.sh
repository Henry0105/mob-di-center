set -x -e
##消费标签上线准备2020-07-21.txt
hive -v -e "
CREATE TABLE dm_sdk_mapping.mapping_consume_pkg(
  pkg string,
  type string)
COMMENT '消费标签的包名类型映射表'
partitioned by (version string)
stored as orc;
"
##model_path
#mv /dmgroup/dba/modelpath/20200721/consume_1001/cluster_6
#mv /dmgroup/dba/modelpath/20200721/consume_1001/cluster_9

hive -v -e "
insert overwrite table dm_sdk_mapping.mapping_consume_pkg partition (version='20200721')
select pkg,type from test.guanyt_consume_pkg;

insert overwrite table dm_sdk_mapping.mapping_consume_pkg partition (version='1000')
select pkg,type from test.guanyt_consume_pkg;


alter table rp_mobdi_app.label_model_type_all_di add columns (consume_1001 string comment '聚类的消费水平') cascade;

alter table rp_mobdi_app.device_profile_label_full_par add columns (consume_1001 string comment '聚类的消费水平') cascade;

alter table rp_mobdi_app.rp_device_profile_full_history_true add columns (consume_1001 string comment '聚类的消费水平') cascade;
"

##### 还在准备test.xxx_cluster表
hive -v -e "
set hive.support.quoted.identifiers=None;
insert overwrite table rp_mobdi_app.device_profile_label_full_par partition (version='xxx.1000')
select a.*,b.income_1001 from
(
  select \`(consume_1001|version)?+.+\` from rp_mobdi_app.device_profile_label_full_par where version='xxx.1000'
) a
left join
(
  select device,cluster as income_1001 from test.xxx_cluster
) b on a.device=b.device
"

##还有标签系统，上线完应该就ok了
