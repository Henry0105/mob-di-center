set -x -e

#2.age模型升级 特征都好了，上线步骤

hive -v -e "
CREATE TABLE dm_sdk_mapping.mapping_age_app_index0(
  apppkg string,
  index int)
partitioned by (version string)
stored as orc;



insert overwrite table dm_sdk_mapping.mapping_age_app_index0 partition (version='20201222')
select apppkg,index from mobdi_analyst_test.zx_1102_age_app_index0;

insert overwrite table dm_sdk_mapping.mapping_age_app_index0 partition (version='1000')
select apppkg,index from mobdi_analyst_test.zx_1102_age_app_index0;

insert overwrite table dm_sdk_mapping.mapping_contacts_words_20000 partition (version='20201222')
select * from mobdi_analyst_test.zx_1010_contacts_words_20000;


insert overwrite table dm_sdk_mapping.mapping_contacts_word2vec2 partition (day='20201222')
select * from mobdi_analyst_test.zx_1010_contacts_word2vec2;

CREATE TABLE dm_sdk_mapping.mapping_age_app_tgi_level(
  apppkg string,
  tag int,
  sample_cnt bigint,
  app_cnt bigint,
  tag_cnt bigint,
  tgi double,
  tgi_level string)
stored as orc;

insert overwrite table dm_sdk_mapping.mapping_age_app_tgi_level
select * from mobdi_analyst_test.zx_1105_age_app_tgi_level;


mobdi_analyst_test.zx_1105_age_app_tgi_feature_index0


CREATE TABLE dm_sdk_mapping.mapping_age_app_tgi_feature_index0(
  index string,
  rk int)
stored as orc ;

insert overwrite table dm_sdk_mapping.mapping_age_app_tgi_feature_index0
select * from mobdi_analyst_test.zx_1105_age_app_tgi_feature_index0;

mobdi_analyst_test.zx_1010_dpiage_shandong_used
CREATE TABLE dm_sdk_mapping.whitelist_dpiage_shandong_used(
  device string,
  tag int)
stored as orc ;

insert overwrite table dm_sdk_mapping.whitelist_dpiage_shandong_used
select * from mobdi_analyst_test.zx_1010_dpiage_shandong_used;
"
hadoop fs -cp /user/xinzhou/model20201105/age_lr_tgi/* /dmgroup/dba/modelpath/20201222/linear_regression_model/agemodel
hadoop fs -cp /user/xinzhou/model20201112/age1001_lr_1/* /dmgroup/dba/modelpath/20201222/linear_regression_model/age1001model_0
hadoop fs -cp /user/xinzhou/model20201112/age1001_lr_2/* /dmgroup/dba/modelpath/20201222/linear_regression_model/age1001model_1
hadoop fs -cp /user/xinzhou/model20201112/age1001_lr_3/* /dmgroup/dba/modelpath/20201222/linear_regression_model/age1001model_2


hive -v -e "
alter table rp_mobdi_app.label_l2_model_with_confidence_union_logic_di add columns (agebin_1002 string) cascade;
alter table rp_mobdi_app.label_l2_model_with_confidence_union_logic_di add columns (agebin_1002_cl string) cascade;

alter table rp_mobdi_app.label_l2_model_with_confidence_union_logic_di add columns (agebin_1003 string) cascade;
alter table rp_mobdi_app.label_l2_model_with_confidence_union_logic_di add columns (agebin_1003_cl string) cascade;
##
alter table rp_mobdi_app.label_model_type_all_di add columns (agebin_1002 string) cascade;
alter table rp_mobdi_app.label_model_type_all_di add columns (agebin_1002_cl string) cascade;

alter table rp_mobdi_app.label_model_type_all_di add columns (agebin_1003 string) cascade;
alter table rp_mobdi_app.label_model_type_all_di add columns (agebin_1003_cl string) cascade;

##
alter table rp_mobdi_app.device_profile_label_full_par add columns (agebin_1002 string) cascade;
alter table rp_mobdi_app.device_profile_label_full_par add columns (agebin_1002_cl string) cascade;

alter table rp_mobdi_app.device_profile_label_full_par add columns (agebin_1003 string) cascade;
alter table rp_mobdi_app.device_profile_label_full_par add columns (agebin_1003_cl string) cascade;

###
alter table rp_mobdi_app.rp_device_profile_full_with_confidence add columns (agebin_1002 string) cascade;
alter table rp_mobdi_app.rp_device_profile_full_with_confidence add columns (agebin_1002_cl string) cascade;

alter table rp_mobdi_app.rp_device_profile_full_with_confidence add columns (agebin_1003 string) cascade;
alter table rp_mobdi_app.rp_device_profile_full_with_confidence add columns (agebin_1003_cl string) cascade;

"