#part1
使用dw_mobdi_md.tmp_score_part1
#part2
新上线

hive -v -e "
CREATE TABLE dm_sdk_mapping.mapping_edu_app_index0(
  apppkg string, 
  index int)
partitioned by (version string)
stored as orc;

insert overwrite table dm_sdk_mapping.mapping_edu_app_index0 partition (version='20210111')
select apppkg,index 
from mobdi_analyst_test.zx_1130_edu_app_index0;

insert overwrite table dm_sdk_mapping.mapping_edu_app_index0 partition (version='1000')
select apppkg,index 
from mobdi_analyst_test.zx_1130_edu_app_index0;

CREATE TABLE dw_mobdi_md.tmp_edu_score_part2(
  device string, 
  index array<int>, 
  cnt array<double>)
stored as orc;
"

#part3
使用dw_mobdi_md.tmp_score_part3
修改线上逻辑
id_mapping替换成加密表

#part4
使用dw_mobdi_md.tmp_score_part4

#part5
修改线上逻辑
id_mapping替换成加密表

新建dm_sdk_mapping.mapping_contacts_words_20000加密表

hive -v -e "
CREATE TABLE dm_sdk_mapping.mapping_contacts_words_20000_sec(
  pid string COMMENT 'pid', 
  wordslist string, 
  word_index array<int>
)
PARTITIONED BY ( 
  version string)
stored as orc;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapreduce.job.queuename=root.yarn_data_compliance;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/etl_udf-1.1.2.jar;
create temporary function Md5EncryptArrayForJhMcIdArray as 'com.mob.udf.Md5EncryptArrayForJhMcIdArray';
create temporary function Md5EncryptArray as 'com.mob.udf.Md5EncryptArray';
create temporary function McidArrayMapStringEncrypt as 'com.mob.udf.McidArrayMapStringEncrypt';
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar;
create temporary function pid_encrypt_array as 'com.mob.udf.PidEncryptArray';

insert overwrite table dm_sdk_mapping.mapping_contacts_words_20000_sec partition (version = '20200111')
select if(phone is not null and phone not in ('','null','NULL','unknown','none','other','未知','na'),concat_ws(',',pid_encrypt_array(split(phone,','))),'') as pid,
       wordslist,
       word_index
from mobdi_analyst_test.zx_1010_contacts_words_20000;

insert overwrite table dm_sdk_mapping.mapping_contacts_words_20000_sec partition (version = '1000')
select pid,wordslist,word_index
from dm_sdk_mapping.mapping_contacts_words_20000_sec
where version = '20200111';
"

#part6
修改线上逻辑
id_mapping替换成加密表

新建dm_sdk_mapping.mapping_contacts_word2vec2加密表
hive -v -e "
CREATE TABLE dm_sdk_mapping.mapping_contacts_word2vec2_sec(
  pid string COMMENT 'pid', 
  w2v_100 array<double> COMMENT 'array double的向量'
)
PARTITIONED BY ( 
  version string
)
stored as orc;


set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapreduce.job.queuename=root.yarn_data_compliance;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/etl_udf-1.1.2.jar;
create temporary function Md5EncryptArrayForJhMcIdArray as 'com.mob.udf.Md5EncryptArrayForJhMcIdArray';
create temporary function Md5EncryptArray as 'com.mob.udf.Md5EncryptArray';
create temporary function McidArrayMapStringEncrypt as 'com.mob.udf.McidArrayMapStringEncrypt';
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar;
create temporary function pid_encrypt_array as 'com.mob.udf.PidEncryptArray';

insert overwrite table dm_sdk_mapping.mapping_contacts_word2vec2_sec partition (version = '20210111')
select if(phone is not null and phone not in ('','null','NULL','unknown','none','other','未知','na'),concat_ws(',',pid_encrypt_array(split(phone,','))),'') as pid,
       w2v_100
from mobdi_analyst_test.zx_1010_contacts_word2vec2;

insert overwrite table dm_sdk_mapping.mapping_contacts_word2vec2_sec partition (version = '1000')
select pid,w2v_100
from dm_sdk_mapping.mapping_contacts_word2vec2_sec;
"

#part7
新上线

#part8
新上线

hive -v -e "
CREATE TABLE dm_sdk_mapping.mapping_edu_app_tgi_level(
  apppkg string, 
  tag int, 
  sample_cnt bigint, 
  app_cnt bigint, 
  tag_cnt bigint, 
  tgi double, 
  tgi_level string)
partitioned by (version string)
stored as orc;

insert overwrite table dm_sdk_mapping.mapping_edu_app_tgi_level partition (version = '20210111')
select *
from mobdi_analyst_test.zx_1130_edu_app_tgi_level;

insert overwrite table dm_sdk_mapping.mapping_edu_app_tgi_level partition (version = '1000')
select *
from mobdi_analyst_test.zx_1130_edu_app_tgi_level;

CREATE TABLE dm_sdk_mapping.mapping_edu_app_tgi_feature_index0(
  index string, 
  rk int)
partitioned by (version string)
stored as orc;

insert overwrite table dm_sdk_mapping.mapping_edu_app_tgi_feature_index0 partition(version = '20210111')
select *
from mobdi_analyst_test.zx_1130_edu_app_tgi_feature_index0;

insert overwrite table dm_sdk_mapping.mapping_edu_app_tgi_feature_index0 partition(version = '1000')
select *
from mobdi_analyst_test.zx_1130_edu_app_tgi_feature_index0;
"

#confidence
更新计算置信度的mapping表
hive -v -e "
insert overwrite table tp_mobdi_model.model_confidence_config_maping partition(version = '1009')
select kind,prediction,max_probability,min_probability,quadratic_coefficient,primary_coefficient,intercept 
from tp_mobdi_model.model_confidence_config_maping
where version = '1008'
and kind <> 'edu'

union all
select 'edu' as kind,6 as prediction,0.998180241 as max_probability,0.253533996 as min_probability,-0.9825 as quadratic_coefficient,1.8321 as primary_coefficient,0.0422 as intercept 
union all 
select 'edu' as kind,7 as prediction,0.991341566 as max_probability,0.075016127 as min_probability,0 as quadratic_coefficient,1.0154 as primary_coefficient,0.0784 as intercept 
union all 
select 'edu' as kind,8 as prediction,0.998158441 as max_probability,0.13151042 as min_probability,0 as quadratic_coefficient,0.8968 as primary_coefficient,0.1307 as intercept 
union all 
select 'edu' as kind,9 as prediction,0.999854717 as max_probability,0.558727795 as min_probability,0 as quadratic_coefficient,0.1873 as primary_coefficient,0.8281 as intercept ;
"

hadoop fs -cp /user/xinzhou/model20201130/edu_lr/* /dmgroup/dba/modelpath/20200111/linear_regression_model/edu_lr


#mobdel_score_logic_all_l2.sh增加edu的逻辑自洽
增加以下逻辑
hive -v -e "
insert overwrite table dw_mobdi_md.models_with_confidence_pre_par partition(day = '20200113')
select a.device,
       case when b.prediction=6 and a.prediction=9 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.8 then 2
       when b.prediction=6 and a.prediction=8 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.3 then 1
       when b.prediction=6 and a.prediction=7 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then 0
       when a.prediction=9 and b.prediction=5 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.5 then 2
       when a.prediction=9 and b.prediction=4 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then 2
       when a.prediction=8 and b.prediction=3 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then 3
       when a.prediction=8 and b.prediction=2 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then 3
       when a.prediction=8 and b.prediction=1 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then 3
       else a.prediction end prediction,
       a.probability,
       a.kind,
       case when b.prediction=6 and a.prediction=9 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.8 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when b.prediction=6 and a.prediction=8 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.3 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when b.prediction=6 and a.prediction=7 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when a.prediction=9 and b.prediction=5 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.5 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when a.prediction=9 and b.prediction=4 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when a.prediction=8 and b.prediction=3 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when a.prediction=8 and b.prediction=2 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when a.prediction=8 and b.prediction=1 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       else a.confidence end confidence
from
(
    select device,prediction,probability,kind,confidence
    from dw_mobdi_md.models_with_confidence_pre_par
    where day = '20200111'
    and kind = 'edu'
)a
left join
(
    select device,prediction,probability,kind
    from rp_mobdi_app.label_l2_result_scoring_di
    where day = '20200111'
    and kind = 'city_level'
)b
on a.device = b.device

union all

select device,prediction,probability,kind,confidence
from dw_mobdi_md.models_with_confidence_pre_par
where day = '20200111'
and kind <> 'edu';
"