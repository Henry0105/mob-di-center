set -x -e

#1.age模型升级 特征都好了，就差训练了

hive -v -e "

create table dm_sdk_mapping.mapping_age_cate_index1
(
  cate_l1_id string,
  index int)
partitioned by (version string)
stored as orc;

create table dm_sdk_mapping.mapping_age_cate_index2(
  cate_l2_id string,
  index int)
partitioned by (version string)
stored as orc;


insert overwrite table dm_sdk_mapping.mapping_age_cate_index1 partition (version='20200608')
select cate_l1_id,index from test.liwj_age_0608_app_cate_index1;

insert overwrite table dm_sdk_mapping.mapping_age_cate_index1 partition (version='1000')
select cate_l1_id,index from test.liwj_age_0608_app_cate_index1;

insert overwrite table dm_sdk_mapping.mapping_age_cate_index2 partition (version='20200608')
select cate_l2_id,index from test.liwj_age_0608_app_cate_index2;

insert overwrite table dm_sdk_mapping.mapping_age_cate_index2 partition (version='1000')
select cate_l2_id,index from test.liwj_age_0608_app_cate_index2;

test.liwj_age_0608_app_cate_index2
select cate_l1_id,index from
test.liwj_age_0608_app_cate_index1
"
hive -v -e "

CREATE TABLE `dm_sdk_mapping.mapping_age_app_index`(
  `apppkg` string,
  `index_after_chi` int,
  `index_before_chi` int)
partitioned by (version string)
stored as orc;

insert overwrite table dm_sdk_mapping.mapping_age_app_index partition (version='1000')
select apppkg,index_after_chi,index_before_chi from test.liwj_age_0611_app_index;


insert overwrite table dm_sdk_mapping.mapping_age_app_index partition (version='20200611')
select apppkg,index_after_chi,index_before_chi from test.liwj_age_0611_app_index;

###
mapping_phonenum_year="test.liwj_phonenum_year"
gdpoi_explode_big="test.liwj_gdpoi_explode_big"
mapping_contacts_words_20000="test.liwj_contacts_words_20000"
mapping_word_index="test.liwj_age_0611_word_index"
mapping_contacts_word2vec2="test.liwj_contacts_word2vec2"

create table dm_sdk_mapping.mapping_phonenum_year
(
  `phone_pre3` string COMMENT 'phone 开始',
  `year` string COMMENT 'year')
COMMENT 'phone 对应时间'
partitioned by (version string)
stored as orc;

insert overwrite table dm_sdk_mapping.mapping_phonenum_year partition (version='1000')
select phone_pre3,year from test.liwj_phonenum_year;

drop table dm_sdk_mapping.mapping_gdpoi_explode_big;
CREATE TABLE `dm_sdk_mapping.mapping_gdpoi_explode_big`(
  `poi_id` string,
  `name` string,
  `lat` string,
  `lon` string,
  `attribute` string,
  `type` string)
stored as orc;

insert overwrite table dm_sdk_mapping.mapping_gdpoi_explode_big
select * from test.liwj_gdpoi_explode_big;

CREATE TABLE `dm_sdk_mapping.mapping_contacts_words_20000`(
  `phone` string COMMENT '通讯录手机号',
  `wordslist` string,
  `word_index` array<int>)
partitioned by (version string)
stored as orc;

insert overwrite table dm_sdk_mapping.mapping_contacts_words_20000 partition (version='20200810')
select * from test.liwj_contacts_words_20000;

test.liwj_age_0611_word_index

CREATE TABLE `dm_sdk_mapping.mapping_age_word_index`(
  `word` string,
  `index_after_chi` int,
  `index_before_chi` int
)partitioned by (version string)
stored as orc;

insert overwrite table dm_sdk_mapping.mapping_age_word_index partition (version='1000')
select * from test.liwj_age_0611_word_index;
"

modelPath="/dmgroup/dba/modelpath/20200811/linear_regression_model/agemodel"
modelPath0="/dmgroup/dba/modelpath/20200811/linear_regression_model/age1001model_0"
modelPath1="/dmgroup/dba/modelpath/20200811/linear_regression_model/age1001model_1"
modelPath2="/dmgroup/dba/modelpath/20200811/linear_regression_model/age1001model_2"

modelPath="/dmgroup/dba/modelpath/20190815/linear_regression_model/agemodel"
modelPath0="/dmgroup/dba/modelpath/20190815/linear_regression_model/age1001model_0"
modelPath1="/dmgroup/dba/modelpath/20190815/linear_regression_model/age1001model_1"
modelPath2="/dmgroup/dba/modelpath/20190815/linear_regression_model/age1001model_2"

hadoop fs -cp /user/liwj/model20200615/age_lr_5bin/* /dmgroup/dba/modelpath/20200811/linear_regression_model/agemodel
hadoop fs -cp /user/liwj/model20200616/age1001_lr_1/* /dmgroup/dba/modelpath/20200811/linear_regression_model/age1001model_0
hadoop fs -cp /user/liwj/model20200616/age1001_lr_2/* /dmgroup/dba/modelpath/20200811/linear_regression_model/age1001model_1
hadoop fs -cp /user/liwj/model20200616/age1001_lr_3/* /dmgroup/dba/modelpath/20200811/linear_regression_model/age1001model_2