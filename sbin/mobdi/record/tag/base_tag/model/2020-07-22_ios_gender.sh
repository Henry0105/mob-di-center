set -x -e
hive -v -e "
CREATE TABLE `test.guanyt_iosgender_index_pkg_mapping`(
  `pkg` string,
  `index` int,
  `index_chi` int)
partitioned by
(version string)
stored as orc;
"

hive -v -e "
insert overwrite table dm_sdk_mapping.model_iosgender_index_pkg_mapping partition (version='20200722')
select pkg,index,index_chi from
test.guanyt_iosgender_index_pkg_mapping;
insert overwrite table dm_sdk_mapping.model_iosgender_index_pkg_mapping partition (version='1000')
select pkg,index,index_chi from
test.guanyt_iosgender_index_pkg_mapping;

"

hadoop fs -cp hdfs://ShareSdkHadoop/user/liwj/iosmodel/gender_lr_refine
hdfs://ShareSdkHadoop/dmgroup/dba/modelpath/20200722/iosmodel/gender_lr_refine
