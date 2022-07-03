
--映射表rid
create table mobdi_test.rid_unid_mid_full(rid_tmp String,rnid String,unid String,mid String) partitioned by(day string) stored as orc;


-----pkg获取rid后的表
drop table dm_mid_master.pkg_it_duid_category_tmp_rid;
CREATE TABLE `dm_mid_master.pkg_it_duid_category_tmp_rid`(
  `rnid` string,
  `pkg` string,
  `firstinstalltime` bigint,
  `version` string,
  `duid` string,
  `ieid` string,
  `oiid` string,
  `factory` string,
  `model` string,
  `unid` string)
PARTITIONED BY (
  `month` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mid_master.db/pkg_it_duid_category_tmp_rid';


drop table dm_mid_master.rnid_ieid_blacklist;
  CREATE TABLE dm_mid_master.rnid_ieid_blacklist( rnid string ) PARTITIONED by (type string) stored as orc;


alter table dm_mid_master.pkg_it_duid_category_tmp_rid change column firstinstalltime firstinstalltime string;