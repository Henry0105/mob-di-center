#!/bin/sh

set -e -x

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_ods.properties

# input
#ieid_full=${archive_id_laws}.ieid_full

# output
#dim_ieid_transform_full_par_sec=dim_mobdi_mapping.dim_ieid_transform_full_par_sec

# view
#dim_ieid_transform_full_par_secview=dim_mobdi_mapping.dim_ieid_transform_full_par_secview

:<<!
CREATE TABLE dm_mobdi_mapping.dim_ieid_transform_full_par_sec(
  `ieid` string COMMENT '明文表ieid加密',
  `ieid14_lower` string COMMENT '小写14位ieid加密',
  `ieid14_upper` string COMMENT '大写14位ieid加密',
  `ieid15_lower` string COMMENT '小写15位ieid加密',
  `ieid15_upper` string COMMENT '大写15位ieid加密')
COMMENT 'ieid转换表'
PARTITIONED BY (
  day string COMMENT '日期')
STORED AS ORC;
!

hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/etl_udf-1.1.2.jar;
create temporary function Md5EncryptArray as 'com.mob.udf.Md5EncryptArray';
create temporary function imei_luhn_recomputation as 'com.youzu.mob.java.udf.ReturnImei15';
create temporary function shahash as 'com.youzu.mob.java.udf.SHAHashing';

SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts='-Xmx4096m';
SET mapreduce.child.map.java.opts='-Xmx4096m';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapred.job.name='ieid加密转换表生成';
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-dpi-aes-with-dependencies.jar;
create temporary function aesieid as 'com.mob.udf.AesIeidEncrypt';

insert overwrite table $dim_ieid_transform_full_par_sec partition (day='$day')
select
    concat_ws(',',Md5EncryptArray(split(trim(ieid), ','))) as ieid,
    concat_ws(',',Md5EncryptArray(split(lower(substring(trim(ieid), 1, 14)), ','))) as ieid14_lower,
    concat_ws(',',Md5EncryptArray(split(upper(substring(trim(ieid), 1, 14)), ','))) as ieid14_upper,
    case
        when trim(ieid) rlike '^[0-9]{14}$' and length(imei_luhn_recomputation(trim(ieid)))=15 then concat_ws(',',Md5EncryptArray(split(lower(imei_luhn_recomputation(trim(ieid))), ',')))
        when trim(ieid) rlike '^[0-9]{14}$' and length(imei_luhn_recomputation(trim(ieid)))<>15 then concat_ws(',',Md5EncryptArray(split(lower(substring(trim(ieid), 1, 14)), ',')))
        else concat_ws(',',Md5EncryptArray(split(lower(substring(trim(ieid), 1, 15)), ',')))
    end as ieid15_lower,
    case
        when trim(ieid) rlike '^[0-9]{14}$' and length(imei_luhn_recomputation(trim(ieid)))=15 then concat_ws(',',Md5EncryptArray(split(upper(imei_luhn_recomputation(trim(ieid))), ',')))
        when trim(ieid) rlike '^[0-9]{14}$' and length(imei_luhn_recomputation(trim(ieid)))<>15 then concat_ws(',',Md5EncryptArray(split(upper(substring(trim(ieid), 1, 14)), ',')))
        else concat_ws(',',Md5EncryptArray(split(upper(substring(trim(ieid), 1, 15)), ',')))
    end as ieid15_upper,
    length(ieid) as ieid_length,
    if(ieid rlike '^[0-9]+$',0,1) as letter_flag,
    shahash(substring(trim(ieid), 1, 14)) as ieid14_sha256,
    shahash(substring(trim(ieid), 1, 15)) as ieid15_sha256,
    aesieid(lower(substring(trim(ieid), 1, 14))) as ieid_dpi
from
(
    select ieid_cleaned as ieid
    from $ieid_full
    where ieid_cleaned <> ''
    and ieid_cleaned is not null
    and day = '$day'
    and regexp_replace(trim(lower(ieid_cleaned)), ' |/|-','') rlike '^[0-9a-f]{14,17}$'
    group by ieid_cleaned
)a;
"


hive -e"
create or replace view $dim_ieid_transform_full_par_secview
as
select ieid,ieid14_lower,ieid14_upper,ieid15_lower,ieid15_upper,ieid_length,letter_flag,ieid14_sha256,ieid15_sha256,ieid_dpi
from $dim_ieid_transform_full_par_sec
group by ieid,ieid14_lower,ieid14_upper,ieid15_lower,ieid15_upper,ieid_length,letter_flag,ieid14_sha256,ieid15_sha256,ieid_dpi
"