#!/bin/bash

set -e -x

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

# input
ifid_full=${archive_id_laws}.ifid_full

# output
#dim_ifid_transform_full_par_sec=dim_mobdi_mapping.dim_ifid_transform_full_par_sec

# view
#dim_ifid_transform_full_par_secview=dim_mobdi_mapping.dim_ifid_transform_full_par_secview


hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/etl_udf-1.1.2.jar;
create temporary function Md5EncryptArray as 'com.mob.udf.Md5EncryptArray';
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.optimize.skewjoin=true;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=3221225472;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapred.job.name='ifid加密转换表生成';


insert overwrite table $dim_ifid_transform_full_par_sec partition (day = '$day')
select concat_ws(',',Md5EncryptArray(split(upper(ifid), ','))) as ifid,
       concat_ws(',',Md5EncryptArray(split(regexp_replace(trim(lower(ifid)), '-|\\\\.','-'), ','))) as ifid_rod_lower,
       concat_ws(',',Md5EncryptArray(split(regexp_replace(trim(lower(ifid)), ':|-|\\\\.',''), ','))) as ifid_disrod_lower,
       concat_ws(',',Md5EncryptArray(split(regexp_replace(trim(upper(ifid)), '-|\\\\.','-'), ','))) as ifid_rod_upper,
       concat_ws(',',Md5EncryptArray(split(regexp_replace(trim(upper(ifid)), ':|-|\\\\.',''), ','))) as ifid_disrod_upper
from
(
    select ifid_cleaned as ifid
    from $ifid_full
    where ifid_cleaned <> ''
    and day = '$day'
    and ifid_cleaned is not null
    and regexp_replace(lower(trim(ifid_cleaned)), ' |-|\\\\.|:|\073','') rlike '^[0-9a-f]{32}$'
    group by ifid_cleaned
)a
"


hive -e"
create or replace view $dim_ifid_transform_full_par_secview
as
select ifid,ifid_rod_lower,ifid_disrod_lower,ifid_rod_upper,ifid_disrod_upper
from $dim_ifid_transform_full_par_sec
group by ifid,ifid_rod_lower,ifid_disrod_lower,ifid_rod_upper,ifid_disrod_upper
"