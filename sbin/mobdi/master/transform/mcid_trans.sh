#!/bin/sh

set -e -x

day=$1
#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

# input
mcid_full=archive_id_laws.mcid_full

# output
#dim_mcid_transform_full_par_sec=dm_mobdi_mapping.dim_mcid_transform_full_par_sec

# view
#dim_mcid_transform_full_par_secview=dm_mobdi_mapping.dim_mcid_transform_full_par_secview


hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/etl_udf-1.1.2.jar;
create temporary function Md5EncryptArray as 'com.mob.udf.Md5EncryptArray';

set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapred.job.name='mcid加密转换表生成';

insert overwrite table $dim_mcid_transform_full_par_sec partition (day='${day}')
select
    concat_ws(',',Md5EncryptArray(split(lower(trim(mcid)), ','))) as mcid,
    concat_ws(',',Md5EncryptArray(split(substring(regexp_replace(regexp_replace(lower(trim(mcid)), '-|\\\\.|:', ''), '(.{2})', '\$1:'), 1, 17), ','))) as mcid_colon_lower,
    concat_ws(',',Md5EncryptArray(split(regexp_replace(lower(trim(mcid)), '-|\\\\.|:', ''), ','))) as mcid_discolon_lower,
    concat_ws(',',Md5EncryptArray(split(substring(regexp_replace(regexp_replace(upper(trim(mcid)), '-|\\\\.|:', ''), '(.{2})', '\$1:'), 1, 17), ','))) as mcid_colon_upper,
    concat_ws(',',Md5EncryptArray(split(regexp_replace(upper(trim(mcid)), '-|\\\\.|:', ''), ','))) as mcid_discolon_upper
from
(
    select mcid_cleaned as mcid
    from $mcid_full
    where mcid_cleaned <> ''
    and day = '$day'
    and mcid_cleaned is not null
    and regexp_replace(trim(lower(mcid_cleaned)), ' |-|\\\\.|:|\073', '') rlike '^[0-9a-f]{12}$'
    group by mcid_cleaned
)a;
"

# 创建/替换视图  mcid 转换表
hive -e"
create or replace view dm_mobdi_mapping.dim_mcid_transform_full_par_secview
as
select mcid,mcid_colon_lower,mcid_discolon_lower,mcid_colon_upper,mcid_discolon_upper
from $dim_mcid_transform_full_par_sec 
group by mcid,mcid_colon_lower,mcid_discolon_lower,mcid_colon_upper,mcid_discolon_upper
"

