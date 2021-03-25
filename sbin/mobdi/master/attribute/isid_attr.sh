#!/bin/sh

set -e -x

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

# input
isid_full=${archive_id_laws}.isid_full

# output
#dim_isid_attribute_full_par_sec=dim_mobdi_mapping.dim_isid_attribute_full_par_sec

# view
#dim_isid_attribute_full_par_secview=dim_mobdi_mapping.dim_isid_attribute_full_par_secview


hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/etl_udf-1.1.2.jar;
create temporary function Md5EncryptArray as 'com.mob.udf.Md5EncryptArray';
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
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
set mapred.job.name='isid属性表生成';

with isid5_tmp as (
    select trim(mcc_mnc) as mcc_mnc, trim(operator) as operator, trim(country_code) as country_code, trim(country) as country from dm_sdk_mapping.mapping_carrier_country
    where length(mcc_mnc)=5 group by trim(mcc_mnc), trim(operator), trim(country_code), trim(country)
),

isid6_tmp as (
    select trim(mcc_mnc) as mcc_mnc, trim(operator) as operator, trim(country_code) as country_code, trim(country) as country from dm_sdk_mapping.mapping_carrier_country
    where length(mcc_mnc)=6 group by trim(mcc_mnc), trim(operator), trim(country_code), trim(country)
),

isid8_tmp as (
    select trim(mcc_mnc) as mcc_mnc, trim(operator) as operator, trim(country_code) as country_code, trim(country) as country from dm_sdk_mapping.mapping_carrier_country
    where length(mcc_mnc)=8 group by trim(mcc_mnc), trim(operator), trim(country_code), trim(country)
),

isid9_tmp as (
    select trim(mcc_mnc) as mcc_mnc, trim(operator) as operator, trim(country_code) as country_code, trim(country) as country from dm_sdk_mapping.mapping_carrier_country
    where length(mcc_mnc)=8 group by trim(mcc_mnc), trim(operator), trim(country_code), trim(country)
)


insert overwrite table $dim_isid_attribute_full_par_sec partition (day='${day}')
select
    isid,
    operator,
    country_code,
    country
from (
    select
    concat_ws(',',Md5EncryptArray(split(lower(trim(isid_table.isid)), ','))) as isid,
    case
        when isid9_tmp.mcc_mnc is not null then isid9_tmp.operator
        when isid8_tmp.mcc_mnc is not null then isid8_tmp.operator
        when isid6_tmp.mcc_mnc is not null then isid6_tmp.operator
        when isid5_tmp.mcc_mnc is not null then isid5_tmp.operator
        else ''
    end as operator,
    case
        when isid9_tmp.mcc_mnc is not null then isid9_tmp.country_code
        when isid8_tmp.mcc_mnc is not null then isid8_tmp.country_code
        when isid6_tmp.mcc_mnc is not null then isid6_tmp.country_code
        when isid5_tmp.mcc_mnc is not null then isid5_tmp.country_code
        else ''
    end as country_code,
    case
        when isid9_tmp.mcc_mnc is not null then isid9_tmp.country
        when isid8_tmp.mcc_mnc is not null then isid8_tmp.country
        when isid6_tmp.mcc_mnc is not null then isid6_tmp.country
        when isid5_tmp.mcc_mnc is not null then isid5_tmp.country
        else ''
    end as country
from
(
    select isid_cleaned as isid
    from $isid_full
    where isid_cleaned <> ''
    and isid_cleaned is not null
    and trim(isid_cleaned) rlike '^[0-9]{14,15}$'
    and day = '$day'
    group by isid_cleaned
) isid_table
left join isid9_tmp on substring(isid_table.isid, 1, 9) = isid9_tmp.mcc_mnc
left join isid8_tmp on substring(isid_table.isid, 1, 8) = isid8_tmp.mcc_mnc
left join isid6_tmp on substring(isid_table.isid, 1, 6) = isid6_tmp.mcc_mnc
left join isid5_tmp on substring(isid_table.isid, 1, 5) = isid5_tmp.mcc_mnc
) tmp where tmp.operator != '';
"

# 创建/替换视图  isid 属性表
hive -e"
create or replace view $dim_isid_attribute_full_par_secview
as
select isid,operator,country_code,country
from $dim_isid_attribute_full_par_sec
group by isid,operator,country_code,country
"
