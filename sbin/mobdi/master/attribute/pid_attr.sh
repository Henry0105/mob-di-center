#!/bin/sh

set -e -x

day=$1
#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

# input
pid_full=${archive_id_laws}.pid_full
#dim_pid_carrier_location_china=dim_sdk_mapping.dim_pid_carrier_location_china
#dim_pid_release_year_china=dim_sdk_mapping.dim_pid_release_year_china

# output
#dim_pid_attribute_full_par_sec=dim_mobdi_mapping.dim_pid_attribute_full_par_sec

# view
#dim_pid_attribute_full_par_secview=dim_mobdi_mapping.dim_pid_attribute_full_par_secview

hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar;
create temporary function pid_encrypt_array as 'com.mob.udf.PidEncryptArray';
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
set mapred.job.name='pid属性表生成';

insert overwrite table $dim_pid_attribute_full_par_sec partition (day='$day')
select
    concat_ws(',',pid_encrypt_array(split(trim(tmp.pid),','))) as pid_id,
    tmp.areacode as areacode,
    tmp.city as city,
    tmp.company as company,
    tmp.zip as zip,
    tmp.carrier as carrier,
    tmp.year as year,
    case
      when tmp.province is not null and length(tmp.province) > 0 then tmp.province
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6)='000852' then '香港'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6)='000853' then '澳门'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6)='000886' then '台湾'
      else ''
    end as province,
    case
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6)='000852' then 'cn_31'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6)='000853' then 'cn_32'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6)='000886' then 'cn_33'
      else ''
    end as province_code,
    case
      when length(trim(tmp.pid))=11 then '中国'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6) in ('000852','000853','000886') then '中国'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6) in ('000001') then '美国'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6) in ('000007') then '俄罗斯'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6) in ('000810') then '日本'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6) in ('000820') then '韩国'
      else '其他'
    end as country,
    case
      when length(trim(tmp.pid))=11 then 'cn'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6) in ('000852','000853','000886') then 'cn'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6) in ('000001') then 'us'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6) in ('000007') then 'ru'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6) in ('000810') then 'jp'
      when length(trim(tmp.pid))=17 and substring(tmp.pid, 1, 6) in ('000820') then 'kr'
      else 'other'
    end as country_code
from (
    select
        android_pid.pid as pid,
        phone_id_clean.areacode as areacode,
        phone_id_clean.city as city,
        phone_id_clean.company as company,
        phone_id_clean.province as province,
        phone_id_clean.zip as zip,
        phone_id_clean.carrier as carrier,
        phone_pre_year.year as year
    from
    (
        select pid_cleaned as pid
        from $pid_full
        where pid_cleaned <> ''
        and pid_cleaned is not null
        and day = '$day'
        and (trim(pid_cleaned) rlike '^[0-9]{11}$' or trim(pid_cleaned) rlike '^[0-9]{17}$')
        group by pid_cleaned
    ) android_pid
    left join (select phone_id, areacode, city, company, province, zip, carrier from $dim_pid_carrier_location_china where day='20200525') phone_id_clean
    on substring(trim(android_pid.pid), 1, 7)=trim(phone_id_clean.phone_id)
    left join (select phone_pre3, year from $dim_pid_release_year_china where version='1000') phone_pre_year
    on substring(trim(android_pid.pid), 1, 3)=trim(phone_pre_year.phone_pre3)
) tmp;
"

hive -e"
create or replace view $dim_pid_attribute_full_par_secview
as
select pid_id,areacode,city,company,zip,carrier,year,province,province_code,country,country_code
from $dim_pid_attribute_full_par_sec
group by pid_id,areacode,city,company,zip,carrier,year,province,province_code,country,country_code
"