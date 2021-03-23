#!/bin/sh

set -e -x

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

# input
ieid_full=archive_id_laws.ieid_full
#imei_factory_par=dim_sdk_mapping.imei_factory_par

# output
#dim_ieid_attribute_full_par_sec=dim_mobdi_mapping.dim_ieid_attribute_full_par_sec

# view
#dim_ieid_attribute_full_par_secview=dim_mobdi_mapping.dim_ieid_attribute_full_par_secview


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
set mapred.job.name='ieid属性表生成';

with imei5_tmp as (
	select
		trim(tac) as tac,trim(manufacturer) as manufacturer,trim(model) as model,trim(aka) as aka,trim(os) as os,trim(year) as year,trim(lte) as lte
	from $imei_factory_par where day='20190220' and length(trim(tac))=5
	group by trim(tac),trim(manufacturer),trim(model),trim(aka),trim(os),trim(year),trim(lte)
),

imei6_tmp as (
	select
		trim(tac) as tac,trim(manufacturer) as manufacturer,trim(model) as model,trim(aka) as aka,trim(os) as os,trim(year) as year,trim(lte) as lte
	from $imei_factory_par where day='20190220' and length(trim(tac))=6
	group by trim(tac),trim(manufacturer),trim(model),trim(aka),trim(os),trim(year),trim(lte)
),

imei7_tmp as (
	select
		trim(tac) as tac,trim(manufacturer) as manufacturer,trim(model) as model,trim(aka) as aka,trim(os) as os,trim(year) as year,trim(lte) as lte
	from $imei_factory_par where day='20190220' and length(trim(tac))=7
	group by trim(tac),trim(manufacturer),trim(model),trim(aka),trim(os),trim(year),trim(lte)
),

imei8_tmp as (
	select
		trim(tac) as tac,trim(manufacturer) as manufacturer,trim(model) as model,trim(aka) as aka,trim(os) as os,trim(year) as year,trim(lte) as lte
	from $imei_factory_par where day='20190220' and length(trim(tac))=8
	group by trim(tac),trim(manufacturer),trim(model),trim(aka),trim(os),trim(year),trim(lte)
)


insert overwrite table $dim_ieid_attribute_full_par_sec partition (day='$day')
select
  ieid,manufacturer,model,aka,os,year,lte
from (
  select
    ieid,
    manufacturer,
    model,
    aka,
    os,
    year,
    lte
  from (
    select
        concat_ws(',',Md5EncryptArray(split(lower(trim(android_ieid.ieid)), ','))) as ieid,
        case
          when imei8_tmp.tac is not null then imei8_tmp.manufacturer
          when imei7_tmp.tac is not null then imei7_tmp.manufacturer
          when imei6_tmp.tac is not null then imei6_tmp.manufacturer
          when imei5_tmp.tac is not null then imei5_tmp.manufacturer
          else ''
        end as manufacturer,
        case
          when imei8_tmp.tac is not null then imei8_tmp.model
          when imei7_tmp.tac is not null then imei7_tmp.model
          when imei6_tmp.tac is not null then imei6_tmp.model
          when imei5_tmp.tac is not null then imei5_tmp.model
          else ''
        end as model,
        case
          when imei8_tmp.tac is not null then imei8_tmp.aka
          when imei7_tmp.tac is not null then imei7_tmp.aka
          when imei6_tmp.tac is not null then imei6_tmp.aka
          when imei5_tmp.tac is not null then imei5_tmp.aka
          else ''
        end as aka,
        case
          when imei8_tmp.tac is not null then imei8_tmp.os
          when imei7_tmp.tac is not null then imei7_tmp.os
          when imei6_tmp.tac is not null then imei6_tmp.os
          when imei5_tmp.tac is not null then imei5_tmp.os
          else ''
        end as os,
        case
          when imei8_tmp.tac is not null then imei8_tmp.year
          when imei7_tmp.tac is not null then imei7_tmp.year
          when imei6_tmp.tac is not null then imei6_tmp.year
          when imei5_tmp.tac is not null then imei5_tmp.year
          else ''
        end as year,
        case
          when imei8_tmp.tac is not null then imei8_tmp.lte
          when imei7_tmp.tac is not null then imei7_tmp.lte
          when imei6_tmp.tac is not null then imei6_tmp.lte
          when imei5_tmp.tac is not null then imei5_tmp.lte
          else ''
        end as lte
    from
    (
        select ieid_cleaned as ieid
        from $ieid_full
        where ieid_cleaned <> ''
        and ieid_cleaned is not null
        and day = '$day'
        and regexp_replace(trim(lower(ieid_cleaned)), ' |/|-','') rlike '^[0-9a-f]{14,17}$'
        group by ieid_cleaned
    ) android_ieid
    left join imei8_tmp on substring(lower(android_ieid.ieid), 1, 8)=lower(imei8_tmp.tac)
    left join imei7_tmp on substring(lower(android_ieid.ieid), 1, 7)=lower(imei7_tmp.tac)
    left join imei6_tmp on substring(lower(android_ieid.ieid), 1, 6)=lower(imei6_tmp.tac)
    left join imei5_tmp on substring(lower(android_ieid.ieid), 1, 5)=lower(imei5_tmp.tac)
  ) tmp where manufacturer != '' or model != ''
) t2 group by ieid,manufacturer,model,aka,os,year,lte;
"



# 创建/替换视图  ieid 属性表
hive -v -e"
create or replace view $dim_ieid_attribute_full_par_secview
as
select ieid,manufacturer,model,aka,os,year,lte
from $dim_ieid_attribute_full_par_sec
group by ieid,manufacturer,model,aka,os,year,lte;
"

