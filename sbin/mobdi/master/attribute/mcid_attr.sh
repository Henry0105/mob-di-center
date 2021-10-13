#!/bin/sh

set -e -x

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

# input
#mcid_full=${archive_id_laws}.mcid_full

# output
#dim_mcid_attribute_full_par_sec=dim_mobdi_mapping.dim_mcid_attribute_full_par_sec

# view
#dim_mcid_attribute_full_par_secview=dim_mobdi_mapping.dim_mcid_attribute_full_par_secview

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
set mapred.job.name='mcid属性表生成';

with mcid6_tmp as (
    select trim(assignment) as assignment, trim(organization_name) as organization_name, trim(brand) as brand, trim(isbrand) as isbrand, trim(comment) as comment
    from dm_sdk_mapping.mac_factory_par where day='20190108' and length(trim(assignment))=6
    group by trim(assignment), trim(organization_name), trim(brand), trim(isbrand), trim(comment)
),

mcid7_tmp as (
    select trim(assignment) as assignment, trim(organization_name) as organization_name, trim(brand) as brand, trim(isbrand) as isbrand, trim(comment) as comment
    from dm_sdk_mapping.mac_factory_par where day='20190108' and length(trim(assignment))=7
    group by trim(assignment), trim(organization_name), trim(brand), trim(isbrand), trim(comment)
),

mcid9_tmp as (
    select trim(assignment) as assignment, trim(organization_name) as organization_name, trim(brand) as brand, trim(isbrand) as isbrand, trim(comment) as comment
    from dm_sdk_mapping.mac_factory_par where day='20190108' and length(trim(assignment))=9
    group by trim(assignment), trim(organization_name), trim(brand), trim(isbrand), trim(comment)
)

insert overwrite table $dim_mcid_attribute_full_par_sec partition (day='${day}')
select
    tmp.mcid as mcid,
    tmp.organization_name as organization_name,
    tmp.brand as brand,
    tmp.isbrand as isbrand,
    tmp.comment as comment
from (
  select
    concat_ws(',',Md5EncryptArray(split(lower(trim(android_mcid.mcid)), ','))) as mcid,
    case
        when mcid9_tmp.assignment is not null then mcid9_tmp.organization_name
        when mcid7_tmp.assignment is not null then mcid7_tmp.organization_name
        when mcid6_tmp.assignment is not null then mcid6_tmp.organization_name
        else ''
  end as organization_name,
    case
        when mcid9_tmp.assignment is not null then mcid9_tmp.brand
        when mcid7_tmp.assignment is not null then mcid7_tmp.brand
        when mcid6_tmp.assignment is not null then mcid6_tmp.brand
        else ''
  end as brand,
    case
        when mcid9_tmp.assignment is not null then mcid9_tmp.isbrand
        when mcid7_tmp.assignment is not null then mcid7_tmp.isbrand
        when mcid6_tmp.assignment is not null then mcid6_tmp.isbrand
        else ''
  end as isbrand,
    case
        when mcid9_tmp.assignment is not null then mcid9_tmp.comment
        when mcid7_tmp.assignment is not null then mcid7_tmp.comment
        when mcid6_tmp.assignment is not null then mcid6_tmp.comment
        else ''
  end as comment
    from
    (
        select mcid_cleaned as mcid
        from $mcid_full
        where mcid_cleaned <> ''
        and day = '$day'
        and mcid_cleaned is not null
        and regexp_replace(trim(lower(mcid_cleaned)), ' |-|\\\\.|:|\073', '') rlike '^[0-9a-f]{12}$'
        group by mcid_cleaned
    ) android_mcid
    left join mcid9_tmp on substring(regexp_replace(trim(lower(android_mcid.mcid)), '-|\\\\.|:', ''), 1, 9) = lower(mcid9_tmp.assignment)
    left join mcid7_tmp on substring(regexp_replace(trim(lower(android_mcid.mcid)), '-|\\\\.|:', ''), 1, 7) = lower(mcid7_tmp.assignment)
    left join mcid6_tmp on substring(regexp_replace(trim(lower(android_mcid.mcid)), '-|\\\\.|:', ''), 1, 6) = lower(mcid6_tmp.assignment)
) tmp where tmp.organization_name != '' or tmp.brand != '';

"


# 创建/替换视图  mcid 属性表
hive -e"
create or replace view $dim_mcid_attribute_full_par_secview
as
select mcid,organization_name,brand,isbrand,comment 
from $dim_mcid_attribute_full_par_sec 
group by mcid,organization_name,brand,isbrand,comment
"