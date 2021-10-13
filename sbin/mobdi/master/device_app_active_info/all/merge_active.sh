#!/bin/bash
: '
@owner: zhtli
@describe: 活跃数据的改进,将pv.log和run.log合并成一张表
@projectName:MobDI
@BusinessName:merge_active
'

set -x -e
export LANG=zh_CN.UTF-8

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

#入参
day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

#源表
#dws_device_active_di=dm_mobdi_topic.dws_device_active_di

#mapping表
#mapping_ip_attribute_code=dim_sdk_mapping.mapping_ip_attribute_code
#dim_mapping_ip_attribute_code=dim_sdk_mapping.dim_mapping_ip_attribute_code

dim_mapping_ip_attribute_code_db=${dim_mapping_ip_attribute_code%.*}
dim_mapping_ip_attribute_code_tb=${dim_mapping_ip_attribute_code#*.}

#目标表
#dws_device_sdk_run_master_di=dm_mobdi_topic.dws_device_sdk_run_master_di

hive -v -e "
SET mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx8192M';
set mapreduce.child.map.java.opts='-Xmx8192M';
set hive.exec.parallel=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.map.aggr=true;
set hive.auto.convert.join=true;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_min_ip as 'com.youzu.mob.java.udf.GetIpAttribute';
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';

insert overwrite table $dws_device_sdk_run_master_di partition (day='$day')
select device,
       pkg,
       appver,
       appkey,
       plat,
       run_cnt,
       0 as pre_run_cnt,
       0 as next_run_cnt,
       ip,
       country,
       province,
       city,
       commonsdkver,
       sdks,
       processtime
from
(
    select device,
           pkg,
           appver,
           appkey,
           plat,
           ip,
           get_min_ip(ip) as minip,
           commonsdkver,
           sdks,
           processtime,
           sum(cnt) as run_cnt
    from
    (
        select device,
               pkg,
               appver,
               appkey,
               plat,
               ip,
               get_min_ip(ip) as minip,
               commonsdkver,
               sdks,
               $day as processtime,
               size(split(sservertime_list,',')) as cnt
        from $dws_device_active_di
        where day='${day}'
        and source in ('pv','logrun')
    ) a
    group by device, pkg, appver, appkey, plat, ip, commonsdkver, sdks, processtime
) b
left join
(
  select minip,
         country,
         province,
         city
  from $dim_mapping_ip_attribute_code
  where day=GET_LAST_PARTITION ('$dim_mapping_ip_attribute_code_db','$dim_mapping_ip_attribute_code_tb')
) ip_info on b.minip=ip_info.minip
cluster by device;
"