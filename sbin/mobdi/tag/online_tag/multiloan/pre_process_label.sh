#! /bin/bash

set -e -x
: '
@owner: yeyy
@describe: 获取dm_mobdi_master.master_reserved_new 在指定范围内的数据，为营销线标签做一次数据预处理
'

if [ $# -lt 2 ]; then
 echo "ERROR: wrong number of parameters"
 echo "USAGE: <date>"
 exit 1
fi

startday=$1
timewindow=$2

p21day=`date -d "$startday -21 day" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#源表
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di

#映射表
#online_category_mapping_v3=dim_sdk_mapping.online_category_mapping_v3

#目标表
tmp_pre_process_label=dw_mobdi_tmp.tmp_pre_process_label

pdays=`date -d "$startday -$timewindow days" +%Y%m%d`
partition=${startday}_${timewindow}

hive -v -e "
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table $tmp_pre_process_label partition(day='$partition')
select device,
       pkg,
       cate_id,
       refine_final_flag,
       substring(day,1,6) as month,
       weekofyear(to_date(from_unixtime(unix_timestamp(day,'yyyyMMdd'),'yyyy-MM-dd'))) as week,
       day as current_day
from
(
  select device,
         pkg,
         cate_id,
         refine_final_flag,
         day
  from
  (
    select device,
           pkg,
           refine_final_flag,
           day
    from $dws_device_install_app_re_status_di
    where (day between $pdays and $startday)
    and (refine_final_flag=1 or refine_final_flag=-1)
  ) master_reserved_new
  inner join
  (
    select relation,
           cate_id
    from $online_category_mapping_v3
    where cate_id in('1','2','3','4','5','6')
  ) cate_mapping
  on cate_mapping.relation=master_reserved_new.pkg
)t1;
"

######### 保留三周内的分区
partition_drop=${p21day}_${timewindow}
hive -e"
alter table $tmp_pre_process_label drop partition(day='${partition_drop}');
"