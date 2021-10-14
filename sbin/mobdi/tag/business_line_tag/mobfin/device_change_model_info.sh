#!/bin/sh

set -e -x

if [ -z "$1" ]; then
  exit 1
fi

day=$1
p1week=`date -d "$day -7 day" +%Y%m%d`
p14day=`date -d "$day -14 day" +%Y%m%d`
p1month=`date -d "$day -30 day" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

## 源表
#dwd_device_info_df=dm_mobdi_master.dwd_device_info_df

## 目标表
#label_l1_anticheat_device_changemodel_wi=dm_mobdi_report.label_l1_anticheat_device_changemodel_wi


hive -v -e "
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=256000000;

insert overwrite table $label_l1_anticheat_device_changemodel_wi partition (day='$day')
select
  t1.device,
  if(cnt_7>1, 1, 0) as if_change_7,
  if(cnt_14>1, 1, 0) as if_change_14,
  if(cnt_30>1, 1, 0) as if_change_30
from
( select
    device, count(1) cnt_30
  from ( select
           device,factory,model
         from $dwd_device_info_df
         where version > '${p1month}.1000' 
		 and version <= '${day}.1000' 
		 and plat='1'
         group by device,factory,model
       ) d30_info
  group by device
) t1
left join
( select
    device, count(1) cnt_14
  from ( select
           device,factory,model
         from $dwd_device_info_df
         where version > '${p14day}.1000' 
		 and version <= '${day}.1000' 
		 and plat='1'
         group by device,factory,model
       ) d14_info
  group by device
) t2
on t1.device = t2.device
left join
( select
    device, count(1) cnt_7
  from ( select
           device,factory,model
         from $dwd_device_info_df
         where version > '${p1week}.1000' 
		 and version <= '${day}.1000' 
		 and plat='1'
         group by device,factory,model
       ) d7_info
  group by device
) t3
on t1.device = t3.device
;
"

