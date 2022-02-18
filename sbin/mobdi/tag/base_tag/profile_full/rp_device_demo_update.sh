#!/bin/sh
: '
@owner:xdzhang
@describe:生成rp_device_demo_update近40天的画像数据
@projectName:MOBDI
@BusinessName:demo_update
@SourceTable:rp_mobdi_app.rp_device_profile_full
@TargetTable:rp_mobdi_app.rp_device_demo_update
@TableRelation:rp_mobdi_app.rp_device_profile_full->rp_mobdi_app.rp_device_demo_update
'


:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)

!
set -e -x
if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

day=$1
update_part=$day

source /home/dba/mobdi_center/conf/hive-env.sh

#rp_device_demo_update=dm_mobdi_report.rp_device_demo_update
#rp_device_profile_full_view=dm_mobdi_report.rp_device_profile_full_view

:<<!
@part_1
实现功能: 生成rp_device_demo_update近40天的画像数据
实现逻辑: 1.取出rp_mobdi_app.rp_device_profile_full_view中processtime的最大日期
          2.在rp_mobdi_app.rp_device_profile_full_view表中取出近40天的数据(当步骤1中的日期小于输入日期时, 取步骤1中的日期的近60天的数据)
          3.将数据存到rp_mobdi_app.rp_device_demo_update分区中
输出结果:表名rp_mobdi_app.rp_device_demo_update 全量表
!


fullday=`hive -e "select max(processtime_all) as processtime_all from $appdb.rp_device_profile_full_view"`

if [ $fullday -ge $day ]; then
 day40=`date -d "$day -40 days" +%Y%m%d`
else
 day40=`date -d "$fullday -60 days" +%Y%m%d`
 day=$fullday
fi

hive -e"
CREATE TABLE IF NOT EXISTS $rp_device_demo_update(
  deviceid string, 
  country string, 
  province string, 
  city string, 
  gender int, 
  agebin int, 
  segment int, 
  edu int, 
  kids int, 
  income int, 
  cell_factory string, 
  model string, 
  model_level string, 
  carrier string, 
  network string, 
  screensize string, 
  sysver string, 
  tot_install_apps int, 
  country_cn string, 
  province_cn string, 
  city_cn string, 
  city_level int, 
  occupation int, 
  car int, 
  identity int,
  price string)
PARTITIONED BY ( 
  day string) stored as orc;"
  
hive -v -e "
insert overwrite table $rp_device_demo_update PARTITION(day='$update_part')
select device as deviceid,country,province,city,gender,agebin,segment,edu,kids,income,cell_factory,model,model_level,
       carrier,network,screensize,sysver,tot_install_apps,country_cn,province_cn,city_cn,city_level,occupation,car,
       -1 as identity,price
from $rp_device_profile_full_view
where processtime_all>='$day40'
and processtime_all<='$day'
"
