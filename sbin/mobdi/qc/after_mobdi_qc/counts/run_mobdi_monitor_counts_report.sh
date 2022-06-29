#!/bin/bash

set -e -x

: '
  监控mapping表，master层的输出表，以及full表的总行数以及主键是否唯一
  每天运行
  设计思路:先运行统计脚本，将表数据统计结果放到MySQL，然后运行report程序从mysql查询发邮件到相关人员
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day> "
    exit 1
fi

day=$1
mail_list="wangych@mob.com,zhaox@uuzu.com,DIMonitor@mob.com,zhangxinyuan@mob.com"
#mail_list="zhtli@mob.com"

: '
  @part 1 :tables counts:统计表的行数，主键是否唯一，如果需要新加入统计其他字段，可以升级工具程序
'
#mapping类
  #sdk_mapping 
  ./mobdi_monitor_table_counts.sh "dm_sdk_mapping.app_pkg_mapping_par where version='1000'" "pkg" $day mapping version='1000' 1
  ./mobdi_monitor_table_counts.sh "dm_sdk_mapping.app_category_mapping_par where version='1000'" "pkg" $day mapping version='1000' 1
  ./mobdi_monitor_table_counts.sh "dm_sdk_mapping.app_tag_system_mapping_par where version='1000'" "pkg,cate,tag" $day mapping version='1000' 1
  ./mobdi_monitor_table_counts.sh "dm_sdk_mapping.cate_id_mapping_par where version='1000'" "cate_l1,cate_l2_id" $day mapping version='1000' 1
  ./mobdi_monitor_table_counts.sh "dm_sdk_mapping.game_app_detail_par where version='1000'" "pkg" $day mapping version='1000' 1
  ./mobdi_monitor_table_counts.sh "dm_sdk_mapping.factory_mapping_upper" "factory_upper" $day mapping version='1000' 1
  ./mobdi_monitor_table_counts.sh "dm_sdk_mapping.tag_cat_mapping_dmp_par where version='1000'" "tag_id" $day mapping version='1000' 1
  ./mobdi_monitor_table_counts.sh "dm_sdk_mapping.tag_id_mapping_par where version='1000'" "tag" $day mapping version='1000' 1

  #mobdi_mapping
  ./mobdi_monitor_table_counts.sh "dm_mobdi_mapping.android_id_mapping_full where version=${day}.1000" "device" $day full version=${day}.1000 1
  ./mobdi_monitor_table_counts.sh "dm_mobdi_mapping.device_applist_new where day='$day'" "device,pkg" $day incr day=$day 1
 ./mobdi_monitor_table_counts.sh "dw_mobdi_md.android_id_mapping_incr where day='$day'" "device" $day incr day=$day 1
  #./mobdi_monitor_table_counts.sh "dm_mobdi_mapping.device_id_mapping_hive_incr where day='$day'" "device_id" $day incr day=$day 1
  ./mobdi_monitor_table_counts.sh "dw_mobdi_md.ios_id_mapping_incr where day='$day'" "device" $day incr day=$day 1
#master类
  
   ./mobdi_monitor_table_counts.sh "rp_mobdi_app.label_l2_model_with_confidence_union_logic_di where day='$day'" "device" $day incr day=$day 1
  ./mobdi_monitor_table_counts.sh "dm_mobdi_master.device_ip_info where day='$day'" "device" $day incr day=$day 0
  ./mobdi_monitor_table_counts.sh "dm_mobdi_master.device_info_master_incr where day='$day'" "device" $day incr day=$day 1
  ./mobdi_monitor_table_counts.sh "dm_mobdi_master.master_reserved_new where day='$day'" "device,pkg" $day incr day=$day 1
  
#full表类
  ./mobdi_monitor_table_counts.sh "rp_mobdi_app.device_profile_label_full_par where version=${day}.1000" "device" $day full version=${day}.1000 1
  ./mobdi_monitor_table_counts.sh "rp_mobdi_app.device_models_confidence_full where version='${day}.1000'"  "device" $day full version=${day}.1000 1

#20190104 add
  lastPar=`hive -S -e "show partitions dm_mobdi_master.device_staying_daily" | sort| tail -n 1`
  lastLocationPar=`hive -S -e "show partitions dm_mobdi_master.device_location_daily" | sort| tail -n 1`
  lastLocPar=`echo $lastLocationPar | awk -F/ '{print $1}'`
  
  ./mobdi_monitor_table_counts.sh "dm_mobdi_mapping.ios_id_mapping_full where version='${day}.1000'"  "device" $day full version=${day}.1000 1
  ./mobdi_monitor_table_counts.sh "dm_mobdi_master.device_staying_daily where $lastPar"  "device,lat,lon,start_time" $day incr $lastPar 1
  ./mobdi_monitor_table_counts.sh "dm_mobdi_master.device_install_app_master_new where day='${day}'"  "device,pkg" $day incr day=${day} 1
  ./mobdi_monitor_table_counts.sh "dm_mobdi_master.device_info_master_full_par where version='${day}.1000'"  "device,plat" $day full version=${day}.1000 1
  ./mobdi_monitor_table_counts.sh "dm_mobdi_master.master_update_new where day='${day}'"  "device,pkg" $day incr day=${day} 1
  ./mobdi_monitor_table_counts.sh "dm_sdk_mapping.device_duid_mapping_new"  "device,duid,plat" $day mapping "not-partitioned" 1
  ./mobdi_monitor_table_counts.sh "dm_mobdi_master.dwd_device_location_di_v2 where $lastLocPar"  "device" $day incr $lastLocPar 0
: '
  @part 2:mail_reporter:将步骤一中统计结果发送到相关人员 
'
./mobdi_monitor_report_tool.sh $day mobdi_monitor.monitor_counts_online "$mail_list"
