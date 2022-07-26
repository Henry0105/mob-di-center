#!/bin/bash

set -e -x
: '
 实现监控kv类型值的左右两边值的长度是否相等的功能，比如：a,b,c=1,2,3 是正确的;a,b,c,d=1,2,3 则是错误的
'

   #多重分区中获取最新分区:
   lastDay=`hive -e "show partitions rp_mobdi_app.timewindow_offline_profile" |awk -F/ '{print $2}' | grep day | sort |uniq | tail -n 1`
   
   #--rp_mobdi_app.timewindow_offline_profile_ios
   lastIOSDay=`hive -e "show partitions rp_mobdi_app.timewindow_offline_profile_ios" |awk -F/ '{print $2}' | grep day | sort |uniq | tail -n 1`
   
   lastTagDay=`hive -e "show partitions rp_mobdi_app.ios_active_tag_list" | grep day | sort | uniq | tail -n 1`
   
   #dm_mobdi_master.catering_lbs_label_monthly
   
   lastMonthly=`hive -e "show partitions dm_mobdi_master.catering_lbs_label_monthly" | grep dt |sort | uniq | tail -n 1`
   
   #dm_mobdi_master.catering_lbs_label_weekly
   
   lastWeekly=`hive -e "show partitions dm_mobdi_master.catering_lbs_label_weekly" |grep dt| sort | uniq | tail -n 1`


  mobdi_monitor_kv.sh rp_mobdi_app.rp_device_profile_full_view device catelist,tag_list

  mobdi_monitor_kv.sh "(select * from rp_mobdi_app.timewindow_offline_profile where flag=3 and $lastDay ) tmp" device cnt

  mobdi_monitor_kv.sh "(select * from rp_mobdi_app.timewindow_offline_profile where flag=6 and $lastDay ) tmp" device cnt

  mobdi_monitor_kv.sh "(select * from rp_mobdi_app.timewindow_offline_profile where flag=9 and $lastDay ) tmp" device cnt

  mobdi_monitor_kv.sh "(select * from rp_mobdi_app.timewindow_offline_profile_ios where flag=3 and $lastIOSDay ) tmp"  idfa cnt

  mobdi_monitor_kv.sh "(select * from rp_mobdi_app.timewindow_offline_profile_ios where flag=6 and $lastIOSDay ) tmp"  idfa cnt

  mobdi_monitor_kv.sh "(select * from rp_mobdi_app.timewindow_offline_profile_ios where flag=9 and $lastIOSDay ) tmp"  idfa cnt

  mobdi_monitor_kv.sh "(select * from rp_mobdi_app.ios_active_tag_list where $lastTagDay) tmp" idfa tag_list

  mobdi_monitor_kv.sh "(select * from dm_mobdi_master.catering_lbs_label_monthly where $lastMonthly)tmp"  device catering_dinein_brand_detail,catering_dinein_taste_detail,catering_dinein_time_detail,catering_dinein_tyle1_detail,catering_dinein_tyle2_detail

  mobdi_monitor_kv.sh  "(select * from dm_mobdi_master.catering_lbs_label_weekly where $lastWeekly)tmp" device catering_dinein_brand_detail,catering_dinein_taste_detail,catering_dinein_time_detail,catering_dinein_tyle1_detail,catering_dinein_tyle2_detail

