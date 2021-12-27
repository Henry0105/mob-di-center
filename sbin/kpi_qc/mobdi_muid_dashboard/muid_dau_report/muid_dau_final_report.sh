#!/bin/bash

set -e -x
if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi
day=$1
pday=`date -d "$day -1 days" +%Y%m%d`
HADOOP_USER_NAME=dba hive -e"
insert overwrite table mobdi_muid_dashboard.muid_dau_final_report partition(day='$day')
select  
muid_dau_android,
gps_base_wifi_in_muid_android,
gps_in_muid_android,
base_in_muid_andriod,
wifi_in_muid_android,
ieid_in_muid,
pid_in_muid_android,
oiid_in_muid,
applist_in_muid,
any_one,
id_lbs,
id_app_lbs,
id_app,
cast(muid_dau_android as bigint)-cast(any_one as bigint)  as no_one,
muid_dau_ios,
pid_in_muid_ios,
ifid_in_muid,
wifi_in_muid_ios,
gps_in_muid_ios,
gps_base_wifi_in_muid_ios,
(muid_dau_android-muid_dau_android_yesterday)/muid_dau_android_yesterday as muid_dau_android_volatility,
(gps_base_wifi_in_muid_android-gps_base_wifi_in_muid_android_yesterday)/gps_base_wifi_in_muid_android_yesterday as gps_base_wifi_in_muid_android_volatility,
(gps_in_muid_android-gps_in_muid_android_yesterday)/gps_in_muid_android_yesterday as gps_in_muid_android_volatility,
(base_in_muid_andriod-base_in_muid_andriod_yesterday)/base_in_muid_andriod_yesterday as base_in_muid_andriod_volatility,
(wifi_in_muid_android-wifi_in_muid_android_yesterday)/wifi_in_muid_android_yesterday as wifi_in_muid_android_volatility,
(ieid_in_muid-ieid_in_muid_yesterday)/ieid_in_muid_yesterday as ieid_in_muid_volatility,
(pid_in_muid_android-pid_in_muid_android_yesterday)/pid_in_muid_android_yesterday as pid_in_muid_android_volatility,
(oiid_in_muid-oiid_in_muid_yesterday)/oiid_in_muid_yesterday as oiid_in_muid_volatility,
(applist_in_muid-applist_in_muid_yesterday)/applist_in_muid_yesterday as applist_in_muid_volatility,
(any_one-any_one_yesterday)/any_one_yesterday as any_one_volatility,
(id_lbs-id_lbs_yesterday)/id_lbs_yesterday as id_lbs_volatility,
(id_app_lbs-id_app_lbs_yesterday)/id_app_lbs_yesterday as id_app_lbs_volatility,
(id_app-id_app_yesterday)/id_app_yesterday as id_app_volatility,
(cast(muid_dau_android as bigint)-cast(any_one as bigint)-no_one_yesterday)/no_one_yesterday as cast_volatility,
(muid_dau_ios-muid_dau_ios_yesterday)/muid_dau_ios_yesterday as muid_dau_ios_volatility,
(pid_in_muid_ios-pid_in_muid_ios_yesterday)/pid_in_muid_ios_yesterday as pid_in_muid_ios_volatility,
(ifid_in_muid-ifid_in_muid_yesterday)/ifid_in_muid_yesterday as ifid_in_muid_volatility,
(wifi_in_muid_ios-wifi_in_muid_ios_yesterday)/wifi_in_muid_ios_yesterday as wifi_in_muid_ios_volatility,
(gps_in_muid_ios-gps_in_muid_ios_yesterday)/gps_in_muid_ios_yesterday as gps_in_muid_ios_volatility,
(gps_base_wifi_in_muid_ios-gps_base_wifi_in_muid_ios_yesterday)/gps_base_wifi_in_muid_ios_yesterday as gps_base_wifi_in_muid_ios_volatility
from 
(select * from mobdi_muid_dashboard.muid_dau_report_by_dws_device_sdk_run_master_di where day='$day' )a
join 
(select * from mobdi_muid_dashboard.muid_dau_report where day='$day') b
left join 
(
select 
muid_dau_android as muid_dau_android_yesterday,
gps_base_wifi_in_muid_android as gps_base_wifi_in_muid_android_yesterday,
gps_in_muid_android as gps_in_muid_android_yesterday,
base_in_muid_andriod as base_in_muid_andriod_yesterday,
wifi_in_muid_android as wifi_in_muid_android_yesterday,
ieid_in_muid as ieid_in_muid_yesterday,
pid_in_muid_android as pid_in_muid_android_yesterday,
oiid_in_muid as oiid_in_muid_yesterday,
applist_in_muid as applist_in_muid_yesterday,
any_one as any_one_yesterday,
id_lbs as id_lbs_yesterday,
id_app_lbs as id_app_lbs_yesterday,
id_app as id_app_yesterday,
no_one as no_one_yesterday,
muid_dau_ios as muid_dau_ios_yesterday,
pid_in_muid_ios as pid_in_muid_ios_yesterday,
ifid_in_muid as ifid_in_muid_yesterday,
wifi_in_muid_ios as wifi_in_muid_ios_yesterday,
gps_in_muid_ios as gps_in_muid_ios_yesterday,
gps_base_wifi_in_muid_ios as gps_base_wifi_in_muid_ios_yesterday
from mobdi_muid_dashboard.muid_dau_final_report where day='$pday'
)c;
"
