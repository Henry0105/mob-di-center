#!/bin/bash
set -x -e

# 作为pre_data的输入

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

appdb="rp_mobdi_report"

day=$1
## input table
rp_device_location_3monthly="${appdb}.rp_device_location_3monthly"

## mapping table
house_price_mapping_par="tp_mobdi_model.house_price_mapping_par"

## target table 
label_house_price_mf=${label_l1_house_price_mf}

sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('rp_mobdi_app', 'rp_device_location_3monthly', 'day');
drop temporary function GET_LAST_PARTITION;
"
lastPartition=(`hive -e "$sql"`)

house_price_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('tp_mobdi_model', 'house_price_mapping_par', 'version');
drop temporary function GET_LAST_PARTITION;
"
house_price_lastPartition=(`hive -e "$house_price_sql"`)

#取rp_device_location_3monthly最近一年的数据
#检查数据是否存在
function check_data(){
  par=$1
  findHdfs=`hadoop fs -ls hdfs://ShareSdkHadoop/user/hive/warehouse/rp_mobdi_app.db/rp_device_location_3monthly/day=${par} | wc -l`
  if [[ ${findHdfs} -eq 0 ]] ;then
    echo "Error! day=${par} partition of $rp_device_location_3monthly is not found"
    exit 1
  fi
}

firstday_month=`date -d "$day " "+%Y%m01"`
par_3month=`date -d "${lastPartition} -3 months" "+%Y%m%d"`
check_data ${par_3month}
par_6month=`date -d "${lastPartition} -6 months" "+%Y%m%d"`
check_data ${par_6month}
par_9month=`date -d "${lastPartition} -9 months" "+%Y%m%d"`
check_data ${par_9month}

hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';
insert overwrite table $label_house_price_mf partition (day='$firstday_month')
select device, price as house_price
from
(
  select device, get_geohash(lat_home, lon_home, 6) as geohash_6
  from
  (
    select device,lat_home,lon_home,
           row_number() over (partition by device order by day desc) as rn
    from $rp_device_location_3monthly
    where day in ('${lastPartition}','${par_3month}','${par_6month}','${par_9month}')
    and lon_home <> ''
    and lat_home <> ''
  ) t
  where rn = 1
) as t1
inner join
(
  select geohash6, price
  from $house_price_mapping_par
  where version = '$house_price_lastPartition'
) as t2 on t1.geohash_6 = t2.geohash6
"
