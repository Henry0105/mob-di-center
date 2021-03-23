#! /bin/sh
set -e -x
cd `dirname $0`
: '
@owner:xdzhang
@describe:时间窗内支付类活跃天数
@projectName:MobDI
@BusinessName:pay_active
@SourceTable:dm_sdk_mapping.tag_cat_mapping_dmp_par,rp_mobdi_app.rp_device_active_label_profile
@TargetTable:dm_mobdi_mapping.online_category_mapping,rp_mobdi_app.timewindow_online_profile
@TableRelation:dm_sdk_mapping.tag_cat_mapping_dmp_par->dm_mobdi_mapping.online_category_mapping|dm_mobdi_mapping.online_category_mapping,rp_mobdi_app.rp_device_active_label_profile->rp_mobdi_app.timewindow_online_profile
'
day=$1
mappingtype=12
computeType=1
windowTime=$2
bday=`date -d "$day -${windowTime} days" "+%Y%m%d"`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
#input
#tag_cat_mapping_dmp_par=dim_sdk_mapping.tag_cat_mapping_dmp_par
#rp_device_active_label_profile=dm_mobdi_report.rp_device_active_label_profile
#output
#dim_online_category_mapping=dim_sdk_mapping.dim_online_category_mapping
  if [ $# -lt 2 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <date><windowTime>"
    exit 1
  fi

hive -e"
insert overwrite table $dim_online_category_mapping partition (type='${mappingtype}')
select tag_id as relation,tag as category,0 as total,0 as percent
from $tag_cat_mapping_dmp_par where version='1000'
and tag='支付'
"
spark2-submit --master yarn --deploy-mode client \
--class com.youzu.mob.tools.OnlineUniversalTools \
--driver-memory 10G \
--executor-memory 12G \
--executor-cores 3 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=3 \
--conf spark.dynamicAllocation.maxExecutors=80 \
--conf spark.dynamicAllocation.executorIdleTimeout=15s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=7168 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.default.parallelism=2000 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.driver.maxResultSize=4g \
--conf spark.storage.memoryFraction=0.3 \
--conf spark.shuffle.memoryFraction=0.5 \
--conf spark.sql.autoBroadcastJoinThreshold=104857600 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--driver-java-options "-XX:MaxPermSize=1024m" \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT.jar \
"
{
    \"partition\": \"${day}\",
    \"fileNum\": \"100\",
    \"repartition\":\"1500\",
    \"labelInfo\": {
        \"labelTable\": \"SELECT device as device,day as day,tag as relation 
		FROM $rp_device_active_label_profile  WHERE day >${bday} and day <=${day}\",
        \"mappingtype\": \"${mappingtype}\",
        \"computeType\": \"${computeType}\",
        \"windowTime\": \"${windowTime}\",
		\"processfield\": \"\"
   }
}
"
