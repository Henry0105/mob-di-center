#! /bin/sh
set -e -x
cd `dirname $0`
: '
@owner:xdzhang
@describe:支付类新安装数量
@projectName:MobDI
@BusinessName:pay_installed
@SourceTable:dm_sdk_mapping.dim_app_tag_system_mapping_par,dm_sdk_mapping.dim_app_pkg_mapping_par,dm_mobdi_master.device_install_app_master_new
@TargetTable:dm_mobdi_mapping.online_category_mapping,rp_mobdi_app.timewindow_online_profile
@TableRelation:dm_sdk_mapping.dim_app_tag_system_mapping_par->dm_mobdi_mapping.online_category_mapping|dm_mobdi_mapping.online_category_mapping,dm_sdk_mapping.dim_app_pkg_mapping_par,dm_mobdi_master.device_install_app_master_new->rp_mobdi_app.timewindow_online_profile
'

day=$1
mappingtype=10
computeType=0
windowTime=1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
#input
#dim_app_tag_system_mapping_par=dim_sdk_mapping.dim_app_tag_system_mapping_par
#dws_device_install_app_status_40d_di=dm_mobdi_topic.dws_device_install_app_status_40d_di
#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par
#output
#dim_online_category_mapping=dim_sdk_mapping.dim_online_category_mapping

  if [ $# -lt 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <date>"
    exit 1
  fi

hive -e"
insert overwrite table $dim_online_category_mapping partition (type='${mappingtype}')
select apppkg as relation,tag as category,0 as total,1 as percent
from $dim_app_tag_system_mapping_par where version='1000'
and tag='支付' group by apppkg,tag
"
spark2-submit --master yarn --deploy-mode client \
--class com.youzu.mob.tools.OnlineUniversalTools \
--driver-memory 10G \
--executor-memory 12G \
--executor-cores 3 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=30 \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.dynamicAllocation.executorIdleTimeout=15s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=7168 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.default.parallelism=2000 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.driver.maxResultSize=4g \
--conf spark.storage.memoryFraction=0.4 \
--conf spark.shuffle.memoryFraction=0.4 \
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
        \"labelTable\": \"SELECT NVL(app_pkg.apppkg,device_filter.pkg) AS relation,
device_filter.device AS device,
device_filter.day AS day
FROM 
(SELECT device.device AS device,
  device.pkg AS pkg,
  device.final_flag AS final_flag,
  device.day AS day
 FROM $dws_device_install_app_status_40d_di device
  WHERE device.final_flag <> -1
  AND device.day =${day} 
) device_filter
LEFT  JOIN (select apppkg,pkg from $dim_app_pkg_mapping_par where version='1000')app_pkg
ON app_pkg.pkg = device_filter.pkg
GROUP BY  NVL(app_pkg.apppkg,device_filter.pkg), device_filter.device, device_filter.day\",
        \"mappingtype\": \"${mappingtype}\",
        \"computeType\": \"${computeType}\",
        \"windowTime\": \"${windowTime}\",
		\"processfield\": \"\"
   }
}
"
