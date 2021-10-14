#!/bin/sh
set -e -x
cd `dirname $0`
: '
@owner:xdzhang
@describe:时间窗内金融类新安装数量
@projectName:MobDI
@BusinessName:financial_install
@SourceTable:$dim_app_tag_system_mapping_par,$dim_tag_cat_mapping_dmp_par,$dws_device_install_app_re_status_di,$dim_app_pkg_mapping_par
@TargetTable:$dim_online_category_mapping
@TableRelation:$dim_app_tag_system_mapping_par,$dim_tag_cat_mapping_dmp_par->$dim_online_category_mapping|$dim_online_category_mapping,$dim_app_pkg_mapping_par,$dws_device_install_app_re_status_di->rp_mobdi_app.timewindow_online_profile
'
day=$1
mappingtype=14
computeType=0
windowTime=$2
bday=`date -d "$day -${windowTime} days" "+%Y%m%d"`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#input
#dim_app_tag_system_mapping_par=dim_sdk_mapping.dim_app_tag_system_mapping_par
#dim_tag_cat_mapping_dmp_par=dim_sdk_mapping.dim_tag_cat_mapping_dmp_par
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di
#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par

#output
#dim_online_category_mapping=dim_sdk_mapping.dim_online_category_mapping



  if [ $# -lt 2 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <date>,<windowTime>"
    exit 1
  fi

hive -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
insert overwrite table $dim_online_category_mapping partition (type='${mappingtype}')
SELECT a.apppkg AS relation,
     b.cat2 AS category ,1 as total,0 as percent
 FROM (select rank.apppkg as apppkg, 
              rank.tag as tag,rank.norm_tfidf as norm_tfidf 
		from (select n.apppkg as apppkg,n.tag as tag ,n.norm_tfidf as norm_tfidf , 
		      Row_number() over(partition by n.apppkg,n.tag ORDER BY  n.norm_tfidf DESC ) as rank 
       from $dim_app_tag_system_mapping_par n where version ='1000') rank where rank.rank =1
) a 
JOIN  (select cat2,tag from $dim_tag_cat_mapping_dmp_par where version='1000')b
ON a.tag = b.tag
GROUP BY  a.apppkg,b.cat2
HAVING SUM(case when b.cat2='借贷' then a.norm_tfidf else 0 end) > 2.73656580503609 
OR SUM(case when b.cat2='银行' then a.norm_tfidf else 0 end) > 2.22706268389954 
OR SUM(case when b.cat2='投资' then a.norm_tfidf else 0 end) > 1.71748682559831 
OR SUM(case when b.cat2='保险' then a.norm_tfidf else 0 end) > 2.1970158073038 
OR SUM(case when b.cat2='证券' then a.norm_tfidf else 0 end) > 3.1456478215668 
OR SUM(case when b.cat2='理财' then a.norm_tfidf else 0 end) > 2.52228575739187
UNION ALL
SELECT a.apppkg AS relation,
'信用卡' AS category,1 as total,
0 as percent
FROM (select rank.apppkg as apppkg, rank.tag as tag,rank.norm_tfidf as norm_tfidf 
from (select n.apppkg as apppkg,n.tag as tag ,n.norm_tfidf as norm_tfidf ,
Row_number() over(partition by n.apppkg,n.tag ORDER BY  n.norm_tfidf DESC ) as rank 
from $dim_app_tag_system_mapping_par n where version ='1000'  ) rank where rank.rank =1) a
where a.tag = '信用卡' group by a.apppkg , a.tag having SUM(a.norm_tfidf) >1.33493881518209
"
spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.tools.OnlineUniversalTools \
--driver-memory 10G \
--executor-memory 12G \
--executor-cores 3 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.dynamicAllocation.executorIdleTimeout=15s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=5120 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.default.parallelism=2000 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.driver.maxResultSize=4g \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.3 \
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
 FROM $dws_device_install_app_re_status_di device
 WHERE device.refine_final_flag=1
 AND device.day <=${day} AND device.day >${bday} 
) device_filter
LEFT  JOIN (select apppkg,pkg from $dim_app_pkg_mapping_par where version='1000' )app_pkg
ON app_pkg.pkg = device_filter.pkg
GROUP BY  NVL(app_pkg.apppkg,device_filter.pkg), device_filter.device, device_filter.day\",
        \"mappingtype\": \"${mappingtype}\",
        \"computeType\": \"${computeType}\",
        \"windowTime\": \"${windowTime}\",
		\"processfield\": \"\"
   }
}
"
