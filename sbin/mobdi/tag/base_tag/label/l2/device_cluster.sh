#!/bin/bash
set -e -x

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <date>"
    exit 1
fi
: '
@owner:xdzhang
@describe:从相关目录导入数据到表
@projectName:mobdi
@BusinessName:profile_model_device_cluster
@SourceTable:rp_mobdi_app.device_tag_tfidf_inc_par,dw_mobdi_md.device_tfidf_incr_list,dw_mobdi_md.device_cluster_incr
@TargetTable:dw_mobdi_md.device_tfidf_incr_list,dw_mobdi_md.device_cluster_incr,rp_mobdi_app.device_cluster_incr,rp_mobdi_app.device_cluster_full
@TableRelation:Dir_device_cluster_incr_Path->rp_mobdi_app.device_cluster_incr,Dir_device_cluster_full_output_path->rp_mobdi_app.device_cluster_full
'

source /home/dba/mobdi_center/conf/hive-env.sh

mddb=${dm_mobdi_tmp}

#input
device_applist_new=${dim_device_applist_new_di}

#output
device_tfidf_incr_list=${mddb}.device_tfidf_incr_list
device_cluster_incr=${mddb}.device_cluster_incr

:<<!
实现功能:使用kmeans算法对设备聚类
实现步骤: 1.从rp_mobdi_app.device_tag_tfidf_inc_par表读取设备的tfidf列表数据, 在spark中使用KMeans算法对设备进行聚类,
            结果存入dw_mobdi_md.device_cluster_incr表的当日分区
          2.将dw_mobdi_md.device_cluster_incr当日分区数据存入rp_mobdi_app.device_cluster_incr表的当日分区
          3.删除dw_mobdi_md.device_cluster_incr表的当日分区
!

cloudera_path=/opt/cloudera/parcels/CDH/bin
#export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:MobAppDeviceMRDriver.jar
DATE=$1
preday=`date -d "$DATE -1 days" "+%Y%m%d"`
row_dim_num=850
col_dim_num=570
hive -v -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function explode_tags as 'com.youzu.mob.java.udtf.ExplodeTags';
insert overwrite table $device_tfidf_incr_list
select device,concat(concat_ws(',',collect_list(tag)),'=',concat_ws(',',collect_list(tfidf))) as tfidflist
from
(
  select device,tag,tfidf
  from $label_l1_taglist_di
  lateral view explode_tags(tag_list) tag_tmp as tag,tfidf
  where day='${DATE}'
  and cast(tag as int)<=849
)u
group by device;
"
tfidfsql="SELECT * from $device_tfidf_incr_list"
device_cluster_incr_Path=/dmgroup/dba/dailyrun/device_cluster_inc_mobdi_muid
model_path=/dmgroup/dba/modelpath/kmeans_model/report/device_cluster
$cloudera_path/spark-submit --master yarn-cluster \
--class com.youzu.mob.score.KMeansStdPCAScoring \
--name "KMeansStdPCAScoring(test2)" \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.executorIdleTimeout=600000 \
--conf spark.dynamicAllocation.minExecutors=3 \
--conf spark.dynamicAllocation.maxExecutors=80 \
--driver-memory 8G \
--executor-cores 4 \
--executor-memory 12G \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.default.parallelism=2000 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.network.timeout=300000 \
--conf spark.core.connection.ack.wait.timeout=300000 \
--conf spark.akka.timeout=300000 \
--conf spark.storage.blockManagerSlaveTimeoutMs=300000 \
--conf spark.shuffle.io.connectionTimeout=300000 \
--conf spark.rpc.askTimeout=300000 \
--conf spark.rpc.lookupTimeout=300000 \
--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=1024m -XX:PermSize=256m" \
/home/dba/lib/appAnnie-spark-1.0-prod-jar-with-dependencies.jar \
"${tfidfsql}" $model_path/std.txt $model_path/mean.txt $model_path/pc.csv $model_path/zhaox_cluster_40 $device_cluster_incr_Path $row_dim_num $col_dim_num


$cloudera_path/hive -e "LOAD DATA INPATH '$device_cluster_incr_Path' OVERWRITE INTO TABLE $device_cluster_incr PARTITION(day='$DATE');"
$cloudera_path/hive -e "insert overwrite table $device_cluster_incr partition(day='$DATE')  select device,cluster from $device_cluster_incr where day='$DATE'"
$cloudera_path/hive -e "alter table $device_cluster_incr drop  partition(day='$DATE')"

