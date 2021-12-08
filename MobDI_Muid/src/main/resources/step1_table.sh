#!/bin/bash
set -x -e

tmp_db=dm_mid_master
install_all="dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di"
brand_mapping="dm_sdk_mapping.brand_model_mapping_par"

pkg_it="$tmp_db.pkg_it_duid_par_tmp"
factory_mapping="dm_mobdi_tmp.dim_muid_factory_model_category"
pkg_it_category="$tmp_db.pkg_it_duid_category_tmp"

token_vertex_par="$tmp_db.duid_vertex_par_tmp"
muid_mapping="$tmp_db.old_new_unid_mapping_par"

duid_fsid_mapping=$tmp_db.duid_unid_mapping

month=$1
version=$2
step_all=1
if [[ -n $3 ]];then
step_all=$3
fi
: '
hive -e "
create table if not exists $token_vertex_par(
id1 string,
id2 string
) partitioned by (
month string,
version string
) stored as orc;

create table if not exists $muid_mapping (
old_id string,
new_id string
) partitioned by (
month string,
version string
) stored as orc;
"
'
if [[ $step_all -eq 1 ]];then
/opt/mobdata/sbin/spark-submit \
--master yarn \
--deploy-mode cluster \
--queue root.yarn_data_compliance1 \
--name Step1Pkg2Vertex_$version \
--class com.mob.mid.Step1Pkg2Vertex \
--conf spark.dynamicAllocation.maxExecutors=80 \
--conf spark.dynamicAllocation.minExecutors=10 \
--executor-memory 25g \
--executor-cores 3 \
--driver-memory 8g \
--conf spark.kryoserializer.buffer.max=128m \
--conf spark.driver.maxResultSize=1024m \
--conf spark.network.maxRemoteBlockSizeFetchToMem=10m \
--conf spark.shuffle.accurateBlockThreshold=10m \
--conf spark.executor.memoryOverhead=10240 \
--conf spark.driver.maxResultSize=5g \
--conf spark.rpc.askTimeout=500 \
--conf spark.sql.shuffle.partitions=2000 \
./muid.jar 100 5000 7 $month $version $pkg_it_category $token_vertex_par
fi

/opt/mobdata/sbin/spark-submit --master yarn \
--deploy-mode cluster \
--queue root.yarn_data_compliance1 \
--name Step2TokenConnectedComponents_$version \
--class com.mob.mid.Step2TokenConnectedComponents \
--conf spark.dynamicAllocation.maxExecutors=150 \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.default.parallelism=300 \
--conf spark.sql.shuffle.partitions=300 \
--executor-memory 30g \
--executor-cores 2 \
--conf spark.executor.memoryOverhead=10240 \
--conf spark.driver.maxResultSize=5g \
--conf spark.kryoserializer.buffer.max=128m \
--conf spark.driver.maxResultSize=1024m \
--conf spark.network.maxRemoteBlockSizeFetchToMem=256m \
--conf spark.shuffle.accurateBlockThreshold=256m \
./muid.jar $month $version 10 $token_vertex_par $muid_mapping


