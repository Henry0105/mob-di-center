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


/opt/mobdata/sbin/spark-submit --master yarn \
--deploy-mode cluster \
--name Step2TokenConnectedComponents_$version \
--class com.mob.mid.ConnectedAllUnid \
--conf spark.dynamicAllocation.maxExecutors=200 \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.default.parallelism=300 \
--conf spark.sql.shuffle.partitions=300 \
--executor-memory 20g \
--executor-cores 2 \
--conf spark.executor.memoryOverhead=10240 \
--conf spark.driver.maxResultSize=5g \
--conf spark.kryoserializer.buffer.max=128m \
--conf spark.driver.maxResultSize=1024m \
--conf spark.network.maxRemoteBlockSizeFetchToMem=256m \
--conf spark.shuffle.accurateBlockThreshold=256m \
./muid.jar 10 $muid_mapping


