#!/bin/bash

: '步骤
1. 插入30天内包名不为空且ieid/oiid任一不为空的在装应数据     dw_install_app_all_rta_table
2. 从dm_mobdi_master.dwd_gaas_rta_id_data_di中提取出1周内的去重ieid、oiid数据，入结果表     dw_gaas_id_data_di_rta_table
3. 更新RTA异常设备池(对应oiid数>2的imei，及对应ieid数>3的oiid均入黑名单，需继承历史黑名单)     dw_gaasid_blacklist_table
4. RTA设备信息池(RTA请求设备信息去重表过滤黑名单，不继承历史数据)     dw_gaasid_final_table
5. RTA设备池与在装应用清洗表取交集      dw_gaasid_in_installid_table
6. RTA最终设备池打一、二级标签,向RTA设备标签表插入数据（因在装应用包名未清洗，所以需先跟数仓的映射表映射取到清洗后的包名(优先取清洗后的包名，未清洗的则保留   dw_device_with_cate_table
7. 向RTA正负向设备表插入设备信息及二级游戏分类的应用数    dw_device_with_cate_installcount_table
8. 写入0324规则数据,新建临时表，包括本次所有0324推荐设备数据    dm_device_rec_for_0324_pre_table
9. 新建临时表，包括本次所有0324推荐设备数据+pre表中没有但在0324表次新分区中有，且未发回安装数据且不在黑名单的设备     dm_device_rec_for_0324_pre_second_table
10. 更新至0324规则结果表最新分区，增加增改、删、不变的状态     dm_device_rec_for_0324_table
11. 新建0325临时结果表，包括付费天数及距今付费间隔时间维度的设备     dm_device_rec_for_0325_pre_table
12. 更新至0325规则结果表最新分区，增加增改、删、不变的状态     dm_device_rec_for_0325_table
13. 新建临时表，包括本次所有0326推荐设备数据     dm_device_rec_for_0326_pre_table
14. 新建临时表，包括本次所有0326推荐设备数据+pre表中没有但在0326表次新分区中有，且未发回安装数据且不在黑名单的设备     dm_device_rec_for_0326_pre_second_table
15. 更新至0326规则结果表最新分区，增加增改、删、不变的状态     dm_device_rec_for_0326_table
16. rta历史请求时存疑的ieid,oiid      dw_gaasid_doubtful_table
17. 新建临时表，包括本次所有0327推荐设备数据     dm_device_rec_for_0327_pre_table
18. 更新至0327规则结果表最新分区，增加增改、删、不变的状态     dm_device_rec_for_0327_table
19. 汇总规则数据(0.5.1后的版本使用)     dm_device_rec_for_all_table
'

set -e -x

if [[ $# -lt 1 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>'"
     exit 1
fi

insert_day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par
#app_category_mapping_par=dm_sdk_mapping.app_category_mapping_par

#获取最新分区
full_par=`hive -e "show partitions $dw_gaasid_blacklist;"  |sort -rn | awk -F "=" '{print $2}' | head -n 1`
dim_app_pkg_par=`hive -e "show partitions $dim_app_pkg_mapping_par;"  |sort -rn | awk -F "=" '{print $2}' | head -n 1`
app_category_par=`hive -e "show partitions $app_category_mapping_par;"  |sort -rn | awk -F "=" '{print $2}' | head -n 1`
echo "$full_par"
echo "$dim_app_pkg_par"
echo "$app_category_par"

spark2-submit --master yarn \
--executor-memory 20G \
--driver-memory 6G \
--executor-cores 3 \
--conf "spark.dynamicAllocation.minExecutors=50" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.dynamicAllocation.initialExecutors=50" \
--name "rta_device_update" \
--deploy-mode cluster \
--class com.youzu.mob.rta.rta_device_update \
--conf spark.sql.shuffle.partitions=3000 \
--conf spark.executor.memoryOverhead=4096 \
--conf spark.default.parallelism=2000 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.network.timeout=300000 \
--conf spark.core.connection.ack.wait.timeout=300000 \
--conf spark.akka.timeout=300000 \
--conf spark.storage.blockManagerSlaveTimeoutMs=300000 \
--conf spark.shuffle.io.connectionTimeout=300000 \
--conf spark.rpc.askTimeout=300000 \
--conf spark.rpc.lookupTimeout=300000 \
--queue root.sdk.mobdashboard_test \
/home/dba/mobdi_center/lib//MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar $insert_day $full_par $dim_app_pkg_par $app_category_par