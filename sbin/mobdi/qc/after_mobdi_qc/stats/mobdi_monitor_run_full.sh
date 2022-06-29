#!/bin/bash

set -e -x
if [ $# -lt 2 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>' 'time_window'"
     exit 1
fi

day=$1
time_window=$2
start_day=`date -d "$day $time_window days ago" +%Y%m%d`

#table_name="rp_mobdi_app.rp_device_profile_incr_orig where day='$day'"
table_name="rp_mobdi_app.device_profile_label_full_par where version='${day}.1000'"
#field_list="country_cn,city_cn,car,agebin,gender,kids"
field_filter_list="device,tag_list,applist,catelist,version,tot_install_apps,model_level,last_active,first_active_time,price,group_list,workplace,residence,processtime,processtime_all"
mysqlInfoStr="{\"userName\":\"root\",\"pwd\":\"mobtech2019java\",\"dbName\":\"mobdi_monitor\",\"host\":\"10.21.33.28\",\"port\":3306,\"tableName\":\"full_fields_monitor\",\"start_date\":$start_day,\"end_date\":$day,\"batchsize\":\"10000\",\"repartition\":\"8\"}"
#mailList="zhtli@mob.com;yqzhou@mob.com"
#mail_list="zhtli@mob.com"
mail_list="wangych@mob.com,zhaox@uuzu.com,DIMonitor@mob.com,zhanjf@mob.com,zhangxinyuan@mob.com"
ignores_value=0.001

mysql -h10.21.33.28 -u root -p'mobtech2019java' -P3306 -e "delete from mobdi_monitor.full_fields_monitor where table_name='rp_mobdi_app.device_profile_label_full_par' and stats_date=$day"

spark2-submit --master yarn \
            --deploy-mode cluster \
            --class com.mob.mobdi.utils.MonitorTool \
            --name mobdi_monitor_full_${day} \
            --driver-memory 4g \
            --executor-memory 12G \
            --executor-cores 4 \
            --conf spark.shuffle.service.enabled=true \
            --conf spark.dynamicAllocation.enabled=true \
            --conf spark.dynamicAllocation.minExecutors=30 \
            --conf spark.dynamicAllocation.maxExecutors=150 \
            --conf spark.dynamicAllocation.executorIdleTimeout=15s \
            --conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
            --conf spark.yarn.executor.memoryOverhead=4096 \
            --conf spark.kryoserializer.buffer.max=1024 \
            --conf spark.default.parallelism=2100 \
            --conf spark.sql.shuffle.partitions=2100 \
            --conf spark.driver.maxResultSize=4g \
            --conf spark.storage.memoryFraction=0.4 \
            --conf spark.shuffle.memoryFraction=0.4 \
            --driver-java-options "-XX:MaxPermSize=1024m" \
            /home/dba/lib/MobDI_Monitor-1.0-SNAPSHOT-jar-with-dependencies.jar $day "$table_name" "$field_filter_list"  "$mysqlInfoStr" 0.1 "$mail_list" $ignores_value
