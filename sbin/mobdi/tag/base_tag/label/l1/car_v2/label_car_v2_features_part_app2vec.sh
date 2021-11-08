#!/bin/sh

set -x -e

: '
@owner:yanhw
@describe: part0 100维的app2vec 在scala里join
@projectName:MOBDI
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi
day=$1

source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp

#input
device_applist_new=${dim_device_applist_new_di}

#tmp
tmp_part_car_app2vec=${tmpdb}.tmp_part_car_app2vec

#put
output_table=${tmpdb}.tmp_car_pre_app2vec

sql="
select device,
avg(coalesce(d1,0)) as d1,avg(coalesce(d2,0)) as d2,avg(coalesce(d3,0)) as d3,avg(coalesce(d4,0)) as d4,avg(coalesce(d5,0)) as d5,
avg(coalesce(d6,0)) as d6,avg(coalesce(d7,0)) as d7,avg(coalesce(d8,0)) as d8,avg(coalesce(d9,0)) as d9,avg(coalesce(d10,0)) as d10,
avg(coalesce(d11,0)) as d11,avg(coalesce(d12,0)) as d12,avg(coalesce(d13,0)) as d13,avg(coalesce(d14,0)) as d14,avg(coalesce(d15,0)) as d15,
avg(coalesce(d16,0)) as d16,avg(coalesce(d17,0)) as d17,avg(coalesce(d18,0)) as d18,avg(coalesce(d19,0)) as d19,avg(coalesce(d20,0)) as d20,
avg(coalesce(d21,0)) as d21,avg(coalesce(d22,0)) as d22,avg(coalesce(d23,0)) as d23,avg(coalesce(d24,0)) as d24,avg(coalesce(d25,0)) as d25,
avg(coalesce(d26,0)) as d26,avg(coalesce(d27,0)) as d27,avg(coalesce(d28,0)) as d28,avg(coalesce(d29,0)) as d29,avg(coalesce(d30,0)) as d30,
avg(coalesce(d31,0)) as d31,avg(coalesce(d32,0)) as d32,avg(coalesce(d33,0)) as d33,avg(coalesce(d34,0)) as d34,avg(coalesce(d35,0)) as d35,
avg(coalesce(d36,0)) as d36,avg(coalesce(d37,0)) as d37,avg(coalesce(d38,0)) as d38,avg(coalesce(d39,0)) as d39,avg(coalesce(d40,0)) as d40,
avg(coalesce(d41,0)) as d41,avg(coalesce(d42,0)) as d42,avg(coalesce(d43,0)) as d43,avg(coalesce(d44,0)) as d44,avg(coalesce(d45,0)) as d45,
avg(coalesce(d46,0)) as d46,avg(coalesce(d47,0)) as d47,avg(coalesce(d48,0)) as d48,avg(coalesce(d49,0)) as d49,avg(coalesce(d50,0)) as d50,
avg(coalesce(d51,0)) as d51,avg(coalesce(d52,0)) as d52,avg(coalesce(d53,0)) as d53,avg(coalesce(d54,0)) as d54,avg(coalesce(d55,0)) as d55,
avg(coalesce(d56,0)) as d56,avg(coalesce(d57,0)) as d57,avg(coalesce(d58,0)) as d58,avg(coalesce(d59,0)) as d59,avg(coalesce(d60,0)) as d60,
avg(coalesce(d61,0)) as d61,avg(coalesce(d62,0)) as d62,avg(coalesce(d63,0)) as d63,avg(coalesce(d64,0)) as d64,avg(coalesce(d65,0)) as d65,
avg(coalesce(d66,0)) as d66,avg(coalesce(d67,0)) as d67,avg(coalesce(d68,0)) as d68,avg(coalesce(d69,0)) as d69,avg(coalesce(d70,0)) as d70,
avg(coalesce(d71,0)) as d71,avg(coalesce(d72,0)) as d72,avg(coalesce(d73,0)) as d73,avg(coalesce(d74,0)) as d74,avg(coalesce(d75,0)) as d75,
avg(coalesce(d76,0)) as d76,avg(coalesce(d77,0)) as d77,avg(coalesce(d78,0)) as d78,avg(coalesce(d79,0)) as d79,avg(coalesce(d80,0)) as d80,
avg(coalesce(d81,0)) as d81,avg(coalesce(d82,0)) as d82,avg(coalesce(d83,0)) as d83,avg(coalesce(d84,0)) as d84,avg(coalesce(d85,0)) as d85,
avg(coalesce(d86,0)) as d86,avg(coalesce(d87,0)) as d87,avg(coalesce(d88,0)) as d88,avg(coalesce(d89,0)) as d89,avg(coalesce(d90,0)) as d90,
avg(coalesce(d91,0)) as d91,avg(coalesce(d92,0)) as d92,avg(coalesce(d93,0)) as d93,avg(coalesce(d94,0)) as d94,avg(coalesce(d95,0)) as d95,
avg(coalesce(d96,0)) as d96,avg(coalesce(d97,0)) as d97,avg(coalesce(d98,0)) as d98,avg(coalesce(d99,0)) as d99,avg(coalesce(d100,0)) as d100
from seed as x
left join
  (select * from $apppkg_app2vec_par_wi where day = '20210124') y
on x.pkg = y.apppkg
group by device
"


spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.newscore.App2Vec \
--driver-memory 4G \
--executor-memory 12G \
--executor-cores 4 \
--queue root.yarn_data_compliance \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=100 \
--conf spark.dynamicAllocation.maxExecutors=200 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.speculation=true \
--conf spark.driver.maxResultSize=4g \
--conf spark.default.parallelism=2000 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=1024m -XX:PermSize=256m" \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar \
--inputTable $device_applist_new \
--outputTable $tmp_part_car_app2vec \
--day $day \
--sql "$sql" \
--flag "car_app2_vec"

#去小文件
hive -v -e "
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=250000000;
set mapred.min.split.size.per.rack=250000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task=250000000;
set hive.exec.reducers.bytes.per.reducer=256000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers=None;

INSERT OVERWRITE TABLE $output_table PARTITION(day)
SELECT \`(stage)?+.+\`
FROM $tmp_part_car_app2vec
WHERE day = '$day';
"

#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table} drop if exists partition($old_version);"
done

hive -v -e "alter table ${tmp_part_car_app2vec} drop if exists partition(day='$b7day');"