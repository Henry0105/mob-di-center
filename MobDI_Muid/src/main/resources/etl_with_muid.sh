#!/bin/bash
set -x
cd $(dirname $0)

if [[ $# -lt 2 ]]
then
echo "参数不足: 1.原表 2.目标表 3.[开始]日期 [4.结束日期]"
exit 1
fi

raw_table=$2
dw_table=$2
yesterday=$1
muid_field="muid"

device_col=$(grep "^$dw_table " ./table.conf |awk '{print $2}')

queue="root.yarn_etl.etl"
#queue="root.yarn_data_compliance"
#queue="root.yarn_mobdi.dba"
if [[ -z $device_col || $device_col == '#' ]]
then
device_col="deviceid"
fi

select=$(hive -e "desc ${dw_table}" | awk -F '\t' '{print $1,","}' | xargs echo | sed s/[[:space:]]//g|awk -F ',,#' '{print $1}')


par=$(echo $select|awk -F ',' '{print $(NF)}')

select_muid=","$(echo $select|sed "s/,$par//g")","
if [[ $select_muid == *,$muid_field,* ]] ;then
muid=1
else
muid=0
fi

if [[ $select_muid == *,sysver,* && $select_muid == *,token,* ]] ;then
device_col_deal=1
else
device_col_deal=0
fi

select_muid=$(echo $select_muid|sed "s/,$muid_field,/,coalesce(b.muid,a.$device_col) $muid_field,/g"| awk -F '#' '{print substr($1,2,(length($0)-2))}')

device_token_muid_mapping="dm_mobdi_mapping.device_token_muid_mapping_full_par"
device_old_muid_mapping="dm_mobdi_mapping.device_old_muid_mapping_full_par"

udf_class="com.mob.udf.SysverGEAndQ"
if [[ $yesterday -ge "20200909" ]]
then
udf_class="com.mob.udf.SysverGEAndQNew"
fi

sqlset="
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set mapreduce.job.queuename=$queue;
set hive.merge.size.per.task = 128000000;
set hive.exec.dynamic.partition.mode=nonstrict;
add jars hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/etl_udf.jar;
create temporary function ge_andq as '$udf_class';
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager.jar;
create temporary function sha as 'com.youzu.mob.java.udf.SHA1Hashing';
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
"
if [[ $muid -gt 0 ]];then
    if [[ $device_col_deal -gt 0  ]];then
        mapping_par=$(hive -e "show partitions $device_token_muid_mapping" |tail -1 |awk -F '=' '{print $2}')
        hive -e "
        $sqlset
        insert overwrite table ${dw_table} partition ($par)
        select $select_muid,a.$par from
        (select * from ${raw_table} where $par = $yesterday) a
        left join
        (select device_token,muid,day from $device_token_muid_mapping where day =$mapping_par)b
        on if(ge_andq(sysver) and token is not null and trim(token)<>'',sha(trim(token)),a.$device_col) = b.device_token
        "
    else
        mapping_par=$(hive -e "show partitions $device_old_muid_mapping" |tail -1 |awk -F '=' '{print $2}')
        hive -e "
        $sqlset
        insert overwrite table ${dw_table} partition ($par)
        select $select_muid,a.$par from
        (select * from ${raw_table} where $par = $yesterday ) a
        left join
        (select device_old,muid,day from $device_old_muid_mapping where day =$mapping_par) b
        on a.$device_col = b.device_old
        "
    fi
else
    hive -e "
    $sqlset
    insert overwrite table ${dw_table} partition ($par)
    select $select from ${raw_table} where $par = $yesterday
    "
fi

if [[ $? -ne 0 ]];then
echo "$raw_table $yesterday " >> logs/error.log
exit 1
fi