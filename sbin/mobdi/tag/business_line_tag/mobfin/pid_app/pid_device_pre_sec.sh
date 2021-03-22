#!/bin/bash
: '
@owner:luost
@describe:pid对应device预处理
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

#入参
day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

#源表
#dim_id_mapping_android_sec_df=dm_mobdi_mapping.dim_id_mapping_android_sec_df

# mapping
#dim_pid_attribute_sec_df=dm_mobdi_mapping.dim_pid_attribute_full_par_secview

#输出表
tmp_anticheat_pid_device_pre_sec=dw_mobdi_tmp.tmp_anticheat_pid_device_pre_sec


androidPartition=`hive -S -e "show partitions $dim_id_mapping_android_sec_df" | sort |tail -n 1 `

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3700m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx3700m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3700m' -XX:+UseG1GC;
set hive.optimize.skewjoin=true;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
SET hive.auto.convert.join=true;
set hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=16;
set hive.exec.reducers.bytes.per.reducer=2147483648;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapred.job.reuse.jvm.num.tasks=10;
set mapred.tasktracker.map.tasks.maximum=24;
set mapred.tasktracker.reduce.tasks.maximum=24;
set mapreduce.job.reduce.slowstart.completedmaps=0.8;

set mapred.job.name=pid_device_pre_sec;

ADD jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function explode_tags as 'com.youzu.mob.java.udtf.ExplodeTags';

insert overwrite table $tmp_anticheat_pid_device_pre_sec partition (day = '$day')
select pid,device
from
(
    select pid,device,
           row_number() over(partition by pid order by time desc) rank
    from
    (
        select a.device as device,mytable.pid as pid,mytable.time as time
        from 
        (
            select device,concat(pid,'=',pid_ltm) as list
            from $dim_id_mapping_android_sec_df
            where $androidPartition
            and pid is not null
            and pid <> ''
        ) a
        lateral view explode_tags(list) mytable as pid,time
    ) b
    left semi join    --将之前判断区号取国内手机号的逻改为通过属性表关联
    (
        select * 
        from $dim_pid_attribute_sec_df
        where country = '中国'
    ) pid_attribute 
    on b.pid = pid_attribute.pid_id
) c
where rank = 1;
"



