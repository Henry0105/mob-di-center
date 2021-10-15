#!/bin/sh

set -x -e

: '
@owner:hugl
@describe: device的bssid_cnt
@projectName:MOBDI
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
tmpdb=${dw_mobdi_md}

##tmpdb="mobdi_test"
##appdb="mobdi_test"
output_table="${tmpdb}.tmp_income1001_part6"
device_applist_new=${dim_device_applist_new_di}
#mapping_contacts_word2vec2_sec="dm_sdk_mapping.mapping_contacts_word2vec2_sec"
android_id_mapping_sec_df=${dim_id_mapping_android_sec_df}
#dim_pid_attribute_full_par_secview="dim_mobdi_mapping.dim_pid_attribute_full_par_secview"
android_id_mapping_sec_df_db=${android_id_mapping_sec_df%.*}
android_id_mapping_sec_df_tb=${android_id_mapping_sec_df#*.}
full_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('$android_id_mapping_sec_df_db', '$android_id_mapping_sec_df_tb', 'version');
drop temporary function GET_LAST_PARTITION;
"
full_last_version=(`hive -e "$full_partition_sql"`)

##取的年龄标签part6 v3版本
HADOOP_USER_NAME=dba hive -e"
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts='-Xmx6g';
SET mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8196;
SET mapreduce.reduce.java.opts='-Xmx6g';

drop table if exists ${output_table};
create table if not exists ${output_table} as
with seed as
(
  select device
  from $device_applist_new
  where day = '$day'
  group by device
)
select a.device,
       if(b.device is null,array(0), b.index) as index,
       if(b.device is null,array(0.0), b.cnt) as cnt
from seed a
left join
(
    select device,
           collect_list(index) as index,
           collect_list(cnt) as cnt
    from
    (
        select device,index,cnt
        from
        (
            select x.device,y.w2v_100
            from
            (
                select device,
                       pid
                from
                (
                    select *,row_number() over(partition by device order by pn_tm desc) rn
                    from
                    (
                        select d.device,d.pid,d.pn_tm
                        from
                        (
                            select device,
                                   n.pid,
                                   n.pn_tm
                            from
                            (
                                select a.device,
                                       concat(pid,'=',pid_ltm) as pid_list
                                from seed a
                                inner join
                                (
                                    select device,pid,pid_ltm
                                    from $android_id_mapping_sec_df
                                    where version = '$full_last_version'
                                ) b
                                on a.device = b.device
                            )c
                            lateral view explode_tags(pid_list) n as pid,pn_tm
                        )d
                        left join
                        (
                            select pid_id,country_code
                            from $dim_pid_attribute_full_par_secview
                        )e
                        on d.pid = e.pid_id
                        where e.country_code = 'cn'
                    )f
                )g
                where rn = 1
            )x
            inner join
            (
                select *
                from $mapping_contacts_word2vec2_sec
                where version = '1000'
            )y
            on x.pid = y.pid
        )xx
        lateral view posexplode(w2v_100) n as index,cnt
    )yy
    group by device
)b
on a.device = b.device;

"
