#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: device的bssid_cnt
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
appdb=$dm_mobdi_report

#input
device_applist_new=${dim_device_applist_new_di}

#mapping
#mapping_contacts_words_20000_sec

#view
#dim_pid_attribute_full_par_secview

#output
output_table_v3=${tmpdb}.tmp_score_part5_v3

pidPartition=`hive -e "show partitions $dim_device_pid_merge_df" | awk -v day=${day} -F '=' '$2<=day {print $0}'| sort| tail -n 1`
##v3版的part5通讯录特征
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
with seed as
(
  select device
  from $device_applist_new
  where day = '$day'
  group by device
)

insert overwrite table ${output_table_v3} partition(day='${day}')
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
        select device,index,1.0 cnt
        from
        (
            select x.device,y.word_index
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
                                    from $dim_device_pid_merge_df
                                    where $pidPartition
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
                from $mapping_contacts_words_20000_sec
                where version = '1000'
            )y
            on x.pid = y.pid
        )xx
        lateral view explode(word_index) n as index
    )yy
    group by device
)b
on a.device = b.device;
"

#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table_v3} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table_v3} drop if exists partition($old_version)"
done