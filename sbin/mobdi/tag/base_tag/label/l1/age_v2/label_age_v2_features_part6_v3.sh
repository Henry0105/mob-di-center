#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: device的part6
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
#mapping_contacts_word2vec2_sec

#output
output_table_v3=${tmpdb}.tmp_score_part6_v3

pidPartition=`hive -e "show partitions $dim_device_pid_merge_df" | awk -v day=${day} -F '=' '$2<=day {print $0}'| sort| tail -n 1`
##-----part6_v3
hive -v -e "
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts='-Xmx6g';
SET mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8196;
SET mapreduce.reduce.java.opts='-Xmx6g';

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
                                    from $dim_device_pid_merge_df
                                    where $pidPartition
                                    and pid <> ''
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