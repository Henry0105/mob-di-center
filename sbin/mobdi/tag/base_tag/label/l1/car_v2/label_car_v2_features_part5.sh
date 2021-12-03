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
source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
tmpdb=dm_mobdi_tmp

#input
#device_applist_new="dm_mobdi_mapping.device_applist_new"

#mapping
#mapping_contacts_words_20000_sec="dm_sdk_mapping.mapping_contacts_words_20000_sec"
#id_mapping_android_sec_df="dim_mobdi_mapping.id_mapping_android_sec_df"
#android_id_mapping_sec_df="dm_mobdi_mapping.android_id_mapping_sec_df"
#mobdi_analyst_test.zx_0204_car_word_index_chi -> dm_sdk_mapping.car_word_index_chi

#car_word_index_chi="dm_sdk_mapping.car_word_index_chi"

#dim_pid_attribute_full_par_secview="dm_mobdi_mapping.dim_pid_attribute_full_par_secview"

## 结果临时表
output_table=${tmpdb}.tmp_car_score_part5

id_mapping_db=${id_mapping_android_sec_df%.*}
id_mapping_tb=${id_mapping_android_sec_df#*.}

#id_mapping最新分区
pidPartition=`hive -e "show partitions $dim_device_pid_merge_df" | awk -v day=${day} -F '=' '$2<=day {print $0}'| sort| tail -n 1`


##v3版的part5通讯录特征
hive -v -e "
with seed as
(
  select device
  from $dim_device_applist_new_di
  where day = '$day'
  group by device
)
insert overwrite table ${output_table} partition(day='${day}')
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
        select x.device,y.index,1.0 cnt
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
         select a.pid,b.index_after_chi index,1.0 cnt
            from
            ( select pid,index_old
              from $mapping_contacts_words_20000_sec
              lateral view explode(word_index) n as index_old
              where version = '1000'
            ) a
            join $dim_car_word_index_chi b
            on a.index_old = b.index_before_chi
        ) y
        on x.pid = y.pid
    )xx
    group by device
)b
on a.device = b.device;
"