#!/bin/bash

set -e -x

: '
@owner:luost
@describe:学历标签part7
@projectName:MOBDI
'

source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
tmpdb=${dm_mobdi_tmp}

#input
device_applist_new=${dim_device_applist_new_di}
#label_device_pkg_install_uninstall_year_info_mf=${label_device_pkg_install_uninstall_year_info_mf}

#mapping
#app_pkg_mapping_par=dim_sdk_mapping.app_pkg_mapping_par
#mapping_edu_app_index0=dim_sdk_mapping.mapping_edu_app_index0
install_uninstall_year_db=${label_device_pkg_install_uninstall_year_info_mf%.*}
install_uninstall_year_tb=${label_device_pkg_install_uninstall_year_info_mf#*.}
#ouput
tmp_edu_score_part7=${tmpdb}.tmp_edu_score_part7


hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=16;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

with seed as (
  select device
  from $device_applist_new
  where day = '$day'
  group by device
),

edu_uninstall_1y as (
    select a.device,
           b.index,
           1.0 as cnt
    from
    (
        select t3.device,coalesce(t4.apppkg, t3.apppkg) apppkg
        from
        (
            select t1.device, coalesce(t2.apppkg, t1.pkg) as apppkg
            from
            (
                select device,pkg
                from $label_device_pkg_install_uninstall_year_info_mf
                where day = GET_LAST_PARTITION('$install_uninstall_year_db','$install_uninstall_year_tb','day')
                and refine_final_flag = -1
            ) t1
            left join
            (
                select *
                from $dim_app_pkg_mapping_par
                where version = '1000'
            ) t2
            on t1.pkg = t2.pkg
            group by t1.device,coalesce(t2.apppkg,t1.pkg)
        )t3
        left join
        (
            select *
            from $dim_app_pkg_mapping_par
            where version = '1000'
        ) t4
        on t3.apppkg = t4.pkg
        group by t3.device,coalesce(t4.apppkg, t3.apppkg)
    ) a
    inner join
    (
        select apppkg,
               index
        from $mapping_edu_app_index0
        where version = '1000'
    ) b
    on a.apppkg=b.apppkg
)

insert overwrite table $tmp_edu_score_part7 partition (day='$day')
select seed.device,
       if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
       if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from seed
left join
(
    select device,
           index,
           cnt
    from edu_uninstall_1y
)t
on seed.device = t.device
group by seed.device;
"

