#!/bin/bash
: '
@owner:luost
@describe:游戏兴趣度标签（活跃天数、留存天数）
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#源表
#dws_device_active_applist_di=dm_mobdi_topic.dws_device_active_applist_di
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di

#mapping表
#dim_game_app_tags_pkg=dim_sdk_mapping.dim_game_app_tags_pkg
#game_app_tags_pkg=dim_sdk_mapping.game_app_tags_pkg
#dim_game_tag_mapping=dim_sdk_mapping.dim_game_tag_mapping
#game_tag_mapping=dim_sdk_mapping.game_tag_mapping
#dim_game_type_threshhold=dim_sdk_mapping.dim_game_type_threshhold
#game_type_threshhold=dim_sdk_mapping.game_type_threshhold

#目标表
#label_device_game_active_label_wi=dm_mobdi_report.label_device_game_active_label_wi
#label_device_game_retention_label_wi=dm_mobdi_report.label_device_game_retention_label_wi

: '
@part_1:
实现功能:生成活跃标签表数据
实现逻辑:
        1.创建主题临时表cate_theme_table
        2.取时间窗内dws_device_active_applist_di数据按照apppkg匹配cate_theme_table得到cate1_id/theme1_id
        2.再按照device,cate1/theme1,day去重得到设备在各类型下的不重复活跃日期
        3.再进行聚合得到设备在各类型下的活跃天数
输出结果:表名:rp_mobdi_app.label_device_game_active_label_wi
'

tags_pkg_partition=`hive -S -e "show partitions $dim_game_app_tags_pkg" | sort |tail -n 1`
game_tag_partition=`hive -S -e "show partitions $dim_game_tag_mapping" | sort |tail -n 1`
threshholdPatition=`hive -S -e "show partitions $dim_game_type_threshhold" | sort |tail -n 1`

#生成活跃标签表数据
function active_label(){

id=$1
level=$2
partition_flag=$3
tag_type=$4

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

with cate_theme_table as (
    select a.$id,a.apppkg
    from 
    (
        select $id,apppkg,$level
        from $dim_game_app_tags_pkg
        where $tags_pkg_partition
        and $id is not null 
        and $level is not null
        and apppkg is not null
        and apppkg <> ''
        group by $id,apppkg,$level
    ) a
    inner join 
    (
        select t1.tag_id,t1.tag_type,t2.threshhold
        from 
        (
            select tag_id,tag_type
            from $dim_game_tag_mapping
            where $game_tag_partition
            and tag_type = '$tag_type'
        )t1
        inner join
        (
            select tag_type,threshhold
            from $dim_game_type_threshhold
            where $threshholdPatition
            and tag_type = '$tag_type'
        )t2
        on t1.tag_type = t2.tag_type
    ) b
    on a.$id = b.tag_id
    where a.$level >= b.threshhold
)

insert overwrite table $label_device_game_active_label_wi partition(day = '$day',timewindow = $i,flag = $partition_flag)
select device,$id as cate_theme_id,count(1) as active_days
from
(
    select device,$id
    from
    (
        select device,apppkg,day
        from $dws_device_active_applist_di
        where day > '$pday' 
        and day <= '$day'
    ) a
    inner join cate_theme_table
    on a.apppkg = cate_theme_table.apppkg
    group by device,$id,day
)t1
group by device,$id;
"
}

: '
@part_2:
实现功能:生成留存标签表数据
实现逻辑:
        1.创建主题临时表cate_theme_table
        2.取dws_device_install_app_re_status_di中在时间窗内每个device对应pkg的所有安装日期数据 inner join
        每个device对应pkg的最近的卸载日期数据
		3.再按照device,pkg,最近的卸载日分组取最近的安装日期并相减，这样得到device对应每个pkg
        最近一次安装卸载的留存天数
		4.然后与主题表关联得到对应主题后，按照device,主题分组聚合按照留存区间规划留存天数后得到device
        在每个主题下各留存区间内的pkg数量（区间数据）
        5.取dws_device_install_app_re_status_di中在时间窗内每个device对应pkg的所有安装日期数据 inner join主题表
        并按照device,主题分组去重聚合得到device,主题下的新装pkg数据量（新装pkg数据）
        6.区间数据 inner join 新装pkg数据得到结果数据
输出结果:表名:rp_mobdi_app.label_device_game_retention_label_wi
'
#生成留存标签表数据
function retention_label(){

id=$1
level=$2
partition_flag=$3
tag_type=$4

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

with cate_theme_table as (
    select a.$id,a.apppkg
    from 
    (
        select $id,apppkg,$level
        from $dim_game_app_tags_pkg
        where $tags_pkg_partition
        and $id is not null 
        and $level is not null
        and apppkg is not null
        and apppkg <> ''
        group by $id,apppkg,$level
    ) a
    inner join 
    (
        select t1.tag_id,t1.tag_type,t2.threshhold
        from 
        (
            select tag_id,tag_type
            from $dim_game_tag_mapping
            where $game_tag_partition
            and tag_type = '$tag_type'
        )t1
        inner join
        (
            select tag_type,threshhold
            from $dim_game_type_threshhold
            where $threshholdPatition
            and tag_type = '$tag_type'
        )t2
        on t1.tag_type = t2.tag_type
    ) b
    on a.$id = b.tag_id
    where a.$level >= b.threshhold
)

insert overwrite table $label_device_game_retention_label_wi partition (day = '$day',timewindow = $i,flag = $partition_flag)
select interval_table.device,
       interval_table.$id as cate_theme_id,
       interval_table.retention_interval1,
       interval_table.retention_interval2,
       interval_table.retention_interval3,
       pkg_cnt_table.pkg_cnt as install_cnt
from 
(
    select device,
           $id,
           sum(if(interval='le2',1,0)) as retention_interval1,
           sum(if(interval='in3_6',1,0)) as retention_interval2,
           sum(if(interval='ge7',1,0)) as retention_interval3
    from
    (
        select device,pkg,
               case when diff<=2 then 'le2' 
               when diff > 2 and diff <=6 then 'in3_6' 
               else 'ge7' end 
               as interval
        from
        (
            select install.device,install.pkg,
                   datediff(
                   to_date(from_unixtime(UNIX_TIMESTAMP(unstallday,'yyyyMMdd'))),
                   to_date(from_unixtime(UNIX_TIMESTAMP(max(installday),'yyyyMMdd')))
                   ) as diff
            from
            (
                select device,pkg,day as installday
                from $dws_device_install_app_re_status_di
                where refine_final_flag = 1
                and day > '$pday' and day <= '$day'
            ) install
            inner join
            (
                select device,pkg,max(day) as unstallday
                from $dws_device_install_app_re_status_di
                where refine_final_flag = -1
                and day > '$pday' and day <= '$day'
                group by device,pkg
            ) unstall
            on install.device = unstall.device and install.pkg = unstall.pkg
            where installday <= unstallday
            group by install.device,install.pkg,unstallday
        ) device_diff
    ) device_interval 
    inner join cate_theme_table game_pkg
    on device_interval.pkg = game_pkg.apppkg
    group by device,$id
) interval_table
inner join 
(
    select device,$id,count(1) as pkg_cnt
    from
    (
        select device,pkg
        from $dws_device_install_app_re_status_di
        where refine_final_flag = 1
        and day > '$pday' and day <= '$day'
        group by device,pkg
    ) t1
    inner join cate_theme_table t2
    on t1.pkg = t2.apppkg
    group by device,$id
) pkg_cnt_table
on interval_table.device = pkg_cnt_table.device 
and interval_table.$id = pkg_cnt_table.$id;
"
}


for i in 30 60 90
do
    pday=$(date -d "$day -$i day" +%Y%m%d)

    for flag in cate theme
    do 
        flag1_level=$flag"1_level"
        flagid=$flag"1_id"

        if [ $flag = "cate" ] 
        then
            partition_flag=1
        else
            partition_flag=2
        fi

        active_label $flagid $flag1_level $partition_flag $flag
        
        retention_label $flagid $flag1_level $partition_flag $flag

    done
done