#!/bin/sh

set -x -e
: '
@owner: qinff
@describe: 新的设备活跃计算
@projectName: mobdi
@BusinessName: grouplist
'
# 无model使用

if [ $# -ne 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: <date>"
     exit 1
fi

source /home/dba/mobdi_center/conf/hive-env.sh

day=$1

##input
device_applist_new=${dim_device_applist_new_di}

##mapping
#dim_app_grouplist2_mapping="dim_sdk_mapping.app_grouplist2_mapping"

##output
label_grouplist2_di=${label_l1_grouplist2_di}

hive -v -e "
create table if not exists $label_grouplist2_di(
  device string COMMENT 'device', 
  grouplist string COMMENT '标签列表'
)
PARTITIONED BY (day string COMMENT '日期')
stored as orc
"

##主逻辑
hive -v -e "
set hive.exec.parallel=true;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
ADD JAR hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function remove_null_str as 'com.youzu.mob.java.udf.RemoveNullStr';

insert overwrite table $label_grouplist2_di partition(day='$day')
select device,remove_null_str(tag_list) as grouplist
from
(
  select device,
         concat_ws(',',
           case when 1_cnt>0 then 1 else '' end,
           case when 2_cnt>1 then 2 else '' end,
           case when 3_cnt>2 then 3 else '' end,
           case when 4_cnt>0 then 4 else '' end,
           case when 5_cnt>2 then 5 else '' end,
           case when 6_cnt>1 then 6 else '' end,
           case when 7_cnt>0 then 7 else '' end,
           case when 8_cnt>1 then 8 else '' end,
           case when 9_cnt>3 then 9 else '' end,
           case when 10_cnt>1 then 10 else '' end,
           case when 11_cnt>0 then 11 else '' end,
           case when 12_cnt>0 then 12 else '' end,
           case when 13_cnt>0 then 13 else '' end,
           case when 14_cnt>0 then 14 else '' end,
           case when 15_cnt>1 then 15 else '' end,
           case when 16_cnt>0 then 16 else '' end,
           case when 17_cnt>2 then 17 else '' end,
           case when 18_cnt>0 then 18 else '' end
         ) as tag_list
  from
  (
    select device,
           sum(case when cate='奢侈品爱好者' then cnt else 0 end) as 1_cnt,
           sum(case when cate='潮牌一族' then cnt else 0 end) as 2_cnt,
           sum(case when cate='自拍达人' then cnt else 0 end) as 3_cnt,
           sum(case when cate='科技极客' then cnt else 0 end) as 4_cnt,
           sum(case when cate='音乐发烧友' then cnt else 0 end) as 5_cnt,
           sum(case when cate='体育发烧友' then cnt else 0 end) as 6_cnt,
		   sum(case when cate='演艺与艺术爱好者' then cnt else 0 end) as 7_cnt,
		   sum(case when cate='二次元宅' then cnt else 0 end) as 8_cnt,
		   sum(case when cate='游戏达人' then cnt else 0 end) as 9_cnt,
		   sum(case when cate='K歌达人' then cnt else 0 end) as 10_cnt,
		   sum(case when cate='米其林吃货' then cnt else 0 end) as 11_cnt,
		   sum(case when cate='脱口秀爱好者' then cnt else 0 end) as 12_cnt,		
		   sum(case when cate='子女年龄0~2岁' then cnt else 0 end) as 13_cnt,		
		   sum(case when cate='子女年龄3~6岁' then cnt else 0 end) as 14_cnt,		
		   sum(case when cate='子女年龄7~16岁' then cnt else 0 end) as 15_cnt,	
		   sum(case when cate='生活美学' then cnt else 0 end) as 16_cnt,
		   sum(case when cate='性价比' then cnt else 0 end) as 17_cnt,
		   sum(case when cate='海淘爱好者' then cnt else 0 end) as 18_cnt
    from
    (
      select device,cate,count(pkg) as cnt
      from
      (
        select a.device,a.pkg,b.cate
        from
        (
          select device,pkg
          from $device_applist_new
          where day=$day
        ) a
        inner join
        (
		   select apppkg,concat(cate_l1,cate_l2) as cate
           from $dim_app_grouplist2_mapping
		   group by apppkg,concat(cate_l1,cate_l2) 
        )b on a.pkg=b.apppkg
      ) c
     group by device,cate
    ) d
    group by device
  )e
)f
"
