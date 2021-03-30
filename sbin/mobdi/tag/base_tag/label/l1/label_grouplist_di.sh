#!/bin/sh

set -x -e
: '
@owner: guanyt
@describe: 新的设备活跃计算
@projectName: mobdi
@BusinessName: grouplist
@attention: 这里还没改，groulist存放的字段叫tag_list
'
# 无model使用

if [ $# -ne 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: <date>"
     exit 1
fi

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

day=$1

##input
device_applist_new=${dim_device_applist_new_di}
##mapping
  #一堆
##output
#label_grouplist_di="dw_mobdi_md.tmp_active_tag"
label_grouplist_di=${label_l1_grouplist_di}

##主逻辑
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET mapreduce.job.queuename=dba;
set hive.exec.parallel=true;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
ADD JAR hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function remove_null_str as 'com.youzu.mob.java.udf.RemoveNullStr';

insert overwrite table $label_grouplist_di partition(day='$day')
select device,remove_null_str(tag_list) as tag_list
from
(
  select device,
         concat_ws(',',
           case when 0_cnt>0 then 0 else '' end,
           case when 1_cnt>1 then 1 else '' end,
           case when 2_cnt>0 then 2 else '' end,
           case when 3_cnt>1 then 3 else '' end,
           case when 4_cnt>0 then 4 else '' end,
           case when 5_cnt>0 then 5 else '' end,
           case when 6_cnt>1 then 6 else '' end,
           case when 7_cnt>0 then 7 else '' end,
           case when 8_cnt>1 then 8 else '' end,
           case when 9_cnt>3 then 9 else '' end,
           case when 10_cnt>1 then 10 else '' end,
           case when 11_cnt>1 then 11 else '' end,
           case when 12_cnt>2 then 12 else '' end,
           case when 13_cnt>0 then 13 else '' end,
           case when 14_cnt>1 then 14 else '' end,
           case when 15_cnt>1 then 15 else '' end,
           case when 16_cnt>1 then 16 else '' end,
           case when 17_cnt>2 then 17 else '' end,
           case when 18_cnt>0 then 18 else '' end,
           case when 19_cnt>0 then 19 else '' end,
           case when 20_cnt>0 then 20 else '' end,
           case when 21_cnt>1 then 21 else '' end,
           case when 22_cnt>2 then 22 else '' end,
           case when 23_cnt>0 then 23 else '' end,
           case when 24_cnt>1 then 24 else '' end,
           case when 25_cnt>2 then 25 else '' end,
           case when 26_cnt>0 then 26 else '' end,
           case when 27_cnt>1 then 27 else '' end,
           case when 29_cnt>0 then 29 else '' end,
           case when 30_cnt>0 then 30 else '' end,
           case when 31_cnt>0 then 31 else '' end,
           case when 32_cnt>1 then 32 else '' end,
           case when 33_cnt>2 then 33 else '' end,
           case when 34_cnt>0 then 34 else '' end,
           case when 35_cnt>0 then 35 else '' end,
           case when 36_cnt>0 then 36 else '' end,
           case when 37_cnt>0 then 37 else '' end,
           case when 38_cnt>0 then 38 else '' end,
           case when 39_cnt>0 then 39 else '' end,
           case when 40_cnt>0 then 40 else '' end
         ) as tag_list
  from
  (
    select device,
           sum(case when cate='股票' then cnt else 0 end) as 0_cnt,
           sum(case when cate in('股票','综合理财','投资','理财工具') then cnt else 0 end) as 1_cnt,
           sum(case when cate='数字货币' then cnt else 0 end) as 2_cnt,
           sum(case when cate='支付' then cnt else 0 end) as 3_cnt,
           sum(case when cate='借贷' then cnt else 0 end) as 4_cnt,
           sum(case when cate='彩票' then cnt else 0 end) as 5_cnt,
           sum(case when cate='信用卡' then cnt else 0 end) as 6_cnt,
           sum(case when cate='保险' then cnt else 0 end) as 7_cnt,
           sum(case when cate in('二次元','动漫') then cnt else 0 end) as 8_cnt,
           sum(case when cate='游戏服务' then cnt else 0 end) as 9_cnt,
           sum(case when cate in('在线音乐','音乐播放器') then cnt else 0 end) as 10_cnt,
           sum(case when cate='网络K歌' then cnt else 0 end) as 11_cnt,
           sum(case when cate in('在线视频','聚合视频','电视直播') then cnt else 0 end) as 12_cnt,
           sum(case when cate='电影票' then cnt else 0 end) as 13_cnt,
           sum(case when cate in('娱乐直播','游戏直播') then cnt else 0 end) as 14_cnt,
           sum(case when cate in('短视频','短视频工具') then cnt else 0 end) as 15_cnt,
           sum(case when cate in('电子书','有声听书','画报杂志') then cnt else 0 end) as 16_cnt,
           sum(case when cate in('微博','论坛','社区交友') then cnt else 0 end) as 17_cnt,
           sum(case when cate in('婚恋交友','同性交友','情侣互动') then cnt else 0 end) as 18_cnt,
           sum(case when cate='运动健身' then cnt else 0 end) as 19_cnt,
           sum(case when cate='体育资讯' then cnt else 0 end) as 20_cnt,
           sum(case when cate='智能设备' then cnt else 0 end) as 21_cnt,
           sum(case when cate in('相机','美化编辑') then cnt else 0 end) as 22_cnt,
           sum(case when cate='知识类' then cnt else 0 end) as 23_cnt,
           sum(case when cate in('周边游','在线旅行','攻略') then cnt else 0 end) as 24_cnt,
           sum(case when cate in('车票服务','机票','用车服务') then cnt else 0 end) as 25_cnt,
           sum(case when cate='求职招聘' then cnt else 0 end) as 26_cnt,
           sum(case when cate='教育培训' then cnt else 0 end) as 27_cnt,
           sum(case when cate='驾照考试' then cnt else 0 end) as 29_cnt,
           sum(case when cate='外卖' then cnt else 0 end) as 30_cnt,
           sum(case when cate in('美食','美食菜谱') then cnt else 0 end) as 31_cnt,
           sum(case when cate in('跨境电商','海外折扣') then cnt else 0 end) as 32_cnt,
           sum(case when cate in('在线购物','品牌电商','购物分享','母婴电商') then cnt else 0 end) as 33_cnt,
           sum(case when cate='房产信息' then cnt else 0 end) as 34_cnt,
           sum(case when cate='家居装修' then cnt else 0 end) as 35_cnt,
           sum(case when cate='婚庆' then cnt else 0 end) as 36_cnt,
           sum(case when cate in('备孕','孕期') then cnt else 0 end) as 37_cnt,
           sum(case when cate in('亲子服务','母婴电商','儿童教育') then cnt else 0 end) as 38_cnt,
           sum(case when cate in('电子竞技') then cnt else 0 end) as 39_cnt,
           sum(case when cate in('充电桩','新能源车') then cnt else 0 end) as 40_cnt

    from
    (
      select device,cate,count(cate) as cnt
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
          select apppkg,cate
          from
          (
            select apppkg,cate_l1 as cate
            from dim_sdk_mapping.app_category_mapping_par
            where version='1000'
            and cate_l1 in ('游戏服务','智能设备','教育培训','亲子服务')

            union all

            select apppkg,cate_l2 as cate
            from dim_sdk_mapping.app_category_mapping_par
            where version='1000'

            union all

            select apppkg,cate
            from dim_sdk_mapping.app_category_add_mapping

            union all

            select apppkg,life_stage as cate
            from tp_sdk_model.mapping_life_stage_applist
            where life_stage in ('备孕','孕期')

            union all
            select apppkg,'电子竞技'  as cate
            from dim_sdk_mapping.app_category_esport
            where version='1000'

            union all
            select apppkg,type as cate
            from dim_sdk_mapping.app_category_energy
            where version='1000'

          )t group by apppkg,cate
        )b on a.pkg=b.apppkg
      )c
      group by device,cate
    )d
    group by device
  )e
)f
"
