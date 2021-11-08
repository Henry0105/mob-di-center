#!/bin/bash
set -x -e

: '
@owner:liuyanqiang
@describe:计算设备app分类偏好度
@projectName:MOBDI
@BusinessName:profile

实现步骤: 1.使用explode_tags udf函数展开dw_mobdi_md.device_catelist_label_incr表中catelist数据，得到设备、设备app分类以及设备在某一app分类下的app数量
          2.对步骤1中的数据使用sum(设备分类数量) over(partition by 设备)方法，得到某个设备的app总数
          3.设备分类数量除以(步骤2中的数据+5)，得到自身偏好度算子preference_self
          4.算子preference_other是统计某一设备在某分类安装数超过了多少用户(%)，所以先要对步骤1中的数据去重统计设备总量，
            然后对步骤1的数据group by cate_id,cate_count算出每个分类数量下的设备数，使用sum(设备数) over(partition by 设备app分类 order by 分类数量 desc)
            算法算出某app分类安装个数没超过多少用户，设备总量-某app分类没超过多少用户=某app分类安装个数超过多少用户，再除以设备总量计算百分比，
            得到preference_other
          5.再计算((1+pow(0.33,2))*preference_other*preference_self)/(pow(0.33,2)*preference_other+preference_self)得到设备app分类偏好度，
            结果保留四位小数，结果存入dw_mobdi_md.device_cate_preference_incr
'

if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>'"
     exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp
##input
label_l1_catelist_di=$label_l1_catelist_di

##output
device_cate_preference_incr=${tmpdb}.device_cate_preference_incr

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function explode_tags as 'com.youzu.mob.java.udtf.ExplodeTags';

with device_cate_base_info as (
  --设备在某一app分类下的app数量
  select device,
         cate_id,
         cast(cate_count as int) as cate_count
  from $label_l1_catelist_di
  lateral view explode_tags(catelist) cate_list as cate_id,cate_count
  where day='${day}'
)

insert overwrite table $device_cate_preference_incr partition(day='${day}')
select device,
       cate_id,
       round(((1+pow(0.33,2))*preference_other*preference_self)/(pow(0.33,2)*preference_other+preference_self),4) as preference
from
(
  select device,
         device_cate_base_info.cate_id,
         --sum(cate_count) over(partition by device) 计算设备的app总数
         device_cate_base_info.cate_count/(sum(device_cate_base_info.cate_count) over(partition by device)+5) as preference_self,
         --设备总量-某app分类安装个数没超过多少用户=某app分类安装个数超过了多少用户
         (device_count-preference_other_cnt)/device_count as preference_other
  from device_cate_base_info
  inner join
  (
    --计算设备总量
    select count(1) as device_count
    from 
    (
      select device
      from device_cate_base_info
      group by device
    ) remove_duplicate
  ) device_count_info
  inner join
  (
    --计算某app分类安装个数没超过多少用户
    select cate_id,
           cate_count,
           sum(cnt) over(partition by cate_id order by cate_count desc) as preference_other_cnt
    from
    (
      select cate_id,
             cate_count,
             count(1) as cnt
      from device_cate_base_info
      group by cate_id,cate_count
    ) group_tmp
  ) preference_other_algorithm on device_cate_base_info.cate_id=preference_other_algorithm.cate_id
                                  and device_cate_base_info.cate_count=preference_other_algorithm.cate_count
) preference_base_calculate;
"
