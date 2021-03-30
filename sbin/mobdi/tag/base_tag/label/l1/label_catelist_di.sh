#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 计算设备的catelist标签
@projectName:MOBDI
'

# 没有model在用?

#第1步:计算设备安装目录的标签，dm_mobdi_master.master_reserved_new和dim_sdk_mapping.app_category_mapping_par进行left join匹配
#catelist格式:cate_l2_id1,cate_l2_id2=cnt1,cnt2
#样例:7001_001,7002_001,7002_002,7006_001,7007_001,7007_002,7008_004,7009_001,7012_001,7012_007,7014_007,7014_010=1,4,1,1,1,1,2,2,1,1,1,1

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

day=$1

##input
device_applist_new=${dim_device_applist_new_di}
##mapping

##output
label_catelist_di=${label_l1_catelist_di}
#label_catelist_di="dw_mobdi_md.device_catelist_label_incr"

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
insert overwrite table $label_catelist_di partition(day='$day')
select device,concat(cate_id_list,'=',cnt_list) as catelist
from
(
  select device,
         concat_ws(',',collect_list(cate_l2_id)) as cate_id_list,concat_ws(',',collect_list( cast(cnt as string))) as cnt_list
  from
  (
    select device,cate_l2_id,count(1) cnt
    from
    (
      select driver.device as device,mapping.cate_l2_id as cate_l2_id
      from
      (
        select *
        from $device_applist_new
        where day='$day'
      ) driver
      left join
      (
        select apppkg,cate_l2_id
        from dim_sdk_mapping.app_category_mapping_par
        where version='1000'
        group by apppkg,cate_l2_id
      ) mapping on driver.pkg=mapping.apppkg
    ) un
    where cate_l2_id!=''
    group by device,cate_l2_id
  ) tmp
  group by device
)un;
"
