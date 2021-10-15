#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 计算设备的feature_category_index，模型计算需要这些特征
@projectName:MOBDI
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

source /home/dba/mobdi_center/conf/hive-env.sh

day=$1

tmpdb="dw_mobdi_tmp"
middb="dw_mobdi_md"
appdb="rp_mobdi_report"
##input
device_applist_new=${dim_device_applist_new_di}

income_1001_app_cate_index=$middb.income_1001_app_cate_index

##mapping
app_category_index_mapping="tp_mobdi_model.mapping_app_category_index_new"
app_category_mapping="dim_sdk_mapping.app_category_mapping_par"

#mapping_app_cate_index1="${mapdb}.mapping_age_cate_index1"
#mapping_app_cate_index2="${mapdb}.mapping_age_cate_index2"
##output
apppkg_category_index=${label_l1_apppkg_category_index}

#得到设备的app分类特征索引
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $apppkg_category_index partition (day = ${day}, version = '1003')
select a3.device, a1.index, 1.0 as cnt
from
(
  select cate_l2, index
  from $app_category_index_mapping
  where version = '1003'
) as a1
inner join
(
  select apppkg, cate_l2
  from $app_category_mapping
  where version = '1000.20190621'
  group by apppkg, cate_l2
) as a2 on a1.cate_l2 = a2.cate_l2
inner join
(
  select device, pkg
  from $device_applist_new
  where day = ${day}
) a3 on a2.apppkg = a3.pkg
group by device, index;
"

#得到设备的app分类特征索引（income_1001模型专用）
hive -v -e "
insert overwrite table $apppkg_category_index partition (day = '$day', version = '1003.income_1001')
select device,index,1.0 as cnt
from
(
  select c.device,d.index
  from
  (
    select a.device,b.cate_l1_id
    from
    (
      select device, pkg
      from $device_applist_new
      where day = '$day'
    ) a
    inner join
    (
      select apppkg,cate_l1_id
      from $app_category_mapping
      where version='1000.20191025'
      group by apppkg,cate_l1_id
    ) b on a.pkg=b.apppkg
  ) c
  inner join $income_1001_app_cate_index d on c.cate_l1_id=d.cate_l1_id
) e
group by device,index;
"

#得到设备的app catel1分类特征索引（occupation_1001模型专用）
occupation_1001_app_cate_l1_index="$middb.occupation_1001_app_cate_l1_index"
hive -v -e "
insert overwrite table $apppkg_category_index partition (day = '$day', version = '1003.occupation_1001.cate_l1')
select device,index,1.0 as cnt
from
(
  select c.device,d.index
  from
  (
    select a.device,b.cate_l1_id
    from
    (
      select device, pkg
      from $device_applist_new
      where day = '$day'
    ) a
    inner join
    (
      select apppkg,cate_l1_id
      from $app_category_mapping
      where version='1000.20200306'
      group by apppkg,cate_l1_id
    ) b on a.pkg=b.apppkg
  ) c
  inner join
  $occupation_1001_app_cate_l1_index d on c.cate_l1_id=d.cate_l1_id
) e
group by device,index;
"

#得到设备的app catel2分类特征索引（occupation_1001模型专用）
occupation_1001_app_cate_l2_index="$middb.occupation_1001_app_cate_l2_index"
hive -v -e "
insert overwrite table $apppkg_category_index partition (day = '$day', version = '1003.occupation_1001.cate_l2')
select device,index,1.0 as cnt
from
(
  select c.device,d.index
  from
  (
    select a.device,b.cate_l2_id
    from
    (
      select device, pkg
      from $device_applist_new
      where day = '$day'
    ) a
    inner join
    (
      select apppkg,cate_l2_id
      from $app_category_mapping
      where version='1000.20200306'
      group by apppkg,cate_l2_id
    ) b on a.pkg=b.apppkg
  ) c
  inner join
  $occupation_1001_app_cate_l2_index d on c.cate_l2_id=d.cate_l2_id
) e
group by device,index;
"

#计算设备的头部apppkg分类特征索引（consume_level模型专用）
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $apppkg_category_index partition (day = '$day', version = '1003.consume_level')
select a3.device, a1.index, cast(count(1) as double) as cnt
from
(
  select cate_l2, index
  from $app_category_index_mapping
  where version = '1003'
) as a1
inner join
(
  select apppkg, cate_l2
  from $app_category_mapping
  where version = '1000.20190621'
  group by apppkg, cate_l2
) as a2 on a1.cate_l2 = a2.cate_l2
inner join
(
  select t1.device, t2.apppkg
  from
  (
    select device, pkg
    from $device_applist_new
    where day = '$day'
  ) t1
  inner join
  tp_mobdi_model.apppkg_index t2 on t1.pkg=t2.apppkg and t2.model='common' and t2.version='1003'
  group by t1.device, t2.apppkg
) a3 on a3.apppkg=a2.apppkg
group by a3.device, a1.index;
"

#得到设备的app catel1分类特征索引（gender模型专用）
gender_app_cate_l1_index="$middb.gender_app_cate_l1_index"
hive -v -e "
insert overwrite table $apppkg_category_index partition (day = '$day', version = '1003.gender.cate_l1')
select device,index,1.0 as cnt
from
(
  select c.device,d.index
  from
  (
    select a.device,b.cate_l1_id
    from
    (
      select device, pkg
      from $device_applist_new
      where day = '$day'
    ) a
    inner join
    (
      select apppkg,cate_l1_id
      from $app_category_mapping
      where version='1000.20200306'
      group by apppkg,cate_l1_id
    ) b on a.pkg=b.apppkg
  ) c
  inner join
  $gender_app_cate_l1_index d on c.cate_l1_id=d.cate_l1_id
) e
group by device,index;
"

#得到设备的app catel2分类特征索引（gender模型专用）
gender_app_cate_l2_index="$middb.gender_app_cate_l2_index"
hive -v -e "
insert overwrite table $apppkg_category_index partition (day = '$day', version = '1003.gender.cate_l2')
select device,index,1.0 as cnt
from
(
  select c.device,d.index
  from
  (
    select a.device,b.cate_l2_id
    from
    (
      select device, pkg
      from $device_applist_new
      where day = '$day'
    ) a
    inner join
    (
      select apppkg,cate_l2_id
      from $app_category_mapping
      where version='1000.20200306'
      group by apppkg,cate_l2_id
    ) b on a.pkg=b.apppkg
  ) c
  inner join
  $gender_app_cate_l2_index d on c.cate_l2_id=d.cate_l2_id
) e
group by device,index;
"


#得到设备的app catel1分类特征索引（age模型专用）
hive -v -e "
insert overwrite table $apppkg_category_index partition (day = '$day', version = '1003.age.cate_l1')
select device,index,1.0 as cnt
from
(
  select c.device,d.index
  from
  (
    select a.device,b.cate_l1_id
    from
    (
      select device, pkg
      from $device_applist_new
      where day = '$day'
    ) a
    inner join
    (
      select apppkg,cate_l1_id
      from $app_category_mapping
      where version='1000'
      group by apppkg,cate_l1_id
    ) b on a.pkg=b.apppkg
  ) c
  inner join
  $mapping_app_cate_index1 d on c.cate_l1_id=d.cate_l1_id
) e
group by device,index;
"

#得到设备的app catel2分类特征索引（age模型专用）

hive -v -e "
insert overwrite table $apppkg_category_index partition (day = '$day', version = '1003.age.cate_l2')
select device,index,1.0 as cnt
from
(
  select c.device,d.index
  from
  (
    select a.device,b.cate_l2_id
    from
    (
      select device, pkg
      from $device_applist_new
      where day = '$day'
    ) a
    inner join
    (
      select apppkg,cate_l2_id
      from $app_category_mapping
      where version='1000.20200306'
      group by apppkg,cate_l2_id
    ) b on a.pkg=b.apppkg
  ) c
  inner join
  $mapping_app_cate_index2 d on c.cate_l2_id=d.cate_l2_id
) e
group by device,index;
"