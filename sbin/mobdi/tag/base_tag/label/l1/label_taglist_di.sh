#!/bin/bash

set -e -x

if [ $# -ne 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: <date> "
     exit 1
fi
: '
@owner:xdzhang
@describe: 计算设备标签的tfidf
@projectName:dailyrun
'
# 无model直接使用

source /home/dba/mobdi_center/conf/hive-env.sh

mddb=${dm_mobdi_tmp}
date=$1

##input
device_applist_new=${dim_device_applist_new_di}

#output
label_taglist_di=${label_l1_taglist_di}

tag_idf=${mddb}.tag_idf

hive -v -e "
set hive.optimize.index.filter=true;
set hive.exec.orc.zerocopy=true;
set hive.optimize.ppd=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=250000000;
set mapred.min.split.size.per.rack=250000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task=250000000;
set hive.exec.reducers.bytes.per.reducer=256000000;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function concat2 as 'com.youzu.mob.java.udaf.concatfortwofields';
with mobdi_tfidf_tmp AS
(
  select apppkg,tag,norm_tfidf
  from
  (
    select apppkg,cate,tag,norm_tfidf,
    row_number() over (partition by apppkg,tag order by norm_tfidf desc) rank
    from
    (
      select apppkg,cate,b.id tag,norm_tfidf
      from
      (
        select *
        from $dim_app_tag_system_mapping_par
        where version='1000'
      ) a
      inner join
      (
        select *
        from $dim_tag_id_mapping_par
        where version='1000'
      ) b on a.tag = b.tag
    ) t
    where norm_tfidf > 0
  ) a
  where rank = 1
),
device_tag_tf_inc as
(
  select device,tag,
         tfidf/sum(tfidf) over (partition by device) tf
  from
  (
    select device,tag,sum(norm_tfidf) tfidf
    from
    (
      select a.device,b.tag,b.norm_tfidf
      from $device_applist_new a
      inner join
      mobdi_tfidf_tmp b on a.pkg = b.apppkg
      where a.day = ${date}
      and a.processtime = ${date}
    ) c
    group by device, tag
  )d
)
insert overwrite table $label_taglist_di partition (day='$date')
select device,concat2(tag,tfidf,',','0.8') as tag_list
from
(
  select a.device,a.tag,a.tf*b.idf tfidf
  from device_tag_tf_inc a
  left join
  $tag_idf b on a.tag = b.tag
) a
group by device;
"
