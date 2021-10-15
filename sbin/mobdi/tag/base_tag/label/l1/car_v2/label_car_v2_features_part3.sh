#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: device的bssid_cnt
@projectName:MOBDI
@update:20210323,将脚本拆分
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb="$dw_mobdi_md"
appdb="$rp_mobdi_app"

#input
tmp_label_phone_year=${appdb}.label_phone_year_${day}
tmp_label_bssid_num=${appdb}.label_bssid_num_${day}
tmp_label_distance_avg=${appdb}.label_distance_avg_${day}
tmp_label_distance_night=${appdb}.label_distance_night_${day}
tmp_label_homeworkdist=${appdb}.label_homeworkdist_${day}
tmp_label_home_poiaround=${appdb}.label_home_poiaround_${day}
tmp_label_work_poiaround=${appdb}.label_work_poiaround_${day}

#output
output_table=${tmpdb}.tmp_score_part3

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table ${output_table} partition(day='${day}')
select device,
       if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
       if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from
(
  select device
        ,case when year>='1995' and year<='1999' then 0
            when year>='2000' and year<='2004' then 1
            when year>='2005' and year<='2009' then 2
            when year>='2010' and year<='2014' then 3
            when year>='2015' and year<='2019' then 4
            else 5 end index
      ,1.0 cnt
  from ${tmp_label_phone_year}

  union all
  select device
        ,case when cnt=1 then 6
              when cnt in (2,3) then 7
              when cnt in (4,5) then 8
              when cnt>5 then 9
              else 10 end index
        ,1.0 cnt
  from ${tmp_label_bssid_num}

  union all
  select device
        ,case when distance < 50 then 11
              when distance < 500 then 12
              when distance < 5000 then 13
              when distance >= 5000 then 14
              else 15 end index
        ,1.0 cnt
  from ${tmp_label_distance_avg}

  union all
  select device
        ,case when distance < 50 then 16
              when distance < 500 then 17
              when distance < 5000 then 18
              when distance >= 5000 then 19
              else 20 end index
        ,1.0 cnt
  from ${tmp_label_distance_night}

  union all
  select device
        ,case when home_work_dist=0 then 21
              when home_work_dist<=1000 then 22
              when home_work_dist<=10000 then 23
              when home_work_dist<=50000 then 24
              when home_work_dist>50000 then 25
              else 26 end index
        ,1.0 cnt
  from ${tmp_label_homeworkdist}

  union all
  select device,cast(26+cast(poi_type as double) as int) index,1.0 cnt
  from ${tmp_label_home_poiaround}

  union all
  select device,cast(46+cast(poi_type as double) as int) index,1.0 cnt
  from ${tmp_label_work_poiaround}
)a
group by device;
"

#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table} drop if exists partition($old_version)"
done
