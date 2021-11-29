#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi
source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
p7=$(date -d "$day -7 days" "+%Y%m%d")
insertday=${day}_muid

tmpdb=$dm_mobdi_tmp
gender_feature_v2_part9="${tmpdb}.gender_feature_v2_part9"

gender_feature_v2_part10="${tmpdb}.gender_feature_v2_part10"

hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
insert overwrite table $gender_feature_v2_part10 partition(day='$insertday')
select t1.device, 
case when app379>0.5 and app123<=0.5  then 1 else 0 end app_comb1,
case when app379>0.5 and app123>0.5  then 1 else 0 end app_comb2,
case when app258<=0.5 and app48>0.5  then 1 else 0 end app_comb3,
case when app258>0.5 and app211<=0.5  then 1 else 0 end app_comb4,
case when app52<=0.5 and app169>0.5  then 1 else 0 end app_comb5,
case when app52>0.5 and app327>0.5  then 1 else 0 end app_comb6,
case when app362>0.5 and app128<=0.5  then 1 else 0 end app_comb7,
case when app107>0.5 and app356>0.5  then 1 else 0 end app_comb8,
case when app312<=0.5 and app48>0.5  then 1 else 0 end app_comb9,
case when app37>0.5 and app410<=0.5  then 1 else 0 end app_comb10,
case when app37>0.5 and app410>0.5  then 1 else 0 end app_comb11,
case when app48>0.5 and app111<=0.5  then 1 else 0 end app_comb12,
case when app169>0.5 and app171<=0.5  then 1 else 0 end app_comb13,
case when app169>0.5 and app171>0.5  then 1 else 0 end app_comb14,
case when app107<=0.5 and app6>0.5  then 1 else 0 end app_comb15,
case when app27>0.5 and app105>0.5  then 1 else 0 end app_comb16,
case when app105<=0.5 and app169>0.5  then 1 else 0 end app_comb17,
case when app105>0.5 and app80<=0.5  then 1 else 0 end app_comb18,
case when app318<=0.5 and app169>0.5  then 1 else 0 end app_comb19,
case when app2<=0.5 and app105>0.5  then 1 else 0 end app_comb20,
case when app178>0.5 and app6>0.5  then 1 else 0 end app_comb21,
case when app48>0.5 and app367<=0.5  then 1 else 0 end app_comb22,
case when app169>0.5 and app83<=0.5  then 1 else 0 end app_comb23,
case when app169>0.5 and app83>0.5  then 1 else 0 end app_comb24,
case when app361>0.5 and app37>0.5  then 1 else 0 end app_comb25,
case when app48>0.5 and app334<=0.5  then 1 else 0 end app_comb26,
case when app48>0.5 and app334>0.5  then 1 else 0 end app_comb27,
case when app258<=0.5 and app169>0.5  then 1 else 0 end app_comb28,
case when app258>0.5 and app17>0.5  then 1 else 0 end app_comb29,
case when app51>0.5 and app169>0.5  then 1 else 0 end app_comb30,
case when app166<=0.5 and app48>0.5  then 1 else 0 end app_comb31,
case when app166>0.5 and app208>0.5  then 1 else 0 end app_comb32,
case when app169>0.5 and app100<=0.5  then 1 else 0 end app_comb33,
case when app169>0.5 and app362<=0.5  then 1 else 0 end app_comb34,
case when app169>0.5 and app362>0.5  then 1 else 0 end app_comb35,
case when app362>0.5 and app258<=0.5  then 1 else 0 end app_comb36,
case when app22<=0.5 and app105>0.5  then 1 else 0 end app_comb37,
case when app105>0.5 and app79<=0.5  then 1 else 0 end app_comb38,
case when app41<=0.5 and app169>0.5  then 1 else 0 end app_comb39,
case when app51>0.5 and app48>0.5  then 1 else 0 end app_comb40,
case when app169>0.5 and app361<=0.5  then 1 else 0 end app_comb41,
case when app169>0.5 and app361>0.5  then 1 else 0 end app_comb42,
case when app165<=0.5 and app362>0.5  then 1 else 0 end app_comb43,
case when app355>0.5 and app107>0.5  then 1 else 0 end app_comb44,
case when app110>0.5 and app105>0.5  then 1 else 0 end app_comb45,
case when app37>0.5 and app305<=0.5  then 1 else 0 end app_comb46,
case when app372>0.5 and app197>0.5  then 1 else 0 end app_comb47,
case when app51>0.5 and app79<=0.5  then 1 else 0 end app_comb48,
case when app169>0.5 and app37<=0.5  then 1 else 0 end app_comb49,
case when app169>0.5 and app37>0.5  then 1 else 0 end app_comb50,
case when app197>0.5 and app132>0.5  then 1 else 0 end app_comb51,
case when app354<=0.5 and app169>0.5  then 1 else 0 end app_comb52,
case when app354>0.5 and app25>0.5  then 1 else 0 end app_comb53,
case when app165<=0.5 and app48>0.5  then 1 else 0 end app_comb54,
case when app110>0.5 and app365>0.5  then 1 else 0 end app_comb55,
case when app362>0.5 and app79<=0.5  then 1 else 0 end app_comb56,
case when app105>0.5 and app306<=0.5  then 1 else 0 end app_comb57,
case when app169>0.5 and app366<=0.5  then 1 else 0 end app_comb58,
case when app169>0.5 and app366>0.5  then 1 else 0 end app_comb59,
case when app366>0.5 and app66>0.5  then 1 else 0 end app_comb60,
case when app44>0.5 and app37>0.5  then 1 else 0 end app_comb61,
case when app382>0.5 and app305>0.5  then 1 else 0 end app_comb62,
case when app123<=0.5 and app169>0.5  then 1 else 0 end app_comb63,
case when app285>0.5 and app169>0.5  then 1 else 0 end app_comb64,
case when app351>0.5 and app161>0.5  then 1 else 0 end app_comb65,
case when app356>0.5 and app66>0.5  then 1 else 0 end app_comb66,
case when app160<=0.5 and app169>0.5  then 1 else 0 end app_comb67,
case when app160>0.5 and app116>0.5  then 1 else 0 end app_comb68,
case when app165>0.5 and app278>0.5  then 1 else 0 end app_comb69,
case when app169>0.5 and app166<=0.5  then 1 else 0 end app_comb70,
case when app169>0.5 and app166>0.5  then 1 else 0 end app_comb71,
case when app5>0.5 and app65>0.5  then 1 else 0 end app_comb72,
case when app51<=0.5 and app48>0.5  then 1 else 0 end app_comb73,
case when app51>0.5 and app105>0.5  then 1 else 0 end app_comb74,
case when app123>0.5 and app211>0.5  then 1 else 0 end app_comb75,
case when app100<=0.5 and app105>0.5  then 1 else 0 end app_comb76,
case when app100>0.5 and app215>0.5  then 1 else 0 end app_comb77,
case when app362>0.5 and app41<=0.5  then 1 else 0 end app_comb78,
case when app293>0.5 and app211>0.5  then 1 else 0 end app_comb79,
case when app169>0.5 and app6<=0.5  then 1 else 0 end app_comb80,
case when app169>0.5 and app6>0.5  then 1 else 0 end app_comb81,
case when app366>0.5 and app66>0.5  then 1 else 0 end app_comb82,
case when app115>0.5 and app234>0.5  then 1 else 0 end app_comb83,
case when app51>0.5 and app106>0.5  then 1 else 0 end app_comb84,
case when app169>0.5 and app294<=0.5  then 1 else 0 end app_comb85,
case when app169>0.5 and app294>0.5  then 1 else 0 end app_comb86
from $gender_feature_v2_part9 t1 where day='$insertday';
"