#!/bin/bash

set -x -e

: '
  金融线定制标签 joint_product1
'

insert_day=$1

sourceTable1="rp_finance_anticheat_muid.ronghui_joint_product1_result"
outputTable="dm_mobdi_report.timewindow_online_profile_joint_product1"


HADOOP_USER_NAME=dba hive -v -e "
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=500;
set mapred.map.tasks.speculative.execution=false;
set mapred.reduce.tasks.speculative.execution=false;
set hive.mapred.reduce.tasks.speculative.execution=false;
set hive.optimize.index.filter=true;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function map_agg as 'com.youzu.mob.java.map.MapAgg';



insert overwrite table $outputTable partition (day='${insert_day}')
select
device,map_agg(profile) as  profile
from
(
select trim(lower(device)) as device, map(
'9360_1000',cast(ac7a as string),
'9361_1000',cast(acin as string),
'9362_1000',cast(acdn as string),
'9363_1000',cast(acn7 as string),
'9364_1000',cast(acd7 as string),
'9365_1000',cast(acn14 as string),
'9366_1000',cast(acd14 as string),
'9367_1000',cast(acn30 as string),
'9368_1000',cast(acd30 as string),
'9369_1000',cast(acn90 as string),
'9370_1000',cast(acd90 as string),
'9371_1000',cast(acn180 as string),
'9372_1000',cast(acd180 as string),
'9373_1000',cast(fc7a as string),
'9374_1000',cast(fcin as string),
'9375_1000',cast(fcdn as string),
'9376_1000',cast(fcn7 as string),
'9377_1000',cast(fcd7 as string),
'9378_1000',cast(fcn14 as string),
'9379_1000',cast(fcd14 as string),
'9380_1000',cast(fcn30 as string),
'9381_1000',cast(fcd30 as string),
'9382_1000',cast(fcn90 as string),
'9383_1000',cast(fcd90 as string),
'9384_1000',cast(fcn180 as string),
'9385_1000',cast(fcd180 as string),
'9386_1000',cast(score1 as string),
'9387_1000',cast(score2 as string)
) as profile
from $sourceTable1
where day='$insert_day'
and trim(lower(device)) rlike '^[a-f0-9]{40}$' and trim(device)!='0000000000000000000000000000000000000000'
) union_source
group by device
cluster by device
;
"
