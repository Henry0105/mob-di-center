#! /bin/sh
set -e -x
: '
@owner:xdzhang
@describe:游戏行业标签
@projectName:MobDI
@BusinessName:game_lable
@SourceTable:dm_sdk_mapping.game_app_detail_par,dw_mobdi_md.timewindow_game_finance

@TargetTable:dw_mobdi_md.game_app_mapping_tmp,rp_mobdi_app.timewindow_online_profile
@TableRelation:dm_sdk_mapping.game_app_detail_par->dw_mobdi_md.game_app_mapping_tmp|dw_mobdi_md.game_app_mapping_tmp->dw_mobdi_md.timewindow_game_finance|dw_mobdi_md.timewindow_game_finance->rp_mobdi_app.timewindow_online_profile
'

: '
@jar:CategoryTag-1.2-SNAPSHOT.jar,通用工具的jar
@links: http://c.mob.com/pages/viewpage.action?pageId=5659881

'
cd `dirname $0`

if [ $# -lt 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <date>"
    exit 1
fi

#入参
day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#input
#game_app_detail_par=dm_sdk_mapping.game_app_detail_par

tmpdb=$dm_mobdi_tmp
#tmp
game_app_mapping_tmp=$tmpdb.game_app_mapping_tmp
timewindow_game_finance=$tmpdb.timewindow_game_finance

#output
#timewindow_online_profile=dm_mobdi_report.timewindow_online_profile

hive -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';

insert overwrite table $game_app_mapping_tmp
select apppkg as pkg,cate_l2 as cat ,regexp_replace(cate_l2_id,'_','') as cat_id
from $dim_game_app_detail_par
where version ='1000'
group by apppkg,cate_l2_id,cate_l2

union all

select apppkg as pkg,network as cat ,network_id  as cat_id
from $dim_game_app_detail_par
where version ='1000'
group by apppkg,network_id,network

union all

select apppkg as pkg,ip_style as cat ,ip_style_id as cat_id
from $dim_game_app_detail_par
where version ='1000'
group by apppkg,ip_style_id,ip_style

union all

select apppkg as pkg,art_style as cat ,art_style_id as cat_id
from $dim_game_app_detail_par
where version ='1000'
group by apppkg,art_style_id,art_style

union all

select apppkg as pkg,frame as cat ,frame_id as cat_id
from $dim_game_app_detail_par
where version ='1000'
group by apppkg,frame_id,frame

union all

select apppkg as pkg,theme_l2 as cat ,theme_l2_id as cat_id
from $dim_game_app_detail_par
where version ='1000'
group by apppkg,theme_l2_id,theme_l2

union all

select apppkg as pkg,theme_l1 as cat ,theme_l1_id as cat_id
from $dim_game_app_detail_par
where version ='1000'
group by apppkg,theme_l1_id,theme_l1;
"

spark2-submit --master yarn \
--deploy-mode cluster \
--class com.mob.data.TimeWindowOnlineProfileApp  \
--name TimeWindowOnlineProfileApp_game_$day \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.locality.wait=100ms \
--conf spark.sql.codegen=true \
--conf spark.sql.broadcastTimeout=600 \
--conf spark.sql.shuffle.partitions=800 \
--conf spark.sql.autoBroadcastJoinThreshold=51457280 \
--conf spark.sql.orc.filterPushdown=true \
--conf spark.shuffle.io.maxRetries=6 \
--conf spark.shuffle.io.retryWait=10s \
--conf spark.network.timeout=300000 \
--conf spark.core.connection.ack.wait.timeout=300000 \
--conf spark.storage.blockManagerSlaveTimeoutMs=300000 \
--conf spark.shuffle.io.connectionTimeout=300000 \
--conf spark.rpc.askTimeout=3000 \
--conf spark.rpc.lookupTimeout=300000 \
--driver-java-options "-XX:MaxPermSize=1g" \
--conf spark.driver.maxResultSize=4g \
--verbose \
--executor-memory 12g \
--executor-cores 4 \
--driver-memory 4g \
/home/dba/mobdi_center/lib/CategoryTag-1.2-SNAPSHOT.jar $game_app_mapping_tmp 'pkg,cat,cat_id' '1' '0,2' '0,1,2,3,4' "$day" "$day" $timewindow_game_finance "false"

hive -e"
from $timewindow_game_finance
insert overwrite table $timewindow_online_profile partition(flag=17,day=${day},timewindow=7)
select device, feature, cnt where flag=0 and day='${day}' and timewindow=7

insert overwrite table $timewindow_online_profile partition(flag=18,day=${day},timewindow=7)
select device, feature, cnt where flag=1 and day='${day}' and timewindow=7

insert overwrite table $timewindow_online_profile partition(flag=19,day=${day},timewindow=7)
select device, feature, cnt where flag=3 and day='${day}' and timewindow=7

insert overwrite table $timewindow_online_profile partition(flag=20,day=${day},timewindow=7)
select device, feature, cnt where flag=2 and day='${day}' and timewindow=7

insert overwrite table $timewindow_online_profile partition(flag=21,day=${day},timewindow=7)
select device, feature, cnt where flag=4 and day='${day}' and timewindow=7

insert overwrite table $timewindow_online_profile partition(flag=17,day=${day},timewindow=30)
select device, feature, cnt where flag=0 and day='${day}' and timewindow=30

insert overwrite table $timewindow_online_profile partition(flag=18,day=${day},timewindow=30)
select device, feature, cnt where flag=1 and day='${day}' and timewindow=30

insert overwrite table $timewindow_online_profile partition(flag=19,day=${day},timewindow=30)
select device, feature, cnt where flag=3 and day='${day}' and timewindow=30

insert overwrite table $timewindow_online_profile partition(flag=20,day=${day},timewindow=30)
select device, feature, cnt where flag=2 and day='${day}' and timewindow=30

insert overwrite table $timewindow_online_profile partition(flag=21,day=${day},timewindow=30)
select device, feature, cnt where flag=4 and day='${day}' and timewindow=30;
"
