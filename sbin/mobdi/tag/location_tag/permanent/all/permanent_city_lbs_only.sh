#!/bin/sh

: '

@author:liuhy | zhtli
@describe:商业地理 常驻省市计算
@createTime 2018-05-15
@projectName:MobDI
@BusinessName:location_permanent
@sourceTable:dm_mobdi_master.device_staying_daily,dm_mobdi_master.device_location_current_lbs_only,dw_mobdi_md.rp_device_location_permanent_lbs_only_tmp,rp_mobdi_app.rp_device_location_permanent_lbs_only
@targetTable:dm_mobdi_master.device_location_current_lbs_only,dw_mobdi_md.rp_device_location_permanent_lbs_only_tmp,rp_mobdi_app.rp_device_location_permanent_lbs_only
@TableRelation:dm_mobdi_master.device_staying_daily->dm_mobdi_master.device_location_current_lbs_only|dm_mobdi_master.device_location_current_lbs_only->dw_mobdi_md.rp_device_location_permanent_lbs_only_tmp|dw_mobdi_md.rp_device_location_permanent_lbs_only_tmp,rp_mobdi_app.rp_device_location_permanent_lbs_only->rp_mobdi_app.rp_device_location_permanent_lbs_only
'

set -x -e
cd `dirname $0`
#参数，结束日期，建议为源表最新分区
day=''
#参数，开始日期，建议为源表最新分区；当需要补数据时，为补数据开始日
day_start=''
lastDay=`hive -e "show partitions dm_mobdi_master.device_staying_daily" | sort | tail -n 1 | awk -F= '{print $2}'`
echo $lastDay
day=$lastDay
day_start=$lastDay
#if [ $# -lt 2 ]; then
#  else
#   day="$1"
#   day_start="$2"
#fi

#30日之前数据
day_b30=`date -d "$day -30 days" "+%Y%m%d"`
#180之前
day_b180=`date -d "$day -180 days" "+%Y%m%d"`

echo $day_b30
echo $day_b180

source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

:'
input:
dm_mobdi_topic.dws_device_location_staying_di

out:  --暂不修改，不明确表名
dm_mobdi_master.device_location_current_lbs_only
dw_mobdi_md.rp_device_location_permanent_lbs_only_tmp
rp_mobdi_app.rp_device_location_permanent_lbs_only

'


#create tables if not exists
hive -e"
CREATE TABLE IF NOT EXISTS dm_mobdi_master.device_location_current_lbs_only(
device string comment '设备ID',
country string comment '国家',
province string comment '省份',
city string comment '城市',
plat string comment '设备系统类型',
cnt  bigint comment '在装数量'
)
partitioned by (day string)
stored as orc;

CREATE TABLE IF NOT EXISTS dw_mobdi_md.rp_device_location_permanent_lbs_only_tmp(
device string,
country string,
province string,
city string,
plat string,
day_cnt_total bigint,
time_cnt_total  bigint,
day_cnt bigint,
time_cnt  bigint,
day_cnt_7 bigint,
time_cnt_7   bigint,
day_cnt_14 bigint,
time_cnt_14  bigint,
day_cnt_30 bigint,
time_cnt_30  bigint
)
partitioned by (day string)
stored as orc;

CREATE TABLE IF NOT EXISTS rp_mobdi_app.rp_device_location_permanent_lbs_only(
device string,
country string,
province string,
city string,
plat string,
day_cnt int,
time_cnt bigint,
update_date string
)
partitioned by (day string)
stored as orc; "


sql1="
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=10000;
SET hive.exec.max.dynamic.partitions=10000;
INSERT OVERWRITE TABLE dm_mobdi_master.device_location_current_lbs_only PARTITION (day)
SELECT device,country,province,city,plat,COUNT(1) AS cnt,day
FROM $dws_device_location_staying_di
WHERE day>=$day_start  AND day<=$day AND province!='' AND start_time!='00:00:00'
and lower(type) in ('gps','wifi','base','tlocation')
GROUP BY device,country,province,city,plat,day"
              
			  
#获取近30日lbs分布情况，分别计算近7,14,30 日情况

sql2="
INSERT OVERWRITE TABLE dw_mobdi_md.rp_device_location_permanent_lbs_only_tmp PARTITION (day=$day)
SELECT  s1.device,'cn' as country,province,city,plat
        ,day_cnt_total
        ,time_cnt_total
        ,day_cnt
        ,time_cnt
        ,day_cnt_7
        ,time_cnt_7
        ,day_cnt_14
        ,time_cnt_14
        ,day_cnt_30
        ,time_cnt_30
FROM(
        SELECT  device,COUNT(1) AS day_cnt_total,SUM(cnt) AS time_cnt_total
        FROM(
                SELECT  device,day,SUM(cnt) AS cnt
                FROM    dm_mobdi_master.device_location_current_lbs_only 
                WHERE   day>=$day_b30 AND day<=$day
                GROUP BY device,day
        )s 
        GROUP BY device 
)s1 
JOIN(
        SELECT  device,country,province,city,plat
                ,COUNT(1) AS day_cnt
                ,SUM(cnt) AS time_cnt  
                ,SUM(CASE WHEN datediff(par_day_sd,day_sd) <=7 THEN 1 ELSE 0 END) AS day_cnt_7
                ,SUM(CASE WHEN datediff(par_day_sd,day_sd) <=7 THEN cnt ELSE 0 END) AS time_cnt_7        
                ,SUM(CASE WHEN datediff(par_day_sd,day_sd) >7  AND datediff(par_day_sd,day_sd) <=14 THEN 1 ELSE 0 END) AS day_cnt_14
                ,SUM(CASE WHEN datediff(par_day_sd,day_sd) >7  AND datediff(par_day_sd,day_sd) <=14 THEN cnt ELSE 0 END) AS time_cnt_14        
                ,SUM(CASE WHEN datediff(par_day_sd,day_sd) >14  AND datediff(par_day_sd,day_sd) <=30 THEN 1 ELSE 0 END) AS day_cnt_30
                ,SUM(CASE WHEN datediff(par_day_sd,day_sd) >14  AND datediff(par_day_sd,day_sd) <=30 THEN cnt ELSE 0 END) AS time_cnt_30
        FROM(
                SELECT  device,country,province,city,plat,day,cnt
                        ,CONCAT_WS('-',SUBSTRING(day,1,4),SUBSTRING(day,5,2),SUBSTRING(day,7,2)) AS day_sd
                        ,CONCAT_WS('-',SUBSTRING($day,1,4),SUBSTRING($day,5,2),SUBSTRING($day,7,2)) AS par_day_sd  
                FROM    dm_mobdi_master.device_location_current_lbs_only 
                WHERE   day>=$day_b30 AND day<=$day

        )t
        GROUP BY device,country,province,city,plat
)t1 ON s1.device=t1.device"


#获取所有设备的常驻城市，保留近180天数据，过滤掉30天内出现天数小于4天的设备，近7日数据比例 1.2，14日比例1，30日0.8 

lastPartStr=`hive -e  "show partitions rp_mobdi_app.rp_device_location_permanent_lbs_only" | sort | tail -n 1`

sql3="
INSERT OVERWRITE TABLE rp_mobdi_app.rp_device_location_permanent_lbs_only PARTITION (day=$day)
SELECT  device,country,province,city,plat,day_cnt,time_cnt,update_date
FROM(
        SELECT  device,country,province,city,plat,day_cnt,time_cnt,update_date
                ,row_number() OVER(PARTITION BY device ORDER BY update_date DESC) AS rn 
        FROM(
                SELECT  device,country,province,city,plat
                        ,day_cnt
                        ,time_cnt
                        ,$day AS  update_date
                FROM(
                        SELECT  device,country,province,city,plat
                                ,day_cnt
                                ,time_cnt
                                ,row_number() over(partition by device order by 1.2*day_cnt_7+1.0*day_cnt_14+0.8*day_cnt_30 desc,day_cnt desc) as rn 
                        FROM    dw_mobdi_md.rp_device_location_permanent_lbs_only_tmp 
                        WHERE   day=$day AND day_cnt_total>=4 
                )s 
                WHERE   rn=1
                UNION ALL
                SELECT  device,country,province,city,plat,day_cnt,time_cnt,update_date
                FROM    rp_mobdi_app.rp_device_location_permanent_lbs_only
                WHERE   $lastPartStr and update_date>=$day_b180
        )s1 
)s2
WHERE rn=1"

#stpe 1
spark2-submit --master yarn \
--deploy-mode cluster \
--class com.youzu.mob.common.SqlSubmit \
--name "lbs_location_1_$day" \
--driver-memory 2G \
--executor-memory 12G \
--executor-cores 4 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=20 \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=10s \
--driver-java-options "-XX:MaxPermSize=1g" \
--conf spark.sql.shuffle.partitions=600 --conf spark.yarn.executor.memoryOverhead=4096 \
/home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$sql1"


# step 2

spark2-submit --master yarn \
--deploy-mode cluster \
--class com.youzu.mob.common.SqlSubmit \
--name "lbs_location_2_$day" \
--driver-memory 6G \
--executor-memory 12G \
--executor-cores 4 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=30 \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=10s \
--driver-java-options "-XX:MaxPermSize=1g" \
--conf spark.sql.shuffle.partitions=600 --conf spark.yarn.executor.memoryOverhead=4096 \
/home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$sql2"


#step 3
spark2-submit --master yarn \
--deploy-mode cluster \
--class com.youzu.mob.common.SqlSubmit \
--name "lbs_location_3_$day" \
--driver-memory 2G \
--executor-memory 12G \
--executor-cores 4 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=20 \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=10s \
--driver-java-options "-XX:MaxPermSize=1g" \
--conf spark.sql.shuffle.partitions=500 --conf spark.yarn.executor.memoryOverhead=4096 \
/home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$sql3"

echo over
