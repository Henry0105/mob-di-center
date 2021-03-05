#! /bin/sh
set -x -e

if [ $# -lt 2 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>,<activeTable>'"
     exit 1
fi
: '
@owner:xdzhang
@describe:餐饮行业标签
@projectName:MobDI
@BusinessName:profile_offline
@SourceTable:dm_sdk_mapping.app_category_mapping_par,rp_mobdi_app.app_active_daily,rp_mobdi_app.app_active_monthly,dm_mobdi_master.catering_lbs_label_weekly,rp_mobdi_app.app_active_weekly,dm_mobdi_master.device_catering_dinein_detail
@TargetTable:dm_mobdi_master.catering_lbs_label_weekly,dm_mobdi_master.catering_lbs_label_monthly
@TableRelation:dm_sdk_mapping.app_category_mapping_par,dm_mobdi_master.device_catering_dinein_detail,rp_mobdi_app.app_active_weekly,rp_mobdi_app.app_active_daily->dm_mobdi_master.catering_lbs_label_weekly|dm_sdk_mapping.app_category_mapping_par,rp_mobdi_app.app_active_monthly,dm_mobdi_master.device_catering_dinein_detail,rp_mobdi_app.app_active_daily->dm_mobdi_master.catering_lbs_label_monthly
'

: '
@parameters
@day:传入日期参数，为脚本运行日期（重跑不同）
@activeTable：标识是周统计还是月统计，0--周，1--月
@repartition:spark执行时自定义的分区数
@此脚本周统计在每个周一运行，月统计在每月一号运行
'
day=$1
activeTable=$2
repartition=2000
cd `dirname $0`
: '
@part_2:
实现功能：根据device汇总每周或每月外卖，堂吃信息
实现逻辑：1,取出时间窗内device_catering_dinein_detail的数据，根据device对数据进行统计，将多列详情数据汇总成一列
          2，取出对应的周（月）活跃数据，与1步骤的数据进行full join
输出结果：dm_mobdi_master.catering_lbs_label_weekly,dm_mobdi_master.catering_lbs_label_monthly
   字段：device                            设备id
         catering_takeout_cnt              外卖app活跃天数
         catering_takeout_detail           外卖app详情
         catering_dinein_cnt               堂吃天数
         catering_dinein_brand_detail      堂吃品牌详情
         catering_dinein_tyle1_detail      堂吃一级分类
         catering_dinein_tyle2_detail      堂吃二级分类
         catering_dinein_taste_detail      口味详情
         catering_dinein_time_detail       堂食时间
'

if [ $activeTable -eq 0 ];then
newdate=$day
#取得统计日期（一般为入参的上周的今天）  
statdayWeek=`date -d "${newdate} -1 weeks" +%Y%m%d`  
  
#得到是当周的周几 (0为星期日，1为星期一,...6为星期六)  
whichdayWeek=$(date -d ${statdayWeek} +%w)

if [ ${whichdayWeek} == 3 ]; then  
		  #用(statday-whichday)+1，就是某周的第一天，这里是星期一
		    if [ ${whichdayWeek} == 0 ]; then  
					 startdayWeek=`date -d "${statdayWeek} -6 days" +%Y%m%d`  
					   else  
							 startdayWeek=`date -d "${statdayWeek} -$[${whichdayWeek} - 1] days" +%Y%m%d`  
							  fi
							  #周数，例：20170013（17年13周）
							   weekActive=`date -d ${newdate} +%Y00%U`
							   #某周的最后一天，星期日为最后一天  
							   enddayWeek=`date -d "${startdayWeek} +6 days" +%Y%m%d` 
								#执行周任务
else
	echo "Today is not Wednesday"
   exit 0
fi
	
bday=${startdayWeek}
day=${enddayWeek}
dinein_partition=${weekActive}
num=`hive -S -e "select par_time from  rp_mobdi_app.app_active_weekly  where par_time =${day} limit 10" | wc -l`
if [ $num -ne 10 ]; then  echo "partition is null "; exit 1 ; fi

dinein_sql="
insert overwrite table dm_mobdi_master.catering_lbs_label_weekly partition(dt=${dinein_partition})
select COALESCE(dinein.device,w.device) as device,
           w.catering_takeout_cnt,w.catering_takeout_detail,
           dinein.catering_dinein_cnt,dinein.catering_dinein_brand_detail,
                   dinein.catering_dinein_tyle1_detail,dinein.catering_dinein_tyle2_detail,
                   dinein.catering_dinein_taste_detail,dinein.catering_dinein_time_detail
 from 
(
        select d.device,d.catering_dinein_brand_detail,d.catering_dinein_taste_detail,d.catering_dinein_tyle1_detail,
        d.catering_dinein_tyle2_detail,d.catering_dinein_time_detail,cnt.days as catering_dinein_cnt from
        dinein_tmp d
        left join
        (select device ,count(1) days from (select device , day from device_catering_tmp group by device,day ) m group by device  ) cnt
        on cnt.device = d.device 
 
 ) dinein
 full join
  (
  select ctd.device,ctc.catering_takeout_cnt,ctd.catering_takeout_detail from
(select wd.device as device, 
         concat(concat_ws(',',collect_list(wd.name)),'=', concat_ws(',',collect_list(cast(wd.cnt as string)))) as catering_takeout_detail from 
        (
		 select week.device,m.name,week.days as cnt from 
           (SELECT apppkg,appname as name from dm_sdk_mapping.app_category_mapping_par where version='1000' and cate_l2='外卖') m
         join
         (
		   select c.device, c.apppkg,c.days
                 from rp_mobdi_app.app_active_weekly c where c.par_time =${day}
		 ) week
                 on m.apppkg = week.apppkg
        ) wd group by wd.device 
) ctd
left join 
(select device ,count(1)  as catering_takeout_cnt  from 
   (
     select device  from 
       (SELECT apppkg,appname as name from dm_sdk_mapping.app_category_mapping_par where version='1000' and cate_l2='外卖') m
     join
     (
	select c.device, c.apppkg,c.day
                 from rp_mobdi_app.app_active_daily c where c.day >= ${bday} and c.day <=${day}
     ) d on m.apppkg = d.apppkg 
group by d.device,d.day 
) mm group by device
) ctc
on ctd.device = ctc.device  
) w
 on w.device = dinein.device
"
~/jdk1.8.0_45/bin/java -cp /home/dba/lib/mysql-utils-1.0-jar-with-dependencies.jar  com.mob.TagUpdateTime -d dm_mobdi_master -t catering_lbs_label_weekly

elif [ $activeTable -eq 1 ];then
  DAY=`date -d $day  +%d`
  if [ $DAY -ne 6 ];then
   echo "Today is not the sixth day of the month"
	exit 0
  fi
  nowdate=`date -d $day +%Y%m01`
  partition=`date -d"$nowdate last month" +%Y%m`
  bday=`date -d"$nowdate last month" +%Y%m%d`
  day=`date -d"$nowdate last day" +%Y%m%d`
  dinein_partition=`date -d"$nowdate last month" +%Y%m00`
  
  num=`hive -S -e "select c.month from rp_mobdi_app.app_active_monthly c where c.month =${partition} limit 10" | wc -l`
  if [ $num -ne 10 ]; then  echo "partition is null"; exit 1 ; fi

  dinein_sql="
insert overwrite table dm_mobdi_master.catering_lbs_label_monthly partition(dt=${dinein_partition})
select COALESCE(dinein.device,w.device) as device,
           w.catering_takeout_cnt,w.catering_takeout_detail,
           dinein.catering_dinein_cnt,dinein.catering_dinein_brand_detail,
                   dinein.catering_dinein_tyle1_detail,dinein.catering_dinein_tyle2_detail,
                   dinein.catering_dinein_taste_detail,dinein.catering_dinein_time_detail
 from 
(
        select d.device,d.catering_dinein_brand_detail,d.catering_dinein_taste_detail,d.catering_dinein_tyle1_detail,
        d.catering_dinein_tyle2_detail,d.catering_dinein_time_detail,cnt.days as catering_dinein_cnt from
        dinein_tmp d
        left join
        (select device ,count(1) days from (select device , day from device_catering_tmp group by device,day ) m group by device  ) cnt
        on cnt.device = d.device 
 
 ) dinein
 full join
  (
  select ctd.device,ctc.catering_takeout_cnt,ctd.catering_takeout_detail from
(select wd.device as device, 
         concat(concat_ws(',',collect_list(wd.name)),'=', concat_ws(',',collect_list(cast(wd.cnt as string)))) as catering_takeout_detail from 
        (
		 select week.device,m.name,week.days as cnt from 
           (SELECT apppkg,appname as name from dm_sdk_mapping.app_category_mapping_par where version='1000' and cate_l2='外卖') m
         join
         (
		   select c.device, c.apppkg,c.days
                 from rp_mobdi_app.app_active_monthly c where c.month =${partition}
		 ) week
                 on m.apppkg = week.apppkg
        ) wd group by wd.device 
) ctd
left join 
(
 select device ,count(1)  as catering_takeout_cnt  from 
   (
     select device from 
        (SELECT apppkg,appname as name from dm_sdk_mapping.app_category_mapping_par where version='1000' and cate_l2='外卖') m
     join
     (
	select c.device, c.apppkg,c.day
                 from rp_mobdi_app.app_active_daily c where c.day >= ${bday} and c.day <=${day}
     ) d on m.apppkg = d.apppkg 
     group by d.device,d.day 
     ) mm group by device
    ) ctc
   on ctd.device = ctc.device 
  ) w
 on w.device = dinein.device
"
~/jdk1.8.0_45/bin/java -cp /home/dba/lib/mysql-utils-1.0-jar-with-dependencies.jar  com.mob.TagUpdateTime -d dm_mobdi_master -t catering_lbs_label_monthly
else
 echo "WARN!Please choose the type of processing(0:week,1:month)"
 exit 1
fi


dinein_tmp_sql="
select device,brand,taste,type1,type2,time,day from 
 (select device,brand,split(taste,'，') as taste2,type1,type2,time,day from dm_mobdi_master.device_catering_dinein_detail 
    where day >=${bday} and day <=${day}) t lateral view explode(t.taste2) adtable as taste 
"

/opt/cloudera/parcels/CDH/bin/spark-submit --master yarn-cluster \
--class com.youzu.mob.label.CateringLbsDaily \
--driver-memory 8G \
--executor-memory 12G \
--executor-cores 4 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=80 \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.dynamicAllocation.executorIdleTimeout=60s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=15s \
--conf spark.default.parallelism=1400 \
--conf spark.sql.shuffle.partitions=1000 \
--conf spark.storage.memoryFraction=0.4 \
--conf spark.shuffle.memoryFraction=0.4 \
--conf spark.yarn.executor.memoryOverhead=6144 \
--conf spark.yarn.driver.memoryOverhead=2048 \
--conf spark.akka.frameSize=100 \
--conf spark.network.timeout=300000 \
--conf spark.core.connection.ack.wait.timeout=300000 \
--conf spark.akka.timeout=300000 \
--conf spark.storage.blockManagerSlaveTimeoutMs=300000 \
--conf spark.shuffle.io.connectionTimeout=300000 \
--conf spark.rpc.askTimeout=300000 \
--conf spark.rpc.lookupTimeout=300000 \
--driver-java-options "-XX:MaxPermSize=1024m" \
/home/dba/lib/appAnnie-spark-1.0-prod-jar-with-dependencies.jar "$day" "${dinein_tmp_sql}" "${dinein_sql}" "${repartition}"
