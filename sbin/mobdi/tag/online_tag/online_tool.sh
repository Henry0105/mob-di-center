#!/bin/sh

set -x -e

day=$1
timewindow=$2
type=$3
computer_type=$4
pdays=`date -d "$day -$timewindow days" +%Y%m%d`

max_executor=200

source /home/dba/mobdi_center/conf/hive-env.sh

online_category_mapping_par_replace="dm_sdk_mapping.online_category_mapping_par_replace"
app_mapping_table="dm_sdk_mapping.app_pkg_mapping_par"
: '
inPutTable:
    dm_sdk_mapping.online_category_mapping_par_replace
    dm_sdk_mapping.app_pkg_mapping_par
    rp_mobdi_app.app_active_daily
    rp_mobdi_app.app_active_monthly
    dm_mobdi_topic.dws_device_install_app_re_status_di
    dm_mobdi_topic.dws_device_install_app_re_status_40d_di
outPutTable:
    rp_mobdi_app.timewindow_online_profile_v2
'

function data_exists(){
  start=$1
  end=$2
  table=$3
  time_flag=$4
  db=`echo ${table} |awk -F. '{print $1}'`
  tb=`echo ${table} |awk -F. '{print $2}'`
  while [ ${start} -ne ${end} ]
  do
    if [ "$time_flag"x = "month"x ];then
      start=`date -d "${start}01 1 months" +%Y%m`
    else
      start=`date -d "${start} 1 days" +%Y%m%d`
    fi
    count=`hadoop fs -ls /user/hive/warehouse/${db}.db/${tb}/${time_flag}=${start}|wc -l`
    if [ ${count} -eq 0 ];then
      echo "${time_flag}=${start} partition of table ${table} is not found !!"
      exit 1
    fi
  done
}

function gen_source_sql(){
  day=$1
  timewindow=$2
  pdays=$3
  type=$4
  data_exists ${pdays} ${day} ${app_active_daily} day
  # 例如20181118 计算90天数据可以分为以下几个段：
  # 20180818~20180901，20181101~20181118 使用 rp_mobdi_app.app_active_daily
  # 20180901~20181101 是两个整月的数据 使用 rp_mobdi_app.app_active_monthly 取201809和201810
  if [ ${timewindow} -eq 90 ];then
    # 计算3需要计算day
    if [[ ${type} -eq 3 || ${type} -eq 5 ]];then
      #当月第一天
      nowDateMonthFirstDate=`date -d "$day" +%Y%m01`
      varSql="select device,apppkg as relation,day from ${app_active_daily}
              where day<=${day} and day>=${nowDateMonthFirstDate} union all "
      #90天前
      before90Date=`date -d "$day -89 day" +%Y%m%d`
      #90天前的月初第一天
      before90MonthFirstDate=`date -d "$before90Date" +%Y%m01`
      #90天前的月末最后一天
      before90MonthLastDate=`date -d "$before90MonthFirstDate +1 month -1 day" +%Y%m%d`
      varSql=${varSql}"select device,apppkg as relation,day from ${app_active_daily}
                       where day<=${before90MonthLastDate} and day>=${before90Date} union all "
      varFirstDate=`date -d "${before90MonthFirstDate} +1 month" "+%Y%m%d"`
      while [ "${varFirstDate}" -ne "${nowDateMonthFirstDate}" ]
      do
        varMonth=`date -d "${varFirstDate}" "+%Y%m"`
        #去rp_mobdi_app.app_active_monthly查看月度文件是否存在,这里不能退出
        findHdfs=`hadoop fs -ls hdfs://ShareSdkHadoop/user/hive/warehouse/rp_mobdi_report.db/app_active_monthly/month=${varMonth} | wc -l`
        if [ $findHdfs -gt 0 ] ;then
          #存在
          varSql=${varSql}"select device,apppkg as relation,day from ${app_active_monthly}
                           LATERAL VIEW explode(split(day_list,',')) t AS day where month=${varMonth} union all "
        else
          #不存在，那只能把rp_mobdi_app.app_active_daily一个月数据弄出来
          varLastDate=`date -d "$varFirstDate +1 month -1 day" +%Y%m%d`
          varSql=${varSql}"select device,apppkg as relation,day from ${app_active_daily}
                           where day<=${varLastDate} and day>=${varFirstDate} union all "
        fi
        varFirstDate=`date -d "${varFirstDate} +1 month" "+%Y%m%d"`
      done
      source_sql=${varSql%union*}
    fi
  else
    source_sql="select device,apppkg as relation,day from ${app_active_daily} where day<=${day} and day>${pdays}"
  fi
  echo ${source_sql}
}



#判断refine_final_flag条件,以及根据timewindow，筛选需要的cate_id
if [ ${type} -eq 0 ]; then
  flagCondition="refine_final_flag = 1"

  if [ ${timewindow} -eq 14 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6) or cate_id like '7019%'"
  elif [ ${timewindow} -eq 90 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6)"
    max_executor=200
  else
    cate_id_regx="1=1"
  fi
elif [ ${type} -eq 1 ]; then
  flagCondition="refine_final_flag = -1"
  if [ ${timewindow} -eq 30 ];then
    cate_id_regx="cate_id like '7019%' or cate_id like 'G%'"
  else
    cate_id_regx="1=1"
  fi
elif [ ${type} -eq 2 ]; then
  flagCondition="1=1"

  if [ ${timewindow} -eq 14 ];then
    cate_id_regx="cate_id like '7019%'"
  else
    cate_id_regx="1=1"
  fi
elif [ ${type} -eq 3 ]; then
  flagCondition="1=1"

  if [ ${timewindow} -eq 14 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6)"
  elif [ ${timewindow} -eq 90 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6,598) or cate_id like 'fin%' or cate_id='7005010' or cate_id='my01'"
    max_executor=200
  else
    cate_id_regx="1=1"
  fi
elif [ ${type} -eq 4 ]; then
  flagCondition="refine_final_flag in (1,0,-1,2)"
  max_executor=200
  if [ ${timewindow} -eq 1 ];then
    cate_id_regx="cate_id =598 or cate_id like 'pay%'"
  elif [ ${timewindow} -eq 14 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6)"
  elif [ ${timewindow} -eq 90 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6) or  cate_id='7005010' or cate_id='my01' "
  else
    cate_id_regx="1=1"
  fi
elif [ ${type} -eq 5 ]; then
  flagCondition="1=1"
  if [ ${timewindow} -eq 14 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6)"
  elif [ ${timewindow} -eq 30 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6)"
  elif [ ${timewindow} -eq 90 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6)"
  else
    cate_id_regx="1=1"
  fi
elif [ ${type} -eq 6 ];then
  bday=`date -d "$day -1 days" "+%Y%m%d"`
  # 计算前需要先check数据是否存在
  count=`hadoop fs -ls /user/hive/warehouse/rp_mobdi_report.db/timewindow_online_profile_v2/flag=3/day=${day}/timewindow=7|wc -l`
  if [ ${count} -eq 0 ];then
    echo "day=${day} partition of table ${timewindow_online_profile_v2} is not found !!"
  fi
  # 计算前需要先check数据是否存在
  count=`hadoop fs -ls /user/hive/warehouse/rp_mobdi_report.db/timewindow_online_profile_v2/flag=3/day=${bday}/timewindow=7|wc -l`
  if [ ${count} -eq 0 ];then
    echo "day=${day} partition of table ${timewindow_online_profile_v2} is not found !!"
  fi

  mapping_version=`hive -e"
set mapreduce.job.queuename=root.yarn_data_compliance1;
select max(version)
                 from ${online_category_mapping_par_replace}
                 where type = ${type}"`
  HADOOP_USER_NAME=dba hive -e "
  SET hive.exec.compress.output=true;
  SET mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;
  set mapred.output.compression.type=BLOCK;
  set hive.exec.parallel=true;
  set hive.exec.parallel.thread.number=100;
  set hive.exec.compress.intermediate=true;
  set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
  set hive.intermediate.compression.type=BLOCK;
  set mapreduce.job.queuename=root.yarn_data_compliance1;

  insert overwrite table ${timewindow_online_profile_v2} partition(flag=${type},day=${day},timewindow=${timewindow})
  SELECT NVL(now_rp.device,bef_rp.device) as device,NVL(now_rp.feature,bef_rp.feature) as feature,
         round ( (NVL(now_rp.cnt,0) - NVL(bef_rp.cnt,0)+1)/(NVL(bef_rp.cnt,0)+1),4 ) as cnt
  from
  (
    select time_online.device,concat(time_online.cate_id,'_','6','_',time_online.time_window) as feature,time_online.cnt
    from
    (
      select device,split(feature,'_')[0] as cate_id,split(feature,'_')[2] as time_window,cnt
      from ${timewindow_online_profile_v2}
      where flag=3
      and day=${day}
      and timewindow=${timewindow}
    )time_online
    inner join
    (
      select cate_id
      from ${online_category_mapping_par_replace}
      where type=${type}
      and version = ${mapping_version}
      group by cate_id

      union all

      select total as cate_id
      from ${online_category_mapping_par_replace}
      where type=${type}
      and version = ${mapping_version}
      and total <>'0'
      group by total

      union all

      select percent as cate_id
      from ${online_category_mapping_par_replace}
      where type=${type}
      and version = ${mapping_version}
      and percent <>'0'
      group by percent
    )mapping on time_online.cate_id = mapping.cate_id
  ) now_rp
  full join
  (
    select time_online2.device,concat(time_online2.cate_id,'_','6','_',time_online2.time_window) as feature,time_online2.cnt
    from
    (
      select device,split(feature,'_')[0] as cate_id,split(feature,'_')[2] as time_window,cnt
      from ${timewindow_online_profile_v2}
      where flag=3
      and day=${bday}
      and timewindow=${timewindow}
    )time_online2
    inner join
    (
      select cate_id
      from ${online_category_mapping_par_replace}
      where type=${type}
      and version = ${mapping_version}
      group by cate_id

      union all

      select total as cate_id
      from ${online_category_mapping_par_replace}
      where type=${type}
      and version = ${mapping_version}
      and total <>'0'
      group by total

      union all

      select percent as cate_id
      from ${online_category_mapping_par_replace}
      where type=${type}
      and version = ${mapping_version}
      and percent <>'0'
      group by percent
    )mapping2 on time_online2.cate_id = mapping2.cate_id
  )bef_rp on bef_rp.device = now_rp.device and bef_rp.feature = now_rp.feature
  ;"
  exit 0
else
  flagCondition="1=1"

  cate_id_regx="1=1"
fi

# 0为计算app个数 and refine_final_flag in (${refine_final_flag})
if [ ${computer_type} -eq 0 ]; then
  if [ ${type} -eq 4 ];then
    data_exists ${pdays} ${day} ${dws_device_install_app_re_status_di} day
    data_exists `date -d "${pdays} -1 days" +%Y%m%d` ${pdays} ${dws_device_install_app_status_40d_di} day
    # 计算装过的app个数，需要计算timewindow时间段前40天的在装，使用dm_mobdi_master.device_install_app_master_new，保存的是40天的app在装
    source_sql="
      select device,pkg
      from ${dws_device_install_app_re_status_di}
      where day<='$day' and day>'$pdays'
      and ${flagCondition}

      union all

      select device,pkg
      from ${dws_device_install_app_status_40d_di}
      where day='$pdays' and final_flag in (0,1)
    "
  elif [ ${type} -eq 2 ];then
    # 如果计算app活跃个数 他属于个数 应该属于0 来计算，但是他计算活跃数据用表用的是rp_mobdi_app.app_active_daily
    source_sql=`gen_source_sql ${day} ${timewindow} ${pdays} ${type}`
  elif [ ${type} -eq 7 ];then
    source_sql="
    select device,pkg
      from ${dws_device_install_app_status_40d_di}
      where day='$day' and final_flag <> -1
    "
  else
    data_exists ${pdays} ${day} ${dws_device_install_app_re_status_di} day
    source_sql="
      select device,pkg
      from ${dws_device_install_app_re_status_di}
      where day<='$day' and day>'$pdays'
      and ${flagCondition}
    "
  fi
else
  # 1为计算活跃数据,源表数据存在daily,weekly和monthly的表，计算时可根据这些表进行优化
  source_sql=`gen_source_sql ${day} ${timewindow} ${pdays} ${type}`
fi

# 两个mapping表
mapping_version=`hive -e"
                 set mapreduce.job.queuename=root.yarn_data_compliance1;
                 select max(version)
                 from ${online_category_mapping_par_replace}
                 where type = '${type}'"`


category_mapping="select relation,cate_id,total,percent
  from ${online_category_mapping_par_replace}
  where version = '${mapping_version}'
  and type = '${type}'
  and (${cate_id_regx})"


apppkg_mapping="
   select apppkg,pkg
   from ${app_mapping_table}
   where version='1000'
"

echo ${source_sql}

HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit \
--master yarn \
--deploy-mode cluster \
--class com.mob.Online \
--driver-memory 12G \
--executor-memory 9G \
--executor-cores 4 \
--queue root.yarn_data_compliance1 \
--conf spark.network.timeout=300 \
--conf spark.sql.shuffle.partitions=3000 \
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=20 \
--conf spark.dynamicAllocation.maxExecutors=${max_executor} \
--conf spark.dynamicAllocation.initialExecutors=20 \
--conf spark.dynamicAllocation.executorIdleTimeout=15s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.driver.maxResultSize=4g \
--conf spark.sql.autoBroadcastJoinThreshold=104857600 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--driver-java-options "-XX:MaxPermSize=1024m" \
/home/dba/lib/online_tag/OnlineUniversalTool-v0.1.0-jar-with-dependencies.jar \
"
{
	\"category_mapping\":\"${category_mapping}\",
	\"apppkg_mapping\":\"${apppkg_mapping}\",
  \"source_sql\":\"${source_sql}\",
  \"day\":\"${day}\",
  \"type\":\"${type}\",
  \"timewindow\":\"${timewindow}\",
  \"computer_type\":\"${computer_type}\",
  \"output_table\":\"${timewindow_online_profile_v2}\"
}
"
# 合并小文件
HADOOP_USER_NAME=dba hive -e"
set mapreduce.job.queuename=root.yarn_data_compliance1;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table ${timewindow_online_profile_v2} partition(flag=$type,day=$day,timewindow=$timewindow)
select device,feature,
       case when cnt like '%E%' then cast(cast(cnt as decimal(38,37)) as string)
       else cnt end as cnt
from ${timewindow_online_profile_v2} where flag=$type and day=$day and timewindow=$timewindow
"
