#!/bin/sh

: '
@owner: wangyc
@describe: 当天设备活跃的applist
'

set -x -e
export LANG=en_US.UTF-8

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <indate>"
  exit 1
fi

: '
@parameters
@indate:输入日期参数
@startdate:只取clienttime是当日或前一日的数据
'
#入参
day=$1

startdate=`date -d "${day} -1 days" +%Y%m%d`
formatDate=`date -d "${day}" +%Y-%m-%d`
formatStartdate=`date -d "${startdate}" +%Y-%m-%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#input
#dws_device_active_di=dm_mobdi_topic.dws_device_active_di

#mapping
app_pkg_mapping_par=dm_sdk_mapping.app_pkg_mapping_par

#output
#dws_device_active_applist_di=dm_mobdi_topic.dws_device_active_applist_di
#device_active_applist_full=dm_mobdi_report.device_active_applist_full

HADOOP_USER_NAME=dba hive -v -e "
SET mapreduce.map.memory.mb=10240;
set mapreduce.map.java.opts='-Xmx10g';
set mapreduce.child.map.java.opts='-Xmx10g';
set mapreduce.reduce.memory.mb=10240;
set mapreduce.reduce.java.opts='-Xmx8192M';
set mapreduce.child.reduce.java.opts='-Xmx8192M';
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

ADD jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION clean_client_time as 'com.youzu.mob.java.udf.ClientTimeCleanUDF';

insert overwrite table $dws_device_active_applist_di partition (day = '${day}')
select a1.device,a1.plat,a1.pkg,coalesce(a2.apppkg,a1.pkg) as apppkg,a1.source,a1.client_time
from
(
    select device,
           plat,
           pkg,
           concat_ws(',',collect_set(source)) as source,
           max(clienttime) as client_time
    from
    (
        select device,
               plat,
               pkg,
               source,
               sort_clienttime[size(sort_clienttime)-1] as clienttime
        from
        (
            select device,
                   plat,
                   pkg,
                   source,
                   sort_array(split(clean_client_time('${formatDate}',clienttime_list),',')) as sort_clienttime
            from $dws_device_active_di
            where day = '${day}'
            and source in ('runtimes','logrun','pv','xm')
            and (clienttime_list like '%${formatDate}%' or clienttime_list like '%${formatStartdate}%')
        ) t
    ) a
    group by device,plat,pkg
    cluster by device
)a1
left join
(
    select pkg,apppkg
    from $app_pkg_mapping_par
    where version='1000'
) a2
on a1.pkg = a2.pkg;
"

HADOOP_USER_NAME=dba hive -e "
INSERT overwrite TABLE $device_active_applist_full PARTITION (day = ${day})
SELECT device,
       plat,
       pkg,
       apppkg,
       processtime
FROM
(
    SELECT device,
           plat,
           pkg,
           apppkg,
           processtime,
           row_number() OVER (PARTITION BY device,plat,pkg ORDER BY processtime DESC) AS rk
    FROM
    (
        SELECT device,
               plat,
               pkg,
               apppkg,
               processtime
        FROM $device_active_applist_full
        WHERE day =${startdate}

        UNION ALL

        SELECT device,
               plat,
               pkg,
               apppkg,
               day AS processtime
        FROM $dws_device_active_applist_di
        WHERE day =${day}
    ) a
) aa
WHERE rk = 1
cluster by device;
"


