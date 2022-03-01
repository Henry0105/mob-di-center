#!/bin/bash

set -e -x

:<<!
@parameters
@day:传入日期参数,为脚本运行日期
!

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

insert_day=$1

# input
log_device_info_jh=dw_sdk_log.log_device_info_jh
log_device_info=dw_sdk_log.log_device_info
blacklist=dm_sdk_mapping.blacklist

# output
dws_device_mac_di=ex_log.dws_device_mac_di



HADOOP_USER_NAME=dba hive -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_mac as 'com.youzu.mob.java.udf.GetMacByWlan0';
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapreduce.map.memory.mb=10240;
set mapreduce.map.java.opts='-Xmx8192m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx8192m';
set mapreduce.reduce.memory.mb=12288;
set mapreduce.reduce.java.opts='-Xmx10240m';
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;



insert overwrite table $dws_device_mac_di partition(day='$insert_day')
select
    device,
    concat_ws(',', collect_list(mac)) as mac,
    concat_ws(',', collect_list(mac_tm)) as mac_tm
from
(
    select
        device,
        CASE
          WHEN jh_blacklist_mac.value IS NOT NULL THEN null
          WHEN mac is not null and mac <> '' then mac
          ELSE null
        END as mac,
        CASE
          WHEN jh_blacklist_mac.value IS NOT NULL THEN null
          WHEN mac is not null and mac <> '' then cast(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as string)
          ELSE null
        END as mac_tm
    from
    (
      select
          log_info_jh.device as device,
          lower(mac) as mac,
          serdatetime
      from
      (

	select
          device,
          case
              when trim(lower(mac)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or mac is null then ''
              when regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', '') in ('000000000000', '020000000000') then ''
              when regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', '') rlike '^[0-9a-f]{12}$'
              then substring(regexp_replace(regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', ''), '(.{2})', '\$1:'), 1, 17)
              else ''
            end as mac,
          serdatetime
        from
        (
            select
                muid as device,
                case
                    when get_mac(macarray) is not null and get_mac(macarray) <> '02:00:00:00:00:00' then lower(get_mac(macarray))
                    else mac
                end as mac,
                serdatetime
            from $log_device_info_jh
            where dt ='$insert_day' and plat = 1 and muid is not null and length(trim(muid))=40
        ) mm

        union all

        select
            muid as device,
            case
              when trim(lower(mac)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or mac is null then ''
              when regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', '') in ('000000000000', '020000000000') then ''
              when regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', '') rlike '^[0-9a-f]{12}$'
              then substring(regexp_replace(regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', ''), '(.{2})', '\$1:'), 1, 17)
              else ''
            end as mac,
            serdatetime
        from $log_device_info
        where dt ='$insert_day' and plat = 1 and muid is not null and length(trim(muid))=40 and mac is not null and length(mac) > 0
      ) log_info_jh where mac is not null and length(mac) > 0
    ) tt
    left join
    (SELECT lower(value) as value FROM $blacklist where type='mac' and day='20180702' GROUP BY lower(value)) jh_blacklist_mac
    on tt.mac=jh_blacklist_mac.value
) a
where mac is not null and length(mac) > 0
group by device
"

# 分区清理，保留最近5个分区
for old_version in `hive -e "show partitions $dws_device_mac_di" | grep -v '_bak' | sort | head -n -5`
do
  echo "rm $old_version"
  hive -e "alter table $dws_device_mac_di drop if exists partition ($old_version)"
done