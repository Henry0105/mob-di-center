#!/bin/sh

: '
@owner:liuhy
@describe:生成设备语言信息表
@projectName:Mobeye_o2o
@BusinessName:device_language
@SourceTable:dw_mobdi_md.device_language_tmp,dm_sdk_mapping.map_country_sdk,dw_mobdi_etl.log_wifi_info,dm_sdk_mapping.lang_code_mapping,rp_mobdi_app.device_language
@TargetTable:dw_mobdi_md.device_language_tmp,rp_mobdi_app.device_language
@TableRelation:dw_mobdi_etl.log_wifi_info,dw_mobdi_etl.pv,dm_sdk_mapping.lang_code_mapping,dm_sdk_mapping.country_abbr_name_mapping->dw_mobdi_md.device_language_tmp|dw_mobdi_md.device_language_tmp,rp_mobdi_app.device_language->rp_mobdi_app.device_language
'

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <date>"
  exit 1
fi

: '
实现步骤: 1.dw_mobdi_etl.log_wifi_info加上dw_mobdi_etl.pv, 对设备、手机语言、系统平添和日期去重
          2.步骤1的数据与dm_sdk_mapping.lang_code_mapping关联, 得到设备语言名称, 再与dm_sdk_mapping.map_country_sdk关联,
            得到设备国家英文名和中文名, 结果存入临时表dw_mobdi_md.device_language_tmp中
          3.dw_mobdi_md.device_language_tmp加上rp_mobdi_app.device_language的最新分区数据及最新一年数据, 取最新数据存入rp_mobdi_app.device_language
'

#参数，结束日期，建议为源表最新分区
day="$1"

#参数，开始日期，建议为源表最新分区；当需要补数据时，为补数据开始日期
day_start="$1"


source /home/dba/mobdi_center/conf/hive-env.sh
#input
#dwd_log_wifi_info_sec_di=dm_mobdi_master.dwd_log_wifi_info_sec_di
#dwd_pv_sec_di=dm_mobdi_master.dwd_pv_sec_di

#mapping
#dim_language_code_mapping=dim_sdk_mapping.dim_language_code_mapping
#lang_code_mapping=dm_sdk_mapping.lang_code_mapping
#dim_map_country_sdk=dim_sdk_mapping.dim_map_country_sdk
#map_country_sdk=dm_sdk_mapping.map_country_sdk

#md
tmp_device_language_tmp=dw_mobdi_tmp.tmp_device_language_tmp

#out
#device_language=rp_mobdi_report.device_language

#生成增量数据，主要为获取该段时间内的语言数据，可一次性跑多天
HADOOP_USER_NAME=dba hive -e "

CREATE TABLE IF NOT EXISTS $tmp_device_language_tmp(
device string,
language_code string,
language_cn string,
language_name string,
country_cn string,
country_en string,
plat string
)
partitioned by (day string)
stored as orc;    

set mapreduce.job.queuename=root.yarn_data_compliance2;
SET hive.exec.dynamic.partition=true;  
SET hive.exec.dynamic.partition.mode=nonstrict; 
SET hive.exec.max.dynamic.partitions.pernode=10000;
SET hive.exec.max.dynamic.partitions=10000;

INSERT OVERWRITE TABLE $tmp_device_language_tmp PARTITION (day)
SELECT a.device,case when trim(lower(d.code)) is not null and length(trim(lower(d.code)))=0 then 'unknown'
                     when length(a.language)=0 then 'other' else a.language end AS language_code,
       coalesce(d.name,b.name,'') AS language_cn,
       coalesce(d.name_mapping,b.name_mapping,'') AS language_name,
       coalesce(c.ch_name,e.ch_name,'') AS country_cn,
       coalesce(c.en_name,e.en_name,'') AS country_en,
       plat,day
FROM
(
  SELECT device,lower(trim(regexp_replace(language,'-','_'))) as language,plat,day
  FROM
  (
    SELECT trim(lower(muid)) as device,language,plat,day
    FROM $dwd_log_wifi_info_sec_di
    WHERE day>=$day_start
    and day<=$day
    and language is not null
    and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
    and trim(muid)!='0000000000000000000000000000000000000000'

    UNION ALL
            
    SELECT trim(lower(muid)) AS device,language,plat,day
    FROM $dwd_pv_sec_di
    WHERE day>=$day_start
    and day<=$day
    and language is not null
    and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
    and trim(muid)!='0000000000000000000000000000000000000000'
  )s
  GROUP BY device,lower(trim(regexp_replace(language,'-','_'))),plat,day
) a
LEFT JOIN
$dim_language_code_mapping d ON a.language=trim(lower(d.code))
LEFT JOIN
$dim_language_code_mapping b ON split(a.language,'_')[0]=trim(lower(b.code))
LEFT JOIN
$dim_map_country_sdk c ON split(a.language,'_')[1]=trim(lower(c.zone))
LEFT JOIN
$dim_map_country_sdk e ON split(a.language,'_')[2]=trim(lower(e.zone));
"

#生成结果数据，保留近365天数据 
HADOOP_USER_NAME=dba hive -e"
set mapreduce.job.queuename=root.yarn_data_compliance2;
CREATE TABLE  IF NOT EXISTS $device_language(
device string,
language_code string,
language_cn string,
language_name string,
country_cn string,
country_en string,
plat string,
update_date string
)
partitioned by (day string)
stored as orc;    
 "

lastPartStr=`HADOOP_USER_NAME=dba hive -e  "show partitions ${device_language}" | sort | tail -n 1`


#如果不为空则加 AND；方便自动化运行
if [ -z "$lastPartStr" ]; then 
    lastPartStrA=$lastPartStr
fi

if [ -n "$lastPartStr" ]; then 
    lastPartStrA=" AND   $lastPartStr"
fi

HADOOP_USER_NAME=dba hive -e"
set mapreduce.job.queuename=root.yarn_data_compliance2;
INSERT OVERWRITE TABLE $device_language PARTITION (day=$day)
SELECT device,language_code,language_cn,language_name,country_cn,country_en, plat,update_date,update_date as processtime
FROM
(
  SELECT device,language_code,language_cn,language_name,country_cn,country_en, plat,update_date,
         row_number() OVER(PARTITION BY device ORDER BY update_date DESC) AS rn
  FROM
  (
    SELECT device,language_code,language_cn,language_name,country_cn,country_en, plat,day AS update_date
    FROM $tmp_device_language_tmp
    WHERE day>=$day_start
    AND day<=$day

    UNION ALL

    SELECT trim(lower(device)) as device,language_code,language_cn,language_name,country_cn,country_en, plat,update_date
    FROM $device_language
    WHERE 1=1 $lastPartStrA
    AND update_date>=regexp_replace(date_sub(CONCAT_WS('-',SUBSTRING($day,1,4),SUBSTRING($day,5,2),SUBSTRING($day,7,2)),365),'-','')
    and trim(lower(device)) rlike '^[a-f0-9]{40}$'
    and trim(device)!='0000000000000000000000000000000000000000'
  )s1
)s2
WHERE rn=1;
"
#~/jdk1.8.0_45/bin/java -cp /home/dba/mobdi_center/lib/mysql-utils-1.0-jar-with-dependencies.jar  com.mob.TagUpdateTime -d rp_mobdi_report -t device_language
