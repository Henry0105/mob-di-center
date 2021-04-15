#!/bin/bash
: '
@owner:luost
@describe:近90天bssid连接量评分
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

#入参
day=$1
p90day=`date -d "$day -90 days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
#dwd_log_wifi_info_sec_di=dm_mobdi_master.dwd_log_wifi_info_sec_di

#md
tmp_anticheat_device_bssid_cnt_90days=dw_mobdi_tmp.tmp_anticheat_device_bssid_cnt_90days

#out
#label_l1_anticheat_device_riskscore=dm_mobdi_report.label_l1_anticheat_device_riskscore

hive -v -e "
create table if not exists $tmp_anticheat_device_bssid_cnt_90days(
    device string comment '设备号',
    cnt bigint comment '设备连接bssid数'
)
comment '近90天设备连接bssid数中间表'
partitioned by (day string comment '日期')
stored as orc;
"

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=3221225472;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $tmp_anticheat_device_bssid_cnt_90days partition (day = '$day')
select device,count(1) as cnt
from 
(
	select device,bssid
	from 
	(
	    select muid as device,
	           regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') as bssid,
	           from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd') as real_date
	    from $dwd_log_wifi_info_sec_di
	    where day > '$p90day' 
	    and day <= '$day'
	    and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') rlike '^[0-9a-f]{12}$' 
	    and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') not in ('000000000000', '020000000000', 'ffffffffffff') 
	    and bssid is not null 
	) as log_wifi_info 
	where real_date > '$p90day'
	and real_date <= '$day'
	group by device,bssid
)a
group by device; 
"


q1=`hive -e "select percentile(cnt,0.25) from $tmp_anticheat_device_bssid_cnt_90days where day = '$day';"`
q3=`hive -e "select percentile(cnt,0.75) from $tmp_anticheat_device_bssid_cnt_90days where day = '$day';"`
maxValue=`hive -e "select percentile(cnt,0.99) from $tmp_anticheat_device_bssid_cnt_90days where day = '$day';"`

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=3221225472;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $label_l1_anticheat_device_riskscore partition(day = '$day',timewindow = '90',flag = '6')
select device,
case 
  when cnt <= ($q3+1.5*($q3-$q1)) then 0
  when cnt > ($q3+1.5*($q3-$q1)) and cnt <= $maxValue then cnt*1/($maxValue-($q3+1.5*($q3-$q1))) + (1-$maxValue*1/($maxValue-($q3+1.5*($q3-$q1))))
  when cnt > $maxValue then 1
  end as riskScore
from $tmp_anticheat_device_bssid_cnt_90days
where day = '$day';
"