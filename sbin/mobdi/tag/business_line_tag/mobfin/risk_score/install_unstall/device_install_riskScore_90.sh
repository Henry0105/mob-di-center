#!/bin/bash
: '
@owner:luost
@describe:近90天安装评分标签
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
p90day=`date -d "$day -90 days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di

#mapping
#app_pkg_mapping_par=dim_sdk_mapping.app_pkg_mapping_par

#tmp
tmp_anticheat_device_install_90days_pre=dw_mobdi_tmp.tmp_anticheat_device_install_90days_pre

#output
#label_l1_anticheat_device_riskscore=dm_mobdi_report.label_l1_anticheat_device_riskscore

mappingPartition=`hive -S -e "show partitions $app_pkg_mapping_par" | sort |tail -n 1`

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

insert overwrite table $tmp_anticheat_device_install_90days_pre partition (day = '$day')
select device,count(1) as cnt_pkg,sum(cnt) as cnt_all
from
(
    select device,pkg,count(1) as cnt
    from 
    (
        select m.device,coalesce(n.apppkg, m.pkg) as pkg,m.day
        from 
        (
            select device,pkg,day,refine_final_flag
            from $dws_device_install_app_re_status_di a
            where day > '$p90day' 
            and day <= '$day'
            and pkg is not null 
            and trim(pkg) <> ''
            and refine_final_flag = 1
        ) as m 
        left join 
        (
            select pkg,apppkg
            from $app_pkg_mapping_par
            where $mappingPartition
            group by pkg, apppkg
        ) as n
        on m.pkg = n.pkg
        group by m.device,coalesce(n.apppkg,m.pkg),m.day
    )a
    group by device,pkg
)b
group by device;
"

#卸载频率评分
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

insert overwrite table $label_l1_anticheat_device_riskscore partition(day = '$day',timewindow = '90',flag='4')
select device,ln(cnt_all/cnt_pkg)/(ln(cnt_all/cnt_pkg)+1) as riskScore
from $tmp_anticheat_device_install_90days_pre
where day = '$day';
"

#卸载评分
q1=`hive -e "select percentile(cnt_pkg,0.25) from $tmp_anticheat_device_install_90days_pre where day = '$day';"`
q3=`hive -e "select percentile(cnt_pkg,0.75) from $tmp_anticheat_device_install_90days_pre where day = '$day';"`
maxValue=`hive -e "select percentile(cnt_pkg,0.99) from $tmp_anticheat_device_install_90days_pre where day = '$day';"`

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

insert overwrite table $label_l1_anticheat_device_riskscore partition(day = '$day',timewindow = '90',flag='3')
select device,
case 
  when cnt_pkg <= ($q3+1.5*($q3-$q1)) then 0
  when cnt_pkg > ($q3+1.5*($q3-$q1)) and cnt_pkg <= $maxValue then cnt_pkg*1/($maxValue-($q3+1.5*($q3-$q1))) + (1-$maxValue*1/($maxValue-($q3+1.5*($q3-$q1))))
  when cnt_pkg > $maxValue then 1
  end as riskScore
from $tmp_anticheat_device_install_90days_pre
where day = '$day';
"