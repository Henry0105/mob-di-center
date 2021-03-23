#!/bin/bash
: '
@owner:luost
@describe:安装、卸载评分标签预处理
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
p30day=`date -d "$day -30 days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#源表
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di

#mapping
#app_pkg_mapping_par=dim_sdk_mapping.app_pkg_mapping_par

#输出表
tmp_anticheat_device_unstall_install_pre=dw_mobdi_tmp.tmp_anticheat_device_unstall_install_pre

hive -v -e "
create table if not exists $tmp_anticheat_device_unstall_install_pre(
    device string comment '设备号',
    pkg string comment '安装包',
    install_unstall_day string comment '安装、卸载日期',
    refine_final_flag int comment '-1 卸载 0 在装 1新安装 2 可属于新安装安装也可以属于新卸载（2的app最终状态可能是在装，也可能是卸载）'
)
comment '安装、卸载评分标签预处理中间表'
partitioned by (day string comment '日期')
stored as orc;
"

mappingPartition=`hive -S -e "show partitions $app_pkg_mapping_par" | sort |tail -n 1`
hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.groupby.skewindata=true;
set hive.map.aggr=true;

insert overwrite table $tmp_anticheat_device_unstall_install_pre partition (day = '$day')
select m.device,coalesce(n.apppkg, m.pkg) as pkg,m.day,refine_final_flag
from 
(
    select device,pkg,day,refine_final_flag
    from $dws_device_install_app_re_status_di a
    where day > '$p30day' 
    and day <= '$day'
    and pkg is not null 
    and trim(pkg) <> ''
    and refine_final_flag in (-1,1)
) as m 
left join 
(
    select pkg,apppkg
    from $app_pkg_mapping_par
    where $mappingPartition
    group by pkg, apppkg
) as n
on m.pkg = n.pkg
group by m.device,coalesce(n.apppkg,m.pkg),m.day,refine_final_flag;
"