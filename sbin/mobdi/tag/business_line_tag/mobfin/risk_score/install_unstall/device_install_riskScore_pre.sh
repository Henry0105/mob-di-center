#!/bin/bash
: '
@owner:luost
@describe:安装评分标签预处理
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
p7days=`date -d "$day -7 days" +%Y%m%d`
p14days=`date -d "$day -14 days" +%Y%m%d`
p30days=`date -d "$day -30 days" +%Y%m%d`

#源表
tmp_anticheat_device_unstall_install_pre=dw_mobdi_tmp.tmp_anticheat_device_unstall_install_pre

#输出表
tmp_anticheat_device_install_7days_pre=dw_mobdi_tmp.tmp_anticheat_device_install_7days_pre
tmp_anticheat_device_install_14days_pre=dw_mobdi_tmp.tmp_anticheat_device_install_14days_pre
tmp_anticheat_device_install_30days_pre=dw_mobdi_tmp.tmp_anticheat_device_install_30days_pre

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $tmp_anticheat_device_install_7days_pre partition (day = '$day')
select device,cnt_pkg,cnt_all 
from 
(
    select device,count(1) over(partition by device) as cnt_pkg,sum(cnt) over(partition by device) as cnt_all
    from 
    (
        select device,pkg,count(1) as cnt
        from $tmp_anticheat_device_unstall_install_pre
        where day = '$day'
        and install_unstall_day > '$p7days' 
        and install_unstall_day <= '$day' 
        and refine_final_flag = 1
        group by device,pkg
    ) as b 
) as c 
group by device, cnt_pkg, cnt_all;
"

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $tmp_anticheat_device_install_14days_pre partition (day = '$day')
select device,cnt_pkg,cnt_all 
from 
(
    select device,count(1) over(partition by device) as cnt_pkg,sum(cnt) over(partition by device) as cnt_all
    from 
    (
        select device,pkg,count(1) as cnt
        from $tmp_anticheat_device_unstall_install_pre
        where day = '$day'
        and install_unstall_day > '$p14days' 
        and install_unstall_day <='$day' 
        and refine_final_flag = 1
        group by device,pkg
    ) as b 
) as c 
group by device, cnt_pkg, cnt_all;
"

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $tmp_anticheat_device_install_30days_pre partition (day = '$day')
select device,cnt_pkg,cnt_all 
from 
(
    select device,count(1) over(partition by device) as cnt_pkg,sum(cnt) over(partition by device) as cnt_all
    from 
    (
        select device,pkg,count(1) as cnt
        from $tmp_anticheat_device_unstall_install_pre
        where day = '$day'
        and install_unstall_day > '$p30days' 
        and install_unstall_day <= '$day' 
        and refine_final_flag = 1
        group by device,pkg
    ) as b 
) as c 
group by device, cnt_pkg, cnt_all;
"