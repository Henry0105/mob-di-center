#!/bin/bash
: '
@owner:luost
@describe:游戏兴趣度标签（付费）
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
p30day=$(date -d "$day -30 day" +%Y%m%d)

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

#源表
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di

#mapping表
#game_services_category=dim_sdk_mapping.game_services_category
#game_plat_model_seed_g=dim_sdk_mapping.game_plat_model_seed_g

#目标表
#label_device_game_pay_label_mi=dm_mobdi_report.label_device_game_pay_label_mi

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

with install_table as (
    select device,pkg
    from $dws_device_install_app_re_status_di
    where refine_final_flag in (0,1)
    and day > '$p30day'
    and day <= '$day'
    group by device,pkg
)

insert overwrite table $label_device_game_pay_label_mi partition (day = '$day')
select device,pay_flag
from 
(
    select device,'1' as pay_flag
    from install_table
    inner join
    (
        select apppkg
        from $game_services_category
        where flag = '游戏交易'
    ) game
    on install_table.pkg = game.apppkg
    group by device

    union all

    select device,'2' as pay_flag
    from install_table
    inner join
    (
        select apppkg
        from $game_services_category
        where flag = '游戏折扣平台'
    ) game
    on install_table.pkg = game.apppkg
    group by device

    union all

    select device,'3' as pay_flag
    from install_table
    inner join
    (
        select id
        from $game_plat_model_seed_g 
        where id_type = 'device' 
        and is_pay = 1
    ) game
    on install_table.device = game.id
    group by device
) pay_table;
"


