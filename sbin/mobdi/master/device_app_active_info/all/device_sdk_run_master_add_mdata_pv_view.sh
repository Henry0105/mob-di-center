#!/bin/bash

set -v -e

: '
@describe: dm_mobdi_master.device_sdk_run_master表原先有两个数据源pv和log_run_new，现在为了日活统计需要增加一个数据源dw_sdk_log.mdata_nginx_pv，
           但是又在没有全面评估前，不想影响业务使用，所以给出临时方案，使用视图方式去编写代码，仅供mobdashboard计算日活时使用
第一次修改：20210126 新增dm_mobdi_master.dwd_app_runtimes_stats_sec_di源：jira :http://j.mob.com/browse/SDK2020-2398
'

: "
create or replace view dm_mobdi_topic.dws_device_sdk_run_master_add_mdata_pv_view
partitioned on (day)
as
select device,pkg,appver,appkey,plat,commonsdkver,sdks,day
from dm_mobdi_topic.dws_device_sdk_run_master_di

union all

select device,pkg,appver,appkey,plat,commonsdkver,sdks,day
from dm_mobdi_topic.dws_device_active_di
where source='mdata_pv'

union all

select deviceid as device,apppkg as pkg,appver,appkey,plat,commonsdkver,array(map()) as sdks,day
from dm_mobdi_master.dwd_app_runtimes_stats_sec_di;
"

#第二次修改
: '
@describe: 波纹项目中修改库表名，下面代码手动执行一次即可
'

:"
create or replace view dm_mobdi_topic.dws_device_sdk_run_master_add_mdata_pv_view
partitioned on (day)
as
select device,pkg,appver,appkey,plat,commonsdkver,sdks,day
from dm_mobdi_topic.dws_device_sdk_run_master_di

union all

select device,pkg,appver,appkey,plat,commonsdkver,sdks,day
from dm_mobdi_topic.dws_device_active_di
where source='mdata_pv'

union all

select deviceid as device,apppkg as pkg,appver,appkey,plat,commonsdkver,array(map()) as sdks,day
from dm_mobdi_master.dwd_app_runtimes_stats_sec_di;
"