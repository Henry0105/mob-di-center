#!/bin/bash

set -e -x

: '
@owner:zhangjch
@describe:商场报点指标监控
@projectName:mob_dashboard
'

if [[ $# -lt 1 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi

day=$1
whichday=`date -d $day +%w`
#本周四
thuday=`date -d "$day -$[${whichday}-4] days" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive_db_tb_dashboard.properties

#input
#mall_polygon_click_dau_di=mob_dashboard.mall_polygon_click_dau_di

#out
#mall_polygon_device_active_statistics_7days=mob_dashboard.mall_polygon_device_active_statistics_7days


max_day=`hive -e"
set hive.cli.print.header=false;
select max(day) from (select day from $mall_polygon_click_dau_di group by day having count(*) > 0)a;
"`
max_p6day=`date -d "$max_day -6 days" +%Y%m%d`
max_p7day=`date -d "$max_day -7 days" +%Y%m%d`
max_p13day=`date -d "$max_day -13 days" +%Y%m%d`
max_p14day=`date -d "$max_day -14 days" +%Y%m%d`

final_sql="
set mapreduce.job.queuename=root.yarn_mobdashboard.mobdashboard;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $mall_polygon_device_active_statistics_7days partition(day = '$day')
select nvl(a.city_type,b.city_type) as city_type,
       nvl(a.province_cn,b.province_cn) as province_cn,
       nvl(a.city_cn,b.city_cn) as city_cn,
       nvl(a.area_cn,b.area_cn) as area_cn,
       nvl(a.mob_mall_name,b.mob_mall_name) as mob_mall_name,
       cast(nvl(a.dau_avg,0) as bigint) as dau_avg,
       cast(nvl(b.dau_avg,0) as bigint) as pre_dau_avg,
       cast(nvl(a.dau_avg,0)/nvl(b.dau_avg,1) as decimal(20,4)) as dau_avg_wow,
       cast(nvl(a.click_cnt_avg,0) as bigint) as click_cnt_avg,
       cast(nvl(b.click_cnt_avg,0) as bigint) as pre_click_cnt_avg,
       cast(nvl(a.click_cnt_avg,0)/nvl(b.click_cnt_avg,1) as decimal(20,4)) as click_cnt_avg_wow
from (
    select city_type,
           province_cn,
           city_cn,
           area_cn,
           mob_mall_name,
           sum(click_cnt)/7 as click_cnt_avg,
           sum(dau)/7 as dau_avg
    from $mall_polygon_click_dau_di where day > '$max_p7day' and day <= '$max_day'
    group by city_type,
             province_cn,
             city_cn,
             area_cn,
             mob_mall_name
) a
full join (
    select city_type,
           province_cn,
           city_cn,
           area_cn,
           mob_mall_name,
           sum(click_cnt)/7 as click_cnt_avg,
           sum(dau)/7 as dau_avg
    from $mall_polygon_click_dau_di where day > '$max_p14day' and day <= '$max_p7day'
    group by city_type,
             province_cn,
             city_cn,
             area_cn,
             mob_mall_name
) b
on a.mob_mall_name = b.mob_mall_name
;
"

path1="/home/dba/zhangjc/mobdashboard/"$max_p6day"-"$max_day"日均统计结果1.csv"
path2="/home/dba/zhangjc/mobdashboard/"$max_p6day"-"$max_day"日均统计结果.csv"

title_sql="select '围栏名称,商场日均活跃,环比上周,商场日均报点,环比上周' as col"
load_sql="
    SELECT concat(mob_mall_name
          ,','
          ,dau_avg
          ,','
          ,dau_avg_wow
          ,','
          ,click_cnt_avg
          ,','
          ,click_cnt_avg_wow) as col, 2 as flag
    from $mall_polygon_device_active_statistics_7days
    where day ='$day' ;
"
content_sql="
    select concat('<tr><td>'
                ,sum(dau_avg)
                ,'</td><td>'
                ,round(cast(sum(dau_avg)/nvl(sum(pre_dau_avg),1) as decimal(20,4)),4)
                ,'</td><td>'
                ,sum(click_cnt_avg)
                ,'</td><td>'
                ,round(cast(sum(click_cnt_avg)/nvl(sum(pre_click_cnt_avg),1) as decimal(20,4)),4)
                ,'</td></tr>')
    from $mall_polygon_device_active_statistics_7days
    where day='$day'
    ;
"

if [ $day == $thuday ]
then
    hive -v -e "$final_sql"
    hive -e "$title_sql" > $path1
    hive -e "$load_sql" >> $path1
    iconv -f UTF-8 -c  -t GBK $path1 > $path2
    resu=`hive -e "$content_sql"`

    subject="近场调频周监控——"$max_p6day"-"$max_day"日统计结果"
    content="<h2 align='center'>"$max_p6day"-"$max_day"七日总量与"$max_p13day"-"$max_p7day"七日总量环比</h2>
         <table border='1' align='center'><th>商场日均活跃</th><th>环比上七日</th><th>商场日均报点</th><th>环比上七日</th>
         "${resu}"
        </table>
        <h4>详细指标定义请咨询叶秋</h4>"

    mailSender="linrb@mob.com,yuxj@mob.com,yxsun@mob.com,zhengwr@mob.com,yehn@mob.com,zhangjch@mob.com"

    /opt/jdk1.8.0_45/bin/java -cp /home/dba/zhangjc/mailSender-1.4.0.jar com.mob.mail.MailSender  "$subject"  "$content"  "$mailSender" "$path2"

    rm -r $path1
    rm -r $path2
fi










































