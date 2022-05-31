#!/bin/bash
 day=$1
set -e -x
#day=20220106


HADOOP_USER_NAME=dba hive -e "
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx7200m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx7200m;
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table mobdi_muid_dashboard.muid_historyfull_applist_ieid_oiid_pid_full partition (day=${day},full_type='year')
select 
a.device,
case when applist !='unknown' and applist !=''  and applist is not null then '1' else '0' end as applist_flag,
case when b.ieid !='' and b.ieid is not null then '1' else '0' end as ieid_flag,
case when b.oiid !='' and b.oiid is not null then '1' else '0' end as oiid_flag,
case when b.pid !=''  and b.pid is not null then '1' else '0' end as pid_flag
from
(
select device,applist
from
dm_mobdi_report.device_profile_label_full_par
where version='${day}.1000'
)a
left join 
(
select device,ieid,oiid,pid
from
dim_mobdi_mapping.dim_id_mapping_android_sec_df
where version='${day}.1000'
)b
on a.device=b.device;

insert overwrite table mobdi_muid_dashboard.muid_historyfull_applist_ieid_oiid_pid_report partition(day=$day,full_type='year')
select name,cnt,round(cnt/full,4) as percent
from
(
select name,cnt,sum(case when name='fullcount' then cnt else 0 end) over() as full
from
(
select 
sum(if(applist_flag=0,1,0)) as applist_n,
sum(if(applist_flag=1,1,0)) as applist_y,
sum(if(oiid_flag=0 and applist_flag=0,1,0)) as oiid_n_applist_n,
sum(if(oiid_flag=0 and applist_flag=1,1,0)) as oiid_n_applist_y,
sum(if(oiid_flag=1 and applist_flag=0,1,0)) as oiid_y_applist_n,
sum(if(oiid_flag=1 and applist_flag=1,1,0)) as oiid_y_applist_y,
sum(if(ieid_flag=0 and oiid_flag=0 and applist_flag=0,1,0)) as ieid_n_oiid_n_applist_n,
sum(if(ieid_flag=0 and oiid_flag=0 and applist_flag=1,1,0)) as ieid_n_oiid_n_applist_y,
sum(if(ieid_flag=0 and oiid_flag=1 and applist_flag=0,1,0)) as ieid_n_oiid_y_applist_n,
sum(if(ieid_flag=0 and oiid_flag=1 and applist_flag=1,1,0)) as ieid_n_oiid_y_applist_y,
sum(if(ieid_flag=1 and oiid_flag=0 and applist_flag=0,1,0)) as ieid_y_oiid_n_applist_n,
sum(if(ieid_flag=1 and oiid_flag=0 and applist_flag=1,1,0)) as ieid_y_oiid_n_applist_y,
sum(if(ieid_flag=1 and oiid_flag=1 and applist_flag=0,1,0)) as ieid_y_oiid_y_applist_n,
sum(if(ieid_flag=1 and oiid_flag=1 and applist_flag=1,1,0)) as ieid_y_oiid_y_applist_y,
sum(if(pid_flag=0 and ieid_flag=0 and oiid_flag=0 and applist_flag=0,1,0)) as pid_n_ieid_n_oiid_n_applist_n,
sum(if(pid_flag=0 and ieid_flag=0 and oiid_flag=0 and applist_flag=1,1,0)) as pid_n_ieid_n_oiid_n_applist_y,
sum(if(pid_flag=0 and ieid_flag=0 and oiid_flag=1 and applist_flag=0,1,0)) as pid_n_ieid_n_oiid_y_applist_n,
sum(if(pid_flag=0 and ieid_flag=0 and oiid_flag=1 and applist_flag=1,1,0)) as pid_n_ieid_n_oiid_y_applist_y,
sum(if(pid_flag=0 and ieid_flag=1 and oiid_flag=0 and applist_flag=0,1,0)) as pid_n_ieid_y_oiid_n_applist_n,
sum(if(pid_flag=0 and ieid_flag=1 and oiid_flag=0 and applist_flag=1,1,0)) as pid_n_ieid_y_oiid_n_applist_y,
sum(if(pid_flag=0 and ieid_flag=1 and oiid_flag=1 and applist_flag=0,1,0)) as pid_n_ieid_y_oiid_y_applist_n,
sum(if(pid_flag=0 and ieid_flag=1 and oiid_flag=1 and applist_flag=1,1,0)) as pid_n_ieid_y_oiid_y_applist_y,
sum(if(pid_flag=1 and ieid_flag=0 and oiid_flag=0 and applist_flag=0,1,0)) as pid_y_ieid_n_oiid_n_applist_n,
sum(if(pid_flag=1 and ieid_flag=0 and oiid_flag=0 and applist_flag=1,1,0)) as pid_y_ieid_n_oiid_n_applist_y,
sum(if(pid_flag=1 and ieid_flag=0 and oiid_flag=1 and applist_flag=0,1,0)) as pid_y_ieid_n_oiid_y_applist_n,
sum(if(pid_flag=1 and ieid_flag=0 and oiid_flag=1 and applist_flag=1,1,0)) as pid_y_ieid_n_oiid_y_applist_y,
sum(if(pid_flag=1 and ieid_flag=1 and oiid_flag=0 and applist_flag=0,1,0)) as pid_y_ieid_y_oiid_n_applist_n,
sum(if(pid_flag=1 and ieid_flag=1 and oiid_flag=0 and applist_flag=1,1,0)) as pid_y_ieid_y_oiid_n_applist_y,
sum(if(pid_flag=1 and ieid_flag=1 and oiid_flag=1 and applist_flag=0,1,0)) as pid_y_ieid_y_oiid_y_applist_n,
sum(if(pid_flag=1 and ieid_flag=1 and oiid_flag=1 and applist_flag=1,1,0)) as pid_y_ieid_y_oiid_y_applist_y,
count(*) as fullcount
from
mobdi_muid_dashboard.muid_historyfull_applist_ieid_oiid_pid_full
where day=$day and full_type='year'
)a
lateral view explode(map(
'applist_n',applist_n,
'applist_y',applist_y,
'oiid_n_applist_n',oiid_n_applist_n,
'oiid_n_applist_y',oiid_n_applist_y,
'oiid_y_applist_n',oiid_y_applist_n,
'oiid_y_applist_y',oiid_y_applist_y,
'ieid_n_oiid_n_applist_n',ieid_n_oiid_n_applist_n,
'ieid_n_oiid_n_applist_y',ieid_n_oiid_n_applist_y,
'ieid_n_oiid_y_applist_n',ieid_n_oiid_y_applist_n,
'ieid_n_oiid_y_applist_y',ieid_n_oiid_y_applist_y,
'ieid_y_oiid_n_applist_n',ieid_y_oiid_n_applist_n,
'ieid_y_oiid_n_applist_y',ieid_y_oiid_n_applist_y,
'ieid_y_oiid_y_applist_n',ieid_y_oiid_y_applist_n,
'ieid_y_oiid_y_applist_y',ieid_y_oiid_y_applist_y,
'pid_n_ieid_n_oiid_n_applist_n',pid_n_ieid_n_oiid_n_applist_n,
'pid_n_ieid_n_oiid_n_applist_y',pid_n_ieid_n_oiid_n_applist_y,
'pid_n_ieid_n_oiid_y_applist_n',pid_n_ieid_n_oiid_y_applist_n,
'pid_n_ieid_n_oiid_y_applist_y',pid_n_ieid_n_oiid_y_applist_y,
'pid_n_ieid_y_oiid_n_applist_n',pid_n_ieid_y_oiid_n_applist_n,
'pid_n_ieid_y_oiid_n_applist_y',pid_n_ieid_y_oiid_n_applist_y,
'pid_n_ieid_y_oiid_y_applist_n',pid_n_ieid_y_oiid_y_applist_n,
'pid_n_ieid_y_oiid_y_applist_y',pid_n_ieid_y_oiid_y_applist_y,
'pid_y_ieid_n_oiid_n_applist_n',pid_y_ieid_n_oiid_n_applist_n,
'pid_y_ieid_n_oiid_n_applist_y',pid_y_ieid_n_oiid_n_applist_y,
'pid_y_ieid_n_oiid_y_applist_n',pid_y_ieid_n_oiid_y_applist_n,
'pid_y_ieid_n_oiid_y_applist_y',pid_y_ieid_n_oiid_y_applist_y,
'pid_y_ieid_y_oiid_n_applist_n',pid_y_ieid_y_oiid_n_applist_n,
'pid_y_ieid_y_oiid_n_applist_y',pid_y_ieid_y_oiid_n_applist_y,
'pid_y_ieid_y_oiid_y_applist_n',pid_y_ieid_y_oiid_y_applist_n,
'pid_y_ieid_y_oiid_y_applist_y',pid_y_ieid_y_oiid_y_applist_y,
'fullcount',fullcount
)) t as name, cnt
)m
"



