#! /bin/sh
set -e -x
export LANG=en_US.UTF-8
: '
@owner: yanhw
@describe: 生成包和app的映射关系表
@projectName:MobDI
@BusinessName:mapping
@SourceTable:dw_mobdi_etl.log_device_install_app_incr_info,dw_mobdi_md.apppkg_name_temp,dw_mobdi_md.pkg_name_sort,dm_sdk_mapping.dim_app_pkg_mapping_par,dm_sdk_mapping.dim_app_name_info_orig,dw_mobdi_etl.log_device_install_app_all_info,dw_mobdi_etl.log_device_unstall_app_info,dm_mobdi_mapping.dim_apppkg_name_info_wf
@TargetTable:dw_mobdi_md.pkg_name_sort,dm_sdk_mapping.dim_app_name_info_orig,dw_mobdi_md.apppkg_name_temp,dm_mobdi_mapping.dim_apppkg_name_info_wf
@TableRelation:dw_mobdi_etl.log_device_unstall_app_info,dw_mobdi_etl.log_device_install_app_incr_info,dw_mobdi_etl.log_device_install_app_all_info->dw_mobdi_md.pkg_name_sort|dm_sdk_mapping.dim_app_name_info_orig,dw_mobdi_md.pkg_name_sort->dm_sdk_mapping.dim_app_name_info_orig|dm_sdk_mapping.dim_app_pkg_mapping_par,dw_mobdi_md.pkg_name_sort->dw_mobdi_md.apppkg_name_temp|dm_mobdi_mapping.dim_apppkg_name_info_wf,dw_mobdi_md.apppkg_name_temp->dm_mobdi_mapping.dim_apppkg_name_info_wf
'
if [ $# -lt 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <date>"
    exit 1
fi

date1=$1
date2=$(date -d "$date1 -7 day" +%Y%m%d)
date3=$(date -d "$date1 -1 day" +%Y%m%d)
#导入配置文件
#source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
: '
@part_1:
实现功能:未经过渠道清理的包名所对应的app_name数据增量更新到dim_app_name_info_orig表
实现逻辑:先从log_device_install_app_all_info,log_device_install_app_incr_info,log_device_unstall_app_info抽出上周数据,
			算出安装量后排序并插入tp_sdk_tmp.pkg_name_sort表,之后再增量添加到dim_app_name_info_orig表
输出结果:dm_sdk_mapping.dim_app_name_info_orig :
			pkg 包名,
			name APP名,
			cnt 安装量,
			update_day 更新时间
'
#input
#dwd_log_device_install_app_all_info_sec_di=dw_sdk_log.log_device_install_app_all_info
#dwd_log_device_install_app_incr_info_sec_di=dw_sdk_log.log_device_install_app_incr_info
#dwd_log_device_unstall_app_info_sec_di=dw_sdk_log.log_device_unstall_app_info

dwd_log_device_install_app_all_info_sec_di=dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di
dwd_log_device_install_app_incr_info_sec_di=dm_mobdi_master.dwd_log_device_install_app_incr_info_sec_di
dwd_log_device_unstall_app_info_sec_di=dm_mobdi_master.dwd_log_device_unstall_app_info_sec_di

#mapping
dim_app_pkg_mapping_par=dm_sdk_mapping.app_pkg_mapping_par

#md
pkg_name_sort=dm_mobdi_tmp.pkg_name_sort
apppkg_name_temp=dm_mobdi_tmp.apppkg_name_temp

#out
dim_app_name_info_orig=dim_mobdi_mapping.dim_app_name_info_orig
dim_apppkg_name_info_wf=dim_mobdi_mapping.dim_apppkg_name_info_wf



#pull out all the pkg between this period
HADOOP_USER_NAME=dba hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3g' -XX:+UseG1GC;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=10;
insert overwrite table $pkg_name_sort
select *,ROW_NUMBER() OVER(PARTITION BY pkg ORDER BY cnt desc) AS rank from
(select pkg,name,count(*) as cnt from
(select pkg,name,device from
(select pkg,name,muid as device from $dwd_log_device_install_app_all_info_sec_di
where day<=$date1 and day >= $date2
and pkg is not null and trim(pkg)<>''
and name is not null and trim(name)<>'' and name <> 'null' and name <> 'NULL'
and name not rlike '\\\\.com$' and name not rlike '\\\\.com\\\\.' and name not rlike '^com\\\\.' and name <> pkg
group by pkg,name,muid
union all
select pkg,name,muid as device from $dwd_log_device_install_app_incr_info_sec_di
where day<=$date1 and day >=$date2
and pkg is not null and trim(pkg)<>''
and name is not null and trim(name)<>'' and name <> 'null' and name <> 'NULL'
and name not rlike '\\\\.com$' and name not rlike '\\\\.com\\\\.' and name not rlike '^com\\\\.' and name <> pkg
group by pkg,name,muid
union all
select pkg,name,muid as device from $dwd_log_device_unstall_app_info_sec_di
where day<=$date1 and day >=$date2
and pkg is not null and trim(pkg)<>''
and name is not null and trim(name)<>'' and name <> 'null' and name <> 'NULL'
and name not rlike '\\\\.com$' and name not rlike '\\\\.com\\\\.' and name not rlike '^com\\\\.' and name <> pkg
group by pkg,name,muid ) a
group by pkg,name,device)aa
group by pkg,name ) x
;"

#incremental update
HADOOP_USER_NAME=dba hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
insert overwrite table $dim_app_name_info_orig
select pkg,name,cnt,update_day from
(select a.*,row_number() over (partition by pkg order by cnt desc) num from
(select pkg,name,cnt, $date1 as update_day from $pkg_name_sort where rank = 1 and pkg = regexp_extract(pkg,'([a-zA-Z0-9\.\_]+)',0)
union all
select pkg,name,cnt,update_day from $dim_app_name_info_orig where pkg = regexp_extract(pkg,'([a-zA-Z0-9\.\_]+)',0)
)a)b
where num = 1
;"

: '
@part_2:
实现功能:渠道清理后的包名所对应的app_name数据增量更新到dim_app_name_info_orig表
实现逻辑:先把tp_sdk_tmp.pkg_name_sort表和$dim_app_pkg_mapping_par表join起来,
			取app_pkg_mapping的apppkg代替pkg_name_sort的apppkg,然后插入到apppkg_name_temp表
			之后再增量添加到dim_apppkg_name_info_wf表
输出结果:$dim_apppkg_name_info_wf :
			pkg 包名,
			name APP名,
			cnt 安装量,
			update_day 更新时间
'

#incremental update by clear pkg
HADOOP_USER_NAME=dba hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
insert overwrite table $apppkg_name_temp
select apppkg, name as app_name,cnt from
(select *,ROW_NUMBER() OVER(PARTITION BY apppkg ORDER BY cnt desc) AS rank from
(select apppkg,name,sum(cnt) as cnt from
(select COALESCE(b.apppkg,a.pkg) as apppkg, a.name, a.cnt
from $pkg_name_sort a
left join (select * from $dim_app_pkg_mapping_par where version='1000') b
on a.pkg=b.pkg
where a.rank=1) x
group by apppkg,name) xx) xxx
where rank = 1 and apppkg is not null and length(apppkg) <> 0 and apppkg = regexp_extract(apppkg,'([a-zA-Z0-9\.\_]+)',0)
;"

HADOOP_USER_NAME=dba hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
insert overwrite table $dim_apppkg_name_info_wf partition (day='$date1')
select apppkg,app_name,cnt,update_day from
(select *,ROW_NUMBER() OVER(PARTITION BY apppkg ORDER BY cnt desc) as num
from
(select apppkg,app_name,cnt,update_day from $dim_apppkg_name_info_wf where day=$date3
union all
select apppkg,app_name,cnt,$date1 as update_day from $apppkg_name_temp) a )b
where num=1
;"

