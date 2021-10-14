#!/bin/bash

set -x -e
export LANG=en_US.UTF-8

: '
@owner: wangych
@describe: 生成渠道清理前后的包名及app_name的映射
@projectName:MobDI
@BusinessName:mapping
@SourceTable:$dim_apppkg_pkg_name,rp_mobeye_app360.rp_app_rank_category_insatll_daily,$cleanpkg_apppkg_temp,$pkg_name_sort,dm_sdk_mapping.dim_app_pkg_mapping_par,$cleanpkg_apppkg_name_temp,$cleanpkg_pkg_cnt_temp,$cleanpkg_result_temp
@TargetTable:$dim_apppkg_pkg_name,$cleanpkg_apppkg_temp,$cleanpkg_apppkg_name_temp,$cleanpkg_pkg_cnt_temp,$cleanpkg_result_temp
@TableRelation:rp_mobeye_app360.rp_app_rank_category_insatll_daily,dm_sdk_mapping.dim_app_pkg_mapping_par->$cleanpkg_apppkg_temp|$cleanpkg_apppkg_temp,dm_sdk_mapping.dim_app_pkg_mapping_par,$pkg_name_sort->$cleanpkg_pkg_cnt_temp|$cleanpkg_pkg_cnt_temp->$cleanpkg_apppkg_name_temp|$cleanpkg_apppkg_name_temp,->$cleanpkg_result_temp|$cleanpkg_result_temp->$dim_apppkg_pkg_name
'
date1=$1
#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#input
#rp_app_rank_category_insatll_daily=rp_mobeye_app360.rp_app_rank_category_insatll_daily
#mapping
#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par
#tmp
tmpdb="$dm_mobdi_tmp"
cleanpkg_pkg_cnt_temp=$tmpdb.cleanpkg_pkg_cnt_temp
cleanpkg_apppkg_temp=$tmpdb.cleanpkg_apppkg_temp
cleanpkg_apppkg_name_temp=$tmpdb.cleanpkg_apppkg_name_temp
cleanpkg_result_temp=$tmpdb.cleanpkg_result_temp
pkg_name_sort=$tmpdb.pkg_name_sort
#out
#dim_apppkg_pkg_name=dim_sdk_mapping.dim_apppkg_pkg_name

if [ $(date -d "$date1" +%w) -eq 2 ] ;then

sql="show partitions $rp_app_rank_category_insatll_daily;"
par=(`hive -e "$sql"`)
last_par=${par[(${#par[*]}-1)]#*=}
arr=(${last_par//// })
last_day=${arr[0]}

: '
@part_1:
实现功能：渠道清理前后的包名及app_name的映射
实现逻辑：获取清理前和清理后的pkg名与APP名做映射
输出结果：$dim_apppkg_pkg_name:
			apppkg 渠道清理后的pkg,
			pkg 渠道清理前的pkg,
			name 应用名
'

	hive -e "
	insert overwrite table $cleanpkg_apppkg_temp
	select coalesce(a2.apppkg,a1.apppkg) as pkg_new from
	(select apppkg from $rp_app_rank_category_insatll_daily
	where rank_date = ${last_day} and zone = 'cn' and rank_install <= 3000 group by apppkg) a1
	left join
	(select * from $dim_app_pkg_mapping_par where version='1000') a2
	on a1.apppkg = a2.pkg
	"

	hive -e "
	insert overwrite table $cleanpkg_pkg_cnt_temp
	select a1.apppkg,a1.pkg,a2.name,cast(a2.cnt as int) cnt from
	(select apppkg,pkg from
	(select apppkg,pkg from
	$cleanpkg_apppkg_temp t1
	join
	(select * from $dim_app_pkg_mapping_par where version='1000') t2
	on t1.pkg_new = t2.apppkg
	union all
	select pkg_new apppkg,pkg_new pkg from $cleanpkg_apppkg_temp)aa
	group by apppkg,pkg)a1
	join 
	(select pkg,name,cnt from $pkg_name_sort where rank = 1) a2
	on a1.pkg = a2.pkg
	"

	hive -e "
	insert overwrite table $cleanpkg_apppkg_name_temp 
	select a3.apppkg,a3.pkg,a4.name,a3.cnt from
	(select a1.* from
	$cleanpkg_pkg_cnt_temp a1
	join
	(select apppkg, sum(cnt) scnt from $cleanpkg_pkg_cnt_temp group by apppkg) a2
	on a1.apppkg = a2.apppkg
	where a1.cnt/a2.scnt > 0.001) a3
	join
	(select apppkg,name from
	(select apppkg,name,row_number() over (partition by apppkg order by cnt desc) rank from
	(select apppkg,name,sum(cnt) cnt
	from $cleanpkg_pkg_cnt_temp
	group by apppkg,name)a)aa
	where rank = 1) a4
	on a3.apppkg = a4.apppkg
	"

	hive -e "
	insert overwrite table $cleanpkg_result_temp 
	select a4.* from
	(select a1.apppkg from
	(select apppkg,name, sum(cnt) apppkgcnt from $cleanpkg_apppkg_name_temp group by apppkg,name)a1
	join
	(select name, sum(cnt) namecnt from $cleanpkg_apppkg_name_temp group by name)a2
	on a1.name = a2.name
	where a1.apppkgcnt/a2.namecnt >0.01) a3
	join
	$cleanpkg_apppkg_name_temp a4
	on a3.apppkg = a4.apppkg
	"

	hive -e "
	insert overwrite table $dim_apppkg_pkg_name
	select * from
	(select a3.apppkg,a3.pkg,a3.name from
	(select a1.* from
	$dim_apppkg_pkg_name a1
	left join
	(select apppkg from $cleanpkg_result_temp group by apppkg) a2
	on a1.apppkg = a2.apppkg
	where a2.apppkg is null) a3
	left join 
	$cleanpkg_result_temp a4
	on a3.pkg = a4.pkg
	where a4.pkg is null) a5
	union all
	select apppkg,pkg,name from $cleanpkg_result_temp
	"
fi
