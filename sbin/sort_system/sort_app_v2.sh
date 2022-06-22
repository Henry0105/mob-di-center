#!/bin/bash

set -e -x

day=$1
p40day=`date -d "$day -40 days" +%Y%m%d`

# input
dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di
rp_device_profile_full_view=dm_mobdi_report.rp_device_profile_full_view
app_wdj_info=dw_appgo_crawl.app_wdj_info
app_yyb_info=dw_appgo_crawl.app_yyb_info
dim_apppkg_name_info_wf=dim_mobdi_mapping.dim_apppkg_name_info_wf

# mapping
app_pkg_mapping_par=dm_sdk_mapping.app_pkg_mapping_par
app_category_mapping_par=dm_sdk_mapping.app_category_mapping_par

# md
tmp_appinstall_cnt_40days_pre1=dm_mobdi_tmp.tmp_appinstall_cnt_40days_pre1
tmp_appinstall_cnt_40days_pre2=dm_mobdi_tmp.tmp_appinstall_cnt_40days_pre2
tmp_appinstall_cnt_40days=dm_mobdi_tmp.tmp_appinstall_cnt_40days
tmp_device_country=dm_mobdi_tmp.tmp_device_country
tmp_yyb_wdj_all=dm_mobdi_tmp.tmp_yyb_wdj_all
tmp_appinstall_cnt_40days_add_country=dm_mobdi_tmp.tmp_appinstall_cnt_40days_add_country
tmp_appinstall_cnt_40days_add_country_filter=dm_mobdi_tmp.tmp_appinstall_cnt_40days_add_country_filter
tmp_sorted_pkg_part1=dm_mobdi_tmp.tmp_sorted_pkg_part1
tmp_app_category_mapping_par_new=dm_mobdi_tmp.tmp_app_category_mapping_par_new
tmp_sorted_pkg_part2=dm_mobdi_tmp.tmp_sorted_pkg_part2

# output
sorted_pkg=dw_mobdi_md.sorted_pkg

last_par_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dim_mobdi_mapping', 'dim_apppkg_name_info_wf', 'day');
drop temporary function GET_LAST_PARTITION;
"
last_par=(`hive -e "$last_par_sql"`)

HADOOP_USER_NAME=dba hive -e "
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=5120;
set mapreduce.map.java.opts='-Xmx4096m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx4096m';
set mapreduce.reduce.memory.mb=14336;
set mapreduce.reduce.java.opts='-Xmx12288m' -XX:+UseG1GC;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

drop table if exists $tmp_appinstall_cnt_40days_pre1;
create table $tmp_appinstall_cnt_40days_pre1 as
select apppkg, install_cnt, row_number() over(order by install_cnt desc) as num
from
(
  select apppkg, count(*) as install_cnt
  from
  (
    select coalesce(i.apppkg, h.pkg) as apppkg, h.device
    from
    (
      select device, pkg
      from $dws_device_install_app_re_status_di
      where day >= '$p40day' and day <= '$day' and refine_final_flag <> -1
      group by device, pkg
    ) as h
    left join
    (
      select pkg, apppkg
      from $app_pkg_mapping_par
      where version = '1000'
      group by pkg, apppkg
    ) as i
    on h.pkg = i.pkg
    group by coalesce(i.apppkg, h.pkg), h.device
  ) as j
  group by apppkg
) as k;



drop table if exists $tmp_appinstall_cnt_40days_pre2;
create table $tmp_appinstall_cnt_40days_pre2 as
select pkg, apppkg, pkg_install_cnt
from
(
  select pkg, apppkg, pkg_install_cnt, row_number() over(partition by apppkg order by pkg_install_cnt desc) as num
  from
  (
    select pkg, apppkg, count(*) as pkg_install_cnt
    from
    (
      select h.pkg, coalesce(i.apppkg, h.pkg) as apppkg, h.device
      from
      (
        select device, pkg
        from $dws_device_install_app_re_status_di
        where day >= '$p40day' and day <= '$day' and refine_final_flag <> -1
        group by device, pkg
      ) as h
      left join
      (
        select pkg, apppkg
        from $app_pkg_mapping_par
        where version = '1000'
        group by pkg, apppkg
      ) as i
      on h.pkg = i.pkg
      group by h.pkg, coalesce(i.apppkg, h.pkg), h.device
    ) as j
    group by pkg, apppkg
  ) as k
) as l
where num = 1;


drop table if exists $tmp_appinstall_cnt_40days;
create table $tmp_appinstall_cnt_40days as
select b.pkg, a.apppkg, a.install_cnt, a.num
from
$tmp_appinstall_cnt_40days_pre1 as a
inner join
$tmp_appinstall_cnt_40days_pre2 as b
on a.apppkg = b.apppkg;



--逻辑不变
drop table if exists $tmp_device_country;
create table $tmp_device_country as
select device, country_cn, permanent_country_cn, nationality_cn, country_work, confidence_work, country_home, confidence_home,
coalesce(if(work_home_confidence >= 0.7, if(work_home_country = 'cn', '中国', ''), null), permanent_country_cn, if(work_home_confidence < 0.7, if(work_home_country = 'cn', '中国', ''), null), country_cn) as final_country, applist
from
(
  select
    device, country_cn, permanent_country_cn, nationality_cn, country_work, confidence_work, country_home, confidence_home,
    case
      when confidence_work > confidence_home then country_work
      else country_home
    end as work_home_country,
    case
      when confidence_work > confidence_home then confidence_work
      else confidence_home
    end as work_home_confidence,
    applist
  from
  (
    select device,
      if(country_cn in ('', 'unknown'), null, country_cn) as country_cn,
      if(permanent_country_cn in ('', 'unknown'), null, country_cn) as permanent_country_cn,
      if(nationality_cn in ('', 'unknown'), null, nationality_cn) as nationality_cn,
      substring(get_json_object(workplace, '$.province'), 1, 2) as country_work,
      get_json_object(workplace, '$.confidence') as confidence_work,
      substring(get_json_object(residence, '$.province'), 1, 2) as country_home,
      get_json_object(residence, '$.confidence') as confidence_home,
      applist
    from
    (
      select
          device, country_cn, permanent_country_cn, nationality_cn,
          case
            when workplace in ('', 'unknown') then null
            else concat('{\"', regexp_replace(regexp_replace(workplace, ':', '\":\"'), ',', '\",\"'), '\"}')
          end as workplace,
          case
            when residence in ('', 'unknown') then null
            else concat('{\"', regexp_replace(regexp_replace(residence, ':', '\":\"'), ',', '\",\"'), '\"}')
          end as residence, applist
      from $rp_device_profile_full_view
      where applist not in ('', 'unknown')
    ) as a
  ) as b
) as c ;



--逻辑不变
drop table if exists $tmp_yyb_wdj_all;
create table $tmp_yyb_wdj_all as
select coalesce(q.apppkg, p.pkg) as apppkg, p.pkg, p.url
from
(
  select pkg, url
  from
  (
    select pkg, url, row_number() over(partition by pkg order by day desc, url desc) as num
    from
    (
      select packagename as pkg, if(url <> '' and url is not null, url, nvl(downloadlink, '')) as url, day
      from $app_wdj_info
      where ((downloadlink <> '' and downloadlink is not null) or (url <> '' and url is not null)) and packagename <> '' and packagename is not null
      union all
      select packagename as pkg, downloadlink as url, day
      from $app_yyb_info
      where ((downloadlink <> '' and downloadlink is not null) or (url <> '' and url is not null)) and packagename <> '' and packagename is not null
    ) as m
  ) as n
  where num = 1
) as p
left join
(
  select pkg, apppkg
  from $app_pkg_mapping_par
  where version = '1000'
  group by pkg, apppkg
) as q
on p.pkg = q.pkg ;





drop table if exists $tmp_appinstall_cnt_40days_add_country;
create table $tmp_appinstall_cnt_40days_add_country as
select pkg, apppkg, install_cnt_40days, install_cnt_40days_rank, final_country, device_cnt_country, row_number() over(partition by apppkg order by device_cnt_country desc) as device_cnt_country_rank
from
(
  select a.pkg, a.apppkg, a.install_cnt as install_cnt_40days, a.num as install_cnt_40days_rank, b.final_country, b.device_cnt as device_cnt_country
  from $tmp_appinstall_cnt_40days as a
  inner join
  (
    select apppkg, final_country, count(*) as device_cnt
    from
    (
      select device, final_country, mytable.app_split as apppkg
      from $tmp_device_country
      lateral view explode(split(applist, ',')) mytable as app_split
    ) as m
    group by apppkg, final_country
  ) as b
  on a.apppkg = b.apppkg
) as c
order by apppkg, device_cnt_country desc;



--这里为什么不先把app分类表的apppkg用最新的渠道清洗表更新一遍再来做下面的操作，因为如果先做了apppkg的更新，那么可能就会导致某些需要改变中文名和分类的app不能被筛选出来
drop table if exists $tmp_appinstall_cnt_40days_add_country_filter;
create table $tmp_appinstall_cnt_40days_add_country_filter as
select a.*
from
(
  select *
  from $tmp_appinstall_cnt_40days_add_country
  where device_cnt_country_rank = 1 and install_cnt_40days_rank <= 4000
) as a
left join
(
  select apppkg
  from $app_category_mapping_par
  where version = '1000'
  group by apppkg
) as b
on a.apppkg = b.apppkg
where b.apppkg is null ;



drop table if exists $tmp_sorted_pkg_part1;
create table $tmp_sorted_pkg_part1 as
select a.pkg, a.apppkg, install_cnt_40days, install_cnt_40days_rank, c.app_name as appname, b.url
from
(
  select pkg, apppkg, install_cnt_40days, install_cnt_40days_rank
  from $tmp_appinstall_cnt_40days_add_country_filter
  where final_country = '中国'
) as a
left join $tmp_yyb_wdj_all as b
on a.apppkg = b.apppkg
left join
(
  select apppkg, app_name
  from $dim_apppkg_name_info_wf
  where day = '$last_par'
) as c
on a.apppkg = c.apppkg;



--app分类表中的apppkg应该在每次更新的时候更新一次，因为渠道清洗表每周更新，它更新之后，master_reserved_new每天的app清洗之后的数据就变成最新的了，
--这样在用master_reserved_new生成诸如catelist等标签时，使用的是新的apppkg，而app分类表中的apppkg如果不跟着改变，则一直是老的，有可能出现本来应该能匹配上分类的app，结果却匹配不上的情况
--理论上渠道清洗表每次更新都要更新下app分类表
drop table if exists $tmp_app_category_mapping_par_new;
create table $tmp_app_category_mapping_par_new as
select a.*, coalesce(b.apppkg, a.apppkg) as apppkg_new
from
(
  select *
  from $app_category_mapping_par
  where version = '1000'
) as a
left join
(
  select pkg, apppkg
  from $app_pkg_mapping_par
  where version = '1000'
  group by pkg, apppkg
) as b
on a.pkg = b.pkg;




drop table if exists $tmp_sorted_pkg_part2;
create table $tmp_sorted_pkg_part2 as
select pkg, apppkg_new, appname, appname_new
from
(
  select pkg, apppkg_new, appname, coalesce(d.app_name, c.appname) as appname_new
  from $tmp_app_category_mapping_par_new as c
  left join
  (
    select apppkg, app_name
    from $dim_apppkg_name_info_wf
    where day = '$last_par'
  ) as d
  on c.apppkg_new = d.apppkg
) as e
where appname <> appname_new;


drop table if exists $sorted_pkg;
create table $sorted_pkg as
select pkg, apppkg, install_cnt_40days, install_cnt_40days_rank, appname, url, type
from
(
  select pkg, apppkg, install_cnt_40days, install_cnt_40days_rank, appname, url, type, row_number() over(partition by apppkg order by flag asc) as num
  from
  (
    select pkg, apppkg, install_cnt_40days, install_cnt_40days_rank, appname, url, 1 as flag, '1' as type
    from $tmp_sorted_pkg_part1
    union all
    select pkg, apppkg_new as apppkg, 0 as install_cnt_40days, 0 as install_cnt_40days_rank, appname_new as appname, '' as url, 2 as flag, '2' as type
    from $tmp_sorted_pkg_part2
  ) as a
) as b
where num = 1;

"