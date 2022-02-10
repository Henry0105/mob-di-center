#!/bin/bash

set -e -x

: '
@owner:zhtli
@describe: 实现分拣系统中，将app标签分类从MySQL导入hive中去，并check数据，如果数据无效，则发邮件
@projectName:SortingSystem
'
source /home/dba/mobdi_center/conf/hive-env.sh

#dim_game_app_detail_par=dim_sdk_mapping.dim_game_app_detail_par
game_app_detail_db=${dim_game_app_detail_par%.*}
game_app_detail_tb=${dim_game_app_detail_par#*.}
#mapping
#dim_cate_id_mapping_par=dim_sdk_mapping.dim_cate_id_mapping_par

result=`mysql -h10.89.120.12 -u root -p'mobtech2019java' -P3310  -e "select max(time) from sorting_system.game_detail" -sN`
targetPar=1000.$result

parSql="
    add jar hdfs://ShareSdkHadoop/user/haom/udf/original-hive_udf-1.0.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('$game_app_detail_db','$game_app_detail_tb', 'version');
    drop temporary function GET_LAST_PARTITION;
"
lastPartition=$(hive -e "$parSql" -SN)

if [ "$lastPartition" \< "$targetPar" ];then
  echo "更新mapping表数据"
else
  exit 0
fi

mysqlInfoStr='{"userName":"root","pwd":"mobtech2019java","dbName":"sorting_system","host":"10.89.120.12","port":3310,"tableName":"game_detail"}'
mailList="DIMonitor@mob.com;yqzhou@mob.com"

#其中mysql_table_info_tmp表是spark临时表，用于存储mysql拉取过来的数据
#sql结果存入spark临时表test_info_tmp中
insertTestTableSql="
select c.pkg, c.apppkg, c.URL as url, c.description, c.appname, c.cate_l1, c.cate_l2, c.network, c.is_ip, c.frame,
       c.ip_name, c.ip_style, c.art_style, c.theme_l1, c.theme_l2, c.developer_long, c.developer_short, c.publisher_long, c.publisher_short, max(e.theme_l1_id) as theme_l1_id, max(f.theme_l2_id) as theme_l2_id,
       max(d.cate_l2_id) as cate_l2_id, max(g.art_style_id) as art_style_id, max(h.ip_style_id) as ip_style_id, max(i.network_id) as network_id, max(j.frame_id) as frame_id
from
(
  select a.pkg, a.apppkg, a.URL, a.description, a.appname, a.cate_l1, a.cate_l2, a.network, a.is_ip, a.frame,
         a.ip_name, a.ip_style, a.art_style, a.theme_l1, a.theme_l2, a.developer_long, a.developer_short, a.publisher_long, a.publisher_short, trim(b.cate_l1_id) as cate_l1_id
  from mysql_table_info_tmp a
  left join
  (
    select trim(cate_l1) as cate_l1,cate_l1_id
    from $dim_cate_id_mapping_par
    where version='1000'
    group by trim(cate_l1),cate_l1_id
  ) b on trim(a.cate_l1)=trim(b.cate_l1)
) c
join
(
  select a.pkg, a.apppkg, a.URL, a.description, a.appname, a.cate_l1, a.cate_l2, a.network, a.is_ip, a.frame,
          a.ip_name, a.ip_style, a.art_style, a.theme_l1, a.theme_l2, a.developer_long, a.developer_short, a.publisher_long, a.publisher_short, trim(b.cate_l2_id) as cate_l2_id
  from mysql_table_info_tmp a
  left join
  (
      select trim(cate_l2) as cate_l2,cate_l2_id
      from $dim_cate_id_mapping_par
      where version='1000'
      group by trim(cate_l2),cate_l2_id
  ) b on trim(a.cate_l2)=trim(b.cate_l2)
) d on c.pkg = d.pkg
join
(
  select a.pkg, a.apppkg, a.URL, a.description, a.appname, a.cate_l1, a.cate_l2, a.network, a.is_ip, a.frame,
          a.ip_name, a.ip_style, a.art_style, a.theme_l1, a.theme_l2, a.developer_long, a.developer_short, a.publisher_long, a.publisher_short, trim(b.name) as theme_l1_id
  from mysql_table_info_tmp a
  left join
  dm_sdk_mapping.game_theme_id_mapping b on trim(a.theme_l1)=trim(b.id)
) e on c.pkg = e.pkg
join
(
  select a.pkg, a.apppkg, a.URL, a.description, a.appname, a.cate_l1, a.cate_l2, a.network, a.is_ip, a.frame,
         a.ip_name, a.ip_style, a.art_style, a.theme_l1, a.theme_l2, a.developer_long, a.developer_short, a.publisher_long, a.publisher_short, trim(b.name) as theme_l2_id
  from mysql_table_info_tmp a
  left join
  dm_sdk_mapping.game_theme_id_mapping b on trim(a.theme_l2)=trim(b.id)
) f on c.pkg = f.pkg
join
(
  select a.pkg, a.apppkg, a.URL, a.description, a.appname, a.cate_l1, a.cate_l2, a.network, a.is_ip, a.frame,
         a.ip_name, a.ip_style, a.art_style, a.theme_l1, a.theme_l2, a.developer_long, a.developer_short, a.publisher_long, a.publisher_short, trim(b.name) as art_style_id
  from mysql_table_info_tmp a
  left join
  dm_sdk_mapping.game_style_id_mapping b on trim(a.art_style)=trim(b.id)
) g on c.pkg = g.pkg
join
(
  select a.pkg, a.apppkg, a.URL, a.description, a.appname, a.cate_l1, a.cate_l2, a.network, a.is_ip, a.frame,
         a.ip_name, a.ip_style, a.art_style, a.theme_l1, a.theme_l2, a.developer_long, a.developer_short, a.publisher_long, a.publisher_short, trim(b.name) as ip_style_id
  from mysql_table_info_tmp a
  left join
  dm_sdk_mapping.game_ip_id_mapping b on trim(a.ip_style)=trim(b.id)
) h on c.pkg = h.pkg
join
(
  select a.pkg, a.apppkg, a.URL, a.description, a.appname, a.cate_l1, a.cate_l2, a.network, a.is_ip, a.frame,
         a.ip_name, a.ip_style, a.art_style, a.theme_l1, a.theme_l2, a.developer_long, a.developer_short, a.publisher_long, a.publisher_short, trim(b.name) as network_id
  from mysql_table_info_tmp a
  left join
  dm_sdk_mapping.game_network_frame_id_mapping b on trim(a.network)=trim(b.id)
) i on c.pkg = i.pkg
join
(
  select a.pkg, a.apppkg, a.URL, a.description, a.appname, a.cate_l1, a.cate_l2, a.network, a.is_ip, a.frame,
         a.ip_name, a.ip_style, a.art_style, a.theme_l1, a.theme_l2, a.developer_long, a.developer_short, a.publisher_long, a.publisher_short, trim(b.name) as frame_id
  from mysql_table_info_tmp a
  left join
  dm_sdk_mapping.game_network_frame_id_mapping b on trim(a.frame)=trim(b.id)
) j on c.pkg = j.pkg
group by c.pkg, c.apppkg, c.URL, c.description, c.appname, c.cate_l1, c.cate_l2, c.network, c.is_ip, c.frame,
         c.ip_name, c.ip_style, c.art_style, c.theme_l1, c.theme_l2, c.developer_long, c.developer_short, c.publisher_long, c.publisher_short
"
checkDataSqls="
--校验pkg是否重复
select pkg,count(1)
from test_info_tmp
group by pkg
having count(1)>1;

--校验同一个apppkg是否出现了多次不同分类
select *
from test_info_tmp c
where c.apppkg in (
  select apppkg
  from
  (
    select apppkg, count(cate_l1) as cnt_cate1, count(cate_l2) as cnt_cate2, count(ip_style) as cnt_ip, count(network) as cnt_network,
           count(frame) as cnt_frame, count(art_style) as cnt_art, count(theme_l1) as cnt_theme_l1, count(theme_l2) as cnt_theme_l2
    from
    (
      select apppkg, trim(cate_l1) as cate_l1, trim(cate_l2) as cate_l2, trim(ip_style) as ip_style, trim(network) as network,
             trim(frame) as frame, trim(art_style) as art_style, trim(theme_l1) as theme_l1, trim(theme_l2) as theme_l2
      from test_info_tmp
      group by apppkg, trim(cate_l1), trim(cate_l2), trim(ip_style), trim(network), trim(frame), trim(art_style), trim(theme_l1), trim(theme_l2)
    ) a
    group by apppkg
  ) b
  where b.cnt_cate1>=2 or b.cnt_cate2>=2 or b.cnt_ip>=2 or b.cnt_network>=2 or b.cnt_frame>=2 or b.cnt_art>=2 or b.cnt_theme_l1>=2 or b.cnt_theme_l2>=2
);

--校验同一个appname是否出现了多次不同分类
select *
from test_info_tmp c
where c.appname in (
  select appname
  from
  (
    select appname, count(cate_l1) as cnt_cate1, count(cate_l2) as cnt_cate2, count(ip_style) as cnt_ip, count(network) as cnt_network,
           count(frame) as cnt_frame, count(art_style) as cnt_art, count(theme_l1) as cnt_theme_l1, count(theme_l2) as cnt_theme_l2
    from
    (
      select appname, trim(cate_l1) as cate_l1, trim(cate_l2) as cate_l2, trim(ip_style) as ip_style, trim(network) as network,
             trim(frame) as frame, trim(art_style) as art_style, trim(theme_l1) as theme_l1, trim(theme_l2) as theme_l2
      from test_info_tmp
      group by appname, trim(cate_l1), trim(cate_l2), trim(ip_style), trim(network), trim(frame), trim(art_style), trim(theme_l1), trim(theme_l2)
    ) a
    group by appname
  ) b
  where b.cnt_cate1>=2 or b.cnt_cate2>=2 or b.cnt_ip>=2 or b.cnt_network>=2 or b.cnt_frame>=2 or b.cnt_art>=2 or b.cnt_theme_l1>=2 or b.cnt_theme_l2>=2
)
"

bakAndInsertSql="
insert overwrite table $dim_game_app_detail_par partition(version=$targetPar)
select a.pkg, a.apppkg, a.url, a.description,trim(regexp_replace(a.appname, '\\n|\\r\\n', '')) as appname, a.cate_l1, a.cate_l2, a.network, a.is_ip, a.frame,
       a.ip_name, a.ip_style, a.art_style, a.theme_l1, a.theme_l2, a.developer_long, a.developer_short, a.publisher_long, a.publisher_short, a.theme_l1_id, a.theme_l2_id,
       a.cate_l2_id, a.art_style_id, a.ip_style_id, a.network_id, a.frame_id
from test_info_tmp a
where length(a.pkg)>0
and cate_l2_id is not null and theme_l1_id is not null and theme_l2_id is not null and art_style_id is not null and ip_style_id is not null
and cate_l1 like '%游戏%';

insert overwrite table $dim_game_app_detail_par partition(version=1000)
select pkg,apppkg,url,description,appname,cate_l1,cate_l2,network,is_ip,frame,ip_name,ip_style,art_style,theme_l1,theme_l2,
       developer_long,developer_short,publisher_long,publisher_short,theme_l1_id,theme_l2_id,cate_l2_id,art_style_id,ip_style_id,
       network_id,frame_id
from $dim_game_app_detail_par
where version='$targetPar'
"

doubleCheckSql="
select *
from $dim_game_app_detail_par
where version='1000'
and (theme_l1_id is null or theme_l2_id is null or cate_l2_id is null or art_style_id is null or ip_style_id is null or network_id is null or frame_id is null) 
"

spark2-submit --master yarn --deploy-mode cluster \
--class com.mob.mobdi.utils.sortsystem.SortingDbUtils \
--driver-memory 6G \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.dynamicAllocation.executorIdleTimeout=20s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--executor-memory 12G --executor-cores 4 \
--name "SortingGameAppCagtegoryUtils" \
--conf spark.sql.shuffle.partitions=100 \
--conf spark.sql.autoBroadcastJoinThreshold=519715200 \
/home/dba/mobdi_center/lib/MobDI_Monitor-1.0-SNAPSHOT-jar-with-dependencies.jar "$mysqlInfoStr" "$mailList" "$insertTestTableSql" "$checkDataSqls" "$bakAndInsertSql" "$doubleCheckSql"
