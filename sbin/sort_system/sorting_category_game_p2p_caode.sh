#!/bin/bash

set -e -x

: '
@owner:liuyanqiang
@describe: 实现分拣系统中，将app标签分类从MySQL导入hive中去，并check数据，如果数据无效，则发邮件
@projectName:SortingSystem
@BusinessName:sort_utils
@SourceTable:sorting_system.app_category,dm_sdk_mapping.cate_id_mapping_par,test.app_category_mapping_new_test,test.chenfq_app_category_mapping_new
@TargetTable:test.chenfq_app_category_mapping_new,test.app_category_mapping_new_test,
@TableRelation:sorting_system.app_category->test.chenfq_app_category_mapping_new|dm_sdk_mapping.cate_id_mapping_par,test.chenfq_app_category_mapping_new->test.app_category_mapping_new_test|dm_sdk_mapping.app_category_mapping_new->dm_sdk_mapping.app_category_mapping_new_bak|test.app_category_mapping_new_test->dm_sdk_mapping.app_category_mapping_new
'
source /home/dba/mobdi_center/conf/hive-env.sh

#固定参数参数管理区
#cate_id和cate_name 映射表
#dim_cate_id_mapping_par=dim_sdk_mapping.dim_cate_id_mapping_par
#cate_id_mapping_par=dm_sdk_mapping.cate_id_mapping_par
#任务类型
type=$1
#脚本启动日期
day=$(date +%Y%m%d)
#mysql连接信息
url=10.18.97.128
port=3307
userName=dba
passWord=zRQkM6BcK102YwGhMXy5RxorqteRfZ
mailList="DIMonitor@mob.com;yqzhou@mob.com"


#测试表专用 （用于对目标表数据的测试，如果测试通过将数据轻度清洗后写入目标表）
#category
#test.app_category_mapping_new_test
category_app_category_mapping_new_test='mobdi_test.dlzhang_app_category_mapping_new_test'
#test.chenfq_mysqlData_20180410_2
game_mysqlData_app_theme_cate_test='mobdi_test.dlzhang_game_mysqlData_app_theme_cate_test'
#test.multiloan_from_mysql_2
p2p_multiloan_from_mysql_app_cate_mapping='mobdi_test.dlzhang_p2p_multiloan_from_mysql_app_cate_mapping'

#方法管理区
: '
方法实现逻辑：
  1、如果mysql中数据更新，则顺序执行代码，并将最大时间写入 xxx.txt
  2、如果mysql中数据没有更新，则终止代码 exit0 退出，并不更新 xxx.txt
  该方法通过将昨日最大日期存在 xxx.txt 文件中,次日通过shell-MySQL sql语句查询是否存在 今日最大maxTime大于昨日最大maxTime 的数据
'
function isUpdatData(){
if [ $result == 0 ]; then
   echo "have no data update,and exist..."
   exit 0
   else
     echo $lastMaxTime > $maxTime2file
fi
}


#数据处理逻辑区域
: '
  app_category_mapping_par 表数据生成逻辑
'
function categoryInit() {
categoryInsertTestTableSql="
--之前逻辑产生的数据中存在带空格的数据，未处理。带重构完后去空操作可以在这部分逻辑进行
CREATE TABLE if not exists $category_app_category_mapping_new_test(pkg string,apppkg string,appname string,cate_l1 string,cate_l2 string,cate_l1_id string,cate_l2_id string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';
insert overwrite table $category_app_category_mapping_new_test
  select pkg_catename_mapping.pkg,pkg_catename_mapping.apppkg,pkg_catename_mapping.appname,pkg_catename_mapping.cate_l1,
         pkg_catename_mapping.cate_l2,trim(regexp_replace(cateid_mapping.cate_l1_id, '\\n|\\r', '')) as cate_l1_id,
         trim(regexp_replace(cateid_mapping.cate_l2_id, '\\n|\\r', '')) as cate_l2_id
    from
      (
        select pkg,apppkg,appname,cate_l1,cate_l2
        from
          (
            select
              trim(regexp_replace(pkg, '\\n|\\r', '')) as pkg,
              trim(regexp_replace(apppkg, '\\n|\\r', '')) as apppkg,
              trim(regexp_replace(appname, '\\n|\\r', '')) as appname,
              trim(regexp_replace(cate_l1, '\\n|\\r', '')) as cate_l1,
              trim(regexp_replace(cate_l2, '\\n|\\r', '')) as cate_l2,
              row_number() over(partition by pkg order by time desc) as rn
            from $hiveTableInfo
            where  note != 'unknown'  and pkg != '' and cate_l1 is not null and cate_l2 is not null
          ) a
        where rn = 1
      ) pkg_catename_mapping
      left join $dim_cate_id_mapping_par cateid_mapping on cateid_mapping.version = '1000'
      and pkg_catename_mapping.cate_l1 = trim(cateid_mapping.cate_l1)
      and pkg_catename_mapping.cate_l2 = trim(cateid_mapping.cate_l2)
"
#数据检测逻辑
categoryCheckDataSqls="
--判断pkg唯一性
select pkg,count(1) cnt from $category_app_category_mapping_new_test group by pkg having cnt>1;
--判断cate_l2_id 是由对应 cate_l1_id 延续生成的
select cate_l1_id,cate_l2_id from $dim_cate_id_mapping_par where cate_l1_id != substr(cate_l2_id,1,4);
--判断apppkg与cate_l1 和 cate_l2 的一对一关系
select apppkg,cate_l1,cate_l2 from $category_app_category_mapping_new_test c
where c.apppkg in ( select apppkg from ( select apppkg, count(cate_l1) as cnt_cate1, count(cate_l2) as cnt_cate2 from
(select apppkg, cate_l1, cate_l2 from test.app_category_mapping_new_test group by apppkg, cate_l1, cate_l2) a group by apppkg)b
where b.cnt_cate1>=2 or b.cnt_cate2>=2);
--判断是否有未匹配上cate_id的catename
select pkg from $category_app_category_mapping_new_test where cate_l1_id is null or cate_l2_id is null
"

#数据清洗&输出数据
categoryBakAndInsertSql="
insert overwrite table $outPutTable partition(version=$version)
select
 pkg,
 apppkg,
 appname,
 cate_l1,
 cate_l2,
 cate_l1_id,
 cate_l2_id
from
 $category_app_category_mapping_new_test
;

insert overwrite table $outPutTable partition(version=$versionNum)
select pkg,apppkg,appname,cate_l1,cate_l2,cate_l1_id,cate_l2_id
from $outPutTable
where version = '$version';
"
#回归数据测试
categoryDoubleCheckSql="select * from test.app_category_mapping_par where version='$versionNum' and  (cate_l1_id is null or cate_l2_id is null)"
}


:'
 game_app_detail_par 表数据生成逻辑
 '
function gameInit() {
gameInsertTestTableSql="
insert onerwrite table $game_mysqlData_app_theme_cate_test
select
  pkg_cate_name.pkg,
  pkg_cate_name.apppkg,
  pkg_cate_name.URL as url,
  pkg_cate_name.description,
  pkg_cate_name.appname,
  pkg_cate_name.cate_l1,
  pkg_cate_name.cate_l2,
  pkg_cate_name.network,
  pkg_cate_name.is_ip,
  pkg_cate_name.frame,
  pkg_cate_name.ip_name,
  pkg_cate_name.ip_style,
  pkg_cate_name.art_style,
  pkg_cate_name.theme_l1,
  pkg_cate_name.theme_l2,
  pkg_cate_name.developer_long,
  pkg_cate_name.developer_short,
  pkg_cate_name.publisher_long,
  pkg_cate_name.publisher_short,
  trim(theme_id_mapping_1.name) as theme_l1_id,
  trim(theme_id_mapping_2.name) as theme_l2_id,
  cate_mapping.cate_l2_id,
  trim(style_id_mapping.name) as art_style_id,
  trim(ip_id_mapping.name) as ip_style_id,
  trim(network_id_mapping.name) as network_id,
  trim(frame_id_mapping.name) as frame_id
  from
(
  select pkg,apppkg,URL,description,appname,cate_l1,cate_l2,network,is_ip,frame,ip_name,ip_style,art_style,theme_l1,
         theme_l2,developer_long,developer_short,publisher_long,publisher_short
  from
  (
   select
    trim(pkg) as pkg,
    trim(apppkg) as apppkg,
    trim(URL) as URL,
    trim(description) as description,
    trim(appname) as appname,
    trim(cate_l1) as cate_l1,
    trim(cate_l2) as cate_l2,
    trim(network) as network,
    trim(is_ip) as is_ip,
    trim(frame) as frame,
    trim(ip_name) as ip_name,
    trim(ip_style) as ip_style,
    trim(art_style) as art_style,
    trim(theme_l1) as theme_l1,
    trim(theme_l2) as theme_l2,
    trim(developer_long) as developer_long,
    trim(developer_short) as developer_short,
    trim(publisher_long) as publisher_long,
    trim(publisher_short) as publisher_short
   from
   $hiveTableInfo
   where cate_l1 like '%游戏%'
  ) source
  group by pkg,apppkg,URL,description,appname,cate_l1,cate_l2,network,is_ip,frame,ip_name,ip_style,art_style,theme_l1,
         theme_l2,developer_long,developer_short,publisher_long,publisher_short
) pkg_cate_name
left join
$dim_cate_id_mapping_par cate_mapping on  cate_mapping.version = '1000' and pkg_cate_name.cate_l1 = cate_mapping.cate_l1 and pkg_cate_name.cate_l2 = cate_mapping.cate_l2
left join
$game_theme_id_mapping theme_id_mapping_1 on  trim(theme_id_mapping_1.id) = pkg_cate_name.theme_l1
left join
$game_theme_id_mapping theme_id_mapping_2 on  trim(theme_id_mapping_2.id) = pkg_cate_name.theme_l2
left join
$game_style_id_mapping style_id_mapping on trim(style_id_mapping.id) = pkg_cate_name.art_style
left join
$game_ip_id_mapping ip_id_mapping on trim(ip_id_mapping.id) = pkg_cate_name.ip_style
left join
$game_network_frame_id_mapping network_id_mapping on trim(network_id_mapping.id) = pkg_cate_name.network
left join
$game_network_frame_id_mapping frame_id_mapping on trim(frame_id_mapping.id) = pkg_cate_name.frame
"
# 数据质量检测
gameCheckDataSqls="
select pkg,count(1) from $game_mysqlData_app_theme_cate_test group by pkg having count(1)>1;
select appname
  from (
        select appname,count(cate_l1) as cnt_cate1,count(cate_l2) as cnt_cate2,count(ip_style) as cnt_ip,
          count(network) as cnt_network,count(frame) as cnt_frame,count(art_style) as cnt_art,count(theme_l1) as cnt_theme_l1,
          count(theme_l2) as cnt_theme_l2
        from (
          select appname, cate_l1, cate_l2, ip_style, network, frame, art_style, theme_l1, theme_l2
          from $game_mysqlData_app_theme_cate_test
          group by appname,cate_l1,cate_l2,ip_style,network,frame,art_style,theme_l1,theme_l2
          ) a
        group by appname
      ) b
    where b.cnt_cate1 >= 2 or b.cnt_cate2 >= 2 or b.cnt_ip >= 2 or b.cnt_network >= 2 or b.cnt_frame >= 2 or b.cnt_art >= 2
        or b.cnt_theme_l1 >= 2 or b.cnt_theme_l2 >= 2;
select apppkg
  from (
        select apppkg,count(cate_l1) as cnt_cate1,count(cate_l2) as cnt_cate2,count(ip_style) as cnt_ip,
          count(network) as cnt_network,count(frame) as cnt_frame,count(art_style) as cnt_art,count(theme_l1) as cnt_theme_l1,
          count(theme_l2) as cnt_theme_l2
        from (
          select apppkg, cate_l1, cate_l2, ip_style, network, frame, art_style, theme_l1, theme_l2
          from $game_mysqlData_app_theme_cate_test
          group by apppkg,cate_l1,cate_l2,ip_style,network,frame,art_style,theme_l1,theme_l2
          ) a
        group by apppkg
      ) b
    where b.cnt_cate1 >= 2 or b.cnt_cate2 >= 2 or b.cnt_ip >= 2 or b.cnt_network >= 2 or b.cnt_frame >= 2 or b.cnt_art >= 2
        or b.cnt_theme_l1 >= 2 or b.cnt_theme_l2 >= 2
"

gameBakAndInsertSql="
create tabke if not exists $outPutTable like  test.game_app_detail_par stored as orc;
insert overwrite table $outPutTable partition(version=$version)
select pkg, apppkg, url, description, appname, cate_l1, cate_l2, network, is_ip, frame, ip_name, ip_style, art_style,
  theme_l1, theme_l2, developer_long, developer_short, publisher_long, publisher_short, theme_l1_id, theme_l2_id,
  cate_l2_id, art_style_id, ip_style_id, network_id, frame_id
from mobdi_test.dlzhang_game_mysqlData_app_theme_cate_test
 where
cate_l2_id is not null
and theme_l1_id is not null
and theme_l2_id is not null
and art_style_id is not null
and ip_style_id is not null;
--备份 分区1000
insert overwrite table $outPutTable partition (version = $versionNum)
select pkg, apppkg, url, description, appname, cate_l1, cate_l2, network, is_ip, frame, ip_name, ip_style, art_style,
  theme_l1, theme_l2, developer_long, developer_short, publisher_long, publisher_short, theme_l1_id, theme_l2_id,
  cate_l2_id, art_style_id, ip_style_id, network_id, frame_id
from $outPutTable whrer version = '$version'
"

gameDoubleCheckSql="
select * from $outPutTable
where version='$versionNum'
and (theme_l1_id is null or theme_l2_id is null or cate_l2_id is null or art_style_id is null or ip_style_id is null or network_id is null or frame_id is null)
"
}

:'
  p2p_app_cat_par 表数据生成逻辑
'
function p2pInit() {
    p2pInsertTestTableSql="
  insert overwrite table $p2p_multiloan_from_mysql_app_cate_mapping
    select pkg, apppkg_new as apppkg, cat, cate_id
    from
      (
        select
          trim(multiloan.pkg) as pkg,
          case when multiloan.apppkg is null or length(trim(multiloan.apppkg)) = 0 then trim(multiloan.pkg) else trim(multiloan.apppkg) end as apppkg_new,
          trim(multiloan.cate_l3) as cat, trim(p2p.cate_id) as cate_id
        from
          (
            select pkg, apppkg, cate_l3
            from
              (
                select trim(pkg) as pkg, trim(apppkg) as apppkg, trim(appname) as appname, trim(cate_l1) as cate_l1,
                  trim(cate_l2) as cate_l2, trim(cate_l3) as cate_l3, trim(cate_l1_id) as cate_l1_id, trim(cate_l2_id) as cate_l2_id
                from $hiveTableInfo
              ) b
          ) multiloan
          left join dm_sdk_mapping.p2p_app_cat_par p2p on p2p.version = '1000'
          and trim(p2p.cat) = trim(multiloan.cate_l3)
      ) source_table
    where cate_id is not null and cat is not null
    group by pkg, apppkg_new, cat, cate_id"

p2pCheckDataSqls="
    select apppkg
        from
          ( select apppkg, count(cat) as cnt_cat, count(cate_id) as cnt_cate_id
            from
              ( select apppkg, cat as cat, cate_id as cate_id
                from $p2p_multiloan_from_mysql_app_cate_mapping
                group by apppkg, cat, cate_id
              ) a
            group by apppkg
          ) b
        where b.cnt_cat >= 2 or b.cnt_cate_id >= 2
"
p2pDakAndInsertSql="
insert overwrite table $outPutTable partition(version=$version)
select apppkg, cat,cate_id
from  $p2p_multiloan_from_mysql_app_cate_mapping;

insert overwrite table test.p2p_app_cat_par partition(version=$versionNum)
select apppkg, cat,cate_id
from $outPutTable where version = '$version'
"
p2pDoubleCheckSql="
select apppkg, cat,cate_id from test.p2p_app_cat_par
where version='$versionNum'
and (cat is null or cate_id is null)
"

}


#选择输出模式区域（私自）
case $type in
  "category")
  echo "app_category_mapping_par 表数据进行更新"
  #Mysql库表
  mysqlSourceDB='sorting_system'
  mysqlSourceTable='app_category'
  #最终数据上线的测试表(app分类结果表) test.app_category_mapping_par
  outPutTable='mobdi_test.dlzhang_app_category_mapping_par'
  #pkg和catename 映射表
  hiveTableInfo='test.chenfq_app_category_mapping_new'
  #用于记录mysql表中当前最大日期的文件
  maxTime2file=app_oldtime.txt
  #mysql表昨日最大日期时间 eg: 20191222
  oldMaxTime=`cat $maxTime2file`
  categoryInit
  insertTestTableSql=$categoryInsertTestTableSql
  checkDataSqls=$categoryCheckDataSqls
  bakAndInsertSql=$categoryBakAndInsertSql
  doubleCheckSql=$categoryDoubleCheckSql;;
  "game")
  echo "game_app_detail_par 表数据进行更新"
  #Mysql库表
  mysqlSourceDB='sorting_system'
  mysqlSourceTable='game_detail'
  #最终数据上线的测试表(app分类结果表) test.game_app_detail_par
  outPutTable='mobdi_test.dlzhang_game_app_detail_par'
  #pkg和catename 映射表
  hiveTableInfo='test.chenfq_mysqlData_20180410_0'
  #用于记录mysql表中当前最大日期的文件
  maxTime2file=game_oldtime.txt
  #mysql表昨日最大日期时间 eg: 20191222
  oldMaxTime=`cat $maxTime2file`
  #mapping表
  #theme_id
  game_theme_id_mapping='dm_sdk_mapping.game_theme_id_mapping'
  #style_id
  game_style_id_mapping='dm_sdk_mapping.game_style_id_mapping'
  #ip_id
  game_ip_id_mapping='dm_sdk_mapping.game_ip_id_mapping'
  #network_id,frame_id
  game_network_frame_id_mapping='dm_sdk_mapping.game_network_frame_id_mapping'
  gameInit
  insertTestTableSql=$gameInsertTestTableSql
  checkDataSqls=$gameCheckDataSqls
  bakAndInsertSql=$gameBakAndInsertSql
  doubleCheckSql=$gameDoubleCheckSql
  ;;
  "p2p")
  echo "p2p_app_cat_par 表数据进行更新"
  #Mysql库表
  mysqlSourceDB='sorting_system'
  mysqlSourceTable='multiloan'
  #最终数据上线的测试表(app分类结果表) dm_sdk_mapping.p2p_app_cat_par
  outPutTable='mobdi_test.dlzhang_p2p_app_cat_par'
  #pkg和catename 映射表
  hiveTableInfo='test.multiloan_from_mysql'
  #用于记录mysql表中当前最大日期的文件
  maxTime2file=multiloan_oldtime.txt
  #mysql表昨日最大日期时间 eg: 20191222
  oldMaxTime=`cat $maxTime2file`
  p2pInit
  insertTestTableSql=$p2pInsertTestTableSql
  checkDataSqls=$p2pCheckDataSqls
  bakAndInsertSql=$p2pDakAndInsertSql
  doubleCheckSql=$p2pDoubleCheckSql
esac

#运行参数设定区域（公用）
#source表今天中time大于昨日最大日期的数据 eg： select * from sourceTable where maxTime > oldMaxTime （用于判断数据更新）
result=`mysql -h$url -u$dba -p'$passWord' -P$port -e "select max(time)>$oldMaxTime from $mysqlSourceDB.$mysqlSourceTable" -sN`
#source表获取当日最大时间日期 eg： 20191223 （用于判断数据更新）
lastMaxTime=`mysql -h$url -u$dba -p'$passWord' -P$port  -e "select max(time) from $mysqlSourceDB.$mysqlSourceTable" -sN`
#清除表数据
cleanTableSql='truncate table ${hiveTableInfo}'
#mysql连接属性，用于sparkReadJDBC
mysqlInfoStr='{"userName":"${dba}","pwd":"$passWord","dbName":"$mysqlSourceDB","host":"$url","port":$port,"tableName":"$mysqlSourceTable"}'

#分区管理（获取最新分区）（公用）
#EG：1000/1001  （这句的意义在于如果日后 版本从1000升级到1001 那么可方便升级后续分区）（但观察一年下来 全为1000并为出现版本升级情况，后续开发就情况自行废除）
versionNum=`hive -e "show partitions $outPutTable" | awk -F '=' '{print $2}' | tail -n 1 | awk -F '.' '{print $1}'`
#作为落地表的分区值 EG： 1000.20191223
version=$versionNum.$lastMaxTime

#判断source表是否已更新
isUpdatData


spark2-submit --master yarn --deploy-mode cluster \
	--class com.youzu.mob.SortingSystem.SortingDbUtils \
	--driver-memory 6G \
	--conf spark.shuffle.service.enabled=true \
	--conf spark.dynamicAllocation.enabled=true \
	--conf spark.dynamicAllocation.minExecutors=10 \
	--conf spark.dynamicAllocation.maxExecutors=20 \
	--conf spark.dynamicAllocation.executorIdleTimeout=20s \
	--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
	--executor-memory 12G --executor-cores 4 \
	--name "SortingAppCagtegoryUtils" \
	--conf spark.sql.shuffle.partitions=100 \
	--conf spark.sql.autoBroadcastJoinThreshold=519715200 \
	SortingSystem-Etl-1.0-SNAPSHOT-jar-with-dependencies.jar  "$cleanTableSql" "$mysqlInfoStr" "$hiveTableInfo" "$mailList" "$insertTestTableSql" "$checkDataSqls" "$bakAndInsertSql" "$doubleCheckSql"

#newVer=`hive -e "show partitions test.app_category_mapping_par" | sort | tail -n 1 | awk -F= '{print $2}'`
 newVer=`hive -S -e "show partitions test.app_category_mapping_par" | grep version |sort | tail -n 1 | awk -F= '{print $2}'`

if [ $newVer == $version ];
then
 echo "update time"
 updatLastTime
else
 echo "source table have errors,does not update!"
fi

