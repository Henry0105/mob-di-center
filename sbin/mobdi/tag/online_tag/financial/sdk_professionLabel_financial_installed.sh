#!/bin/sh

: '
@owner:xdzhang
@describe:行业标签-App在装量标签(根据近40天在装量计算)
@projectName:MobDI
@BusinessName:profile_online
@SourceTable:dm_sdk_mapping.app_tag_system_mapping_par,dm_sdk_mapping.tag_cat_mapping_dmp_par,dm_sdk_mapping.app_pkg_mapping_par,dm_mobdi_topic.dws_device_install_app_status_40d_di
@TargetTable:rp_mobdi_app.rp_device_financial_installed_profile,dw_mobdi_md.device_installed_tmp
@TableRelation:dm_sdk_mapping.app_tag_system_mapping_par,dm_sdk_mapping.tag_cat_mapping_dmp_par,dm_sdk_mapping.app_pkg_mapping_par,dm_mobdi_topic.dws_device_install_app_status_40d_di->dw_mobdi_md.device_installed_tmp|dw_mobdi_md.device_installed_tmp,dm_mobdi_topic.dws_device_install_app_status_40d_di,dm_sdk_mapping.app_pkg_mapping_par->rp_mobdi_app.rp_device_financial_installed_profile
'
set -x -e

: '
@parameters
@date1:传入日期参数，为脚本运行日期（重跑不同）
@tmptable:中间表名称（不带库名）
@targetTable:结果表名称（不带库名）
'
if [ $# -lt 3 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: <date> <tmptable> <targetTable>"
     exit 1
fi

date1=$1
tmptable=$2
targetTable=$3

#计算数据分区，date1-2（t-2）
date=`date -d "$date1" "+%Y%m%d"`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#databases
appdb=$dm_mobdi_report
tmpdb=$dw_mobdi_tmp

#input
#dws_device_install_app_status_40d_di=dm_mobdi_topic.dws_device_install_app_status_40d_di

#mapping
#dim_app_tag_system_mapping_par=dim_sdk_mapping.dim_app_tag_system_mapping_par
#app_tag_system_mapping_par=dim_sdk_mapping.app_tag_system_mapping_par
#dim_tag_cat_mapping_dmp_par=dim_sdk_mapping.dim_tag_cat_mapping_dmp_par
#tag_cat_mapping_dmp_par=dim_sdk_mapping.tag_cat_mapping_dmp_par
#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par
#app_pkg_mapping_par=dim_sdk_mapping.app_pkg_mapping_par

#create tmp table
#hive -e "CREATE TABLE IF NOT EXISTS dw_mobdi_md.${tmptable} (device string, tag string ,apppkg string);"
weekDay=`date -d "$date" +%w`
if [ $weekDay -ne 1 ]; then
	exit 0;
fi

: '
@part_1:读config.conf配置文件，拼接所输出表的列名、标签权重。
配置文件示例：借贷=borrowing=2.73656580503609，
                    ----借贷 为tag的二级分类（dm_sdk_mapping.tag_cat_mapping_dmp中cat2字段）；
                    ----borrowing 为输出表列名；
                    ----2.73656580503609 为app在借贷类标签的总权重最小值，低于此值的App不列入计算；
功能：根据配置文件内容，拼接sql
                havingStr    ----Having函数处理条件   
                total        ----行业标签汇总值计算方式
                param        ----输出字段列表 
'
cd `dirname $0`
FILENAME=./config.conf
int=0;

while read LINE
do

   LD_IFS="$IFS";
   IFS="=";
   arr=($LINE);
   IFS="$OLD_IFS";
  if [ ${int} = 0 ]; then
        havingStr="SUM(case when b.cat2='${arr[0]}' then a.norm_tfidf else 0 end) >${arr[2]}"
		total="max(case WHEN t.tag='${arr[0]}' then 1 else 0 END)"
		let totalN+=1
  else
        if [ ${arr[0]} = '信用卡' ]; then
            havingStr="${havingStr} UNION ALL
                       SELECT a.apppkg AS apppkg,
                          '信用卡' AS cat
                        FROM (select rank.apppkg as apppkg, rank.tag as tag,rank.norm_tfidf as norm_tfidf from (select n.apppkg as apppkg,n.tag as tag ,n.norm_tfidf as norm_tfidf , Row_number() over(partition by n.apppkg,n.tag ORDER BY  n.norm_tfidf DESC ) as rank
                               from $dim_app_tag_system_mapping_par n where
							   version='1000') rank where rank.rank =1) a
                        where a.tag = '信用卡' group by a.apppkg , a.tag having SUM(a.norm_tfidf) >${arr[2]}"
          else
           havingStr="${havingStr} OR SUM(case when b.cat2='${arr[0]}' then a.norm_tfidf else 0 end) >${arr[2]}"
		   total="${total}+max(case WHEN t.tag='${arr[0]}' then 1 else 0 END)"
		   let totalN+=1
        fi
  fi
   int=1
   param="${param},MAX(CASE t.tag WHEN '${arr[0]}' THEN 1 ELSE 0 END) AS ${arr[1]}"
done <$FILENAME


: '
@part_2:
实现功能：在设备安装表中筛选符合标签的在装信息，得到设备号，标签，和apppkg。

实现逻辑：1.找出Apppkg在每一tag上的最大tfidf（dm_sdk_mapping.app_tag_system_mapping_par）；
          2.关联tag对应cat2（二级分类）,保留配置文件中的二级分类标签(若为tag则处理不用再cat2上汇总),
            对Apppkg在cat2上进行sum(tfidf),过滤掉权重较小的App（dm_sdk_mapping.app_tag_system_mapping_par，
            dm_sdk_mapping.tag_cat_mapping_dmp_par）；
          3.提取每一用户近40天安装的pkg(dm_mobdi_topic.dws_device_install_app_status_40d_di,final_flag <> -1),对
            pkg进行渠道清理（dm_sdk_mapping.app_pkg_mapping_par，pkg->apppkg）;
          4.将第2和第3步结构进行关联，得到每一device安装的每一Apppkg以及Apppkg对应标签；
输出结果：表名（根据传入参数$tmptable确定）
          字段：device  --设备
                tag     --标签（符合配置文件中的标签）
                apppkg  --包名
'
hive -e "SET hive.exec.compress.output=true;
         SET mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;
         set mapred.output.compression.type=BLOCK;
         set hive.exec.parallel=true;
         set hive.exec.parallel.thread.number=100;
         set hive.exec.compress.intermediate=true;
         set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
         set hive.intermediate.compression.type=BLOCK;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';

insert overwrite table ${tmpdb}.${tmptable}
SELECT device_install.device AS device,
       app_install.cat AS tag,
       device_install.apppkg AS apppkg
FROM
  (SELECT a.apppkg AS apppkg,
          b.cat2 AS cat
  FROM (select rank.apppkg as apppkg, rank.tag as tag,rank.norm_tfidf as norm_tfidf
    from (select n.apppkg as apppkg,n.tag as tag ,n.norm_tfidf as norm_tfidf ,
       	   Row_number() over(partition by n.apppkg,n.tag ORDER BY  n.norm_tfidf DESC ) as rank
            from $dim_app_tag_system_mapping_par n
			where version ='1000'
		) rank where rank.rank =1) a
  JOIN  (select cat2,tag from $dim_tag_cat_mapping_dmp_par
	         where version='1000'
	     ) b
        ON a.tag = b.tag
    GROUP BY  a.apppkg,b.cat2
    HAVING ${havingStr}) app_install

  JOIN

  (SELECT NVL(app_pkg.apppkg,device_filter.pkg) AS apppkg,
          device_filter.device AS device,
          device_filter.day AS day
  FROM
    (SELECT device.device AS device,
            device.pkg AS pkg,
            device.final_flag AS final_flag,
            device.day AS day

    FROM $dws_device_install_app_status_40d_di device
    WHERE device.final_flag <> -1
            AND device.day =${date} ) device_filter
    LEFT  JOIN (
			select apppkg,pkg from $dim_app_pkg_mapping_par
			where version='1000'
		) app_pkg
        ON app_pkg.pkg = device_filter.pkg
    GROUP BY  NVL(app_pkg.apppkg,device_filter.pkg), device_filter.device, device_filter.day ) device_install
      ON device_install.apppkg = app_install.apppkg;
"



: '
@part_3:
实现功能：处理part_2得到的设备标签app中间表，统计每个设备安装在各个标签中的Apppkg安装个数,安装带有行业标签Apppkg总量,
          安装带有行业标签Apppkg在总安装量中的占比。

实现逻辑：1.对上述中间表的每一device，tag上对apppkg进行求和，得出每个设备安装在各个标签中的app安装个数，
            安装带有行业标签Apppkg总量；
          2.提取每一用户近40天安装的pkg总量(dm_mobdi_topic.dws_device_install_app_status_40d_di,final_flag <> -1);
          3.将第1和第2部结构进行关联，得到每个设备安装在各个标签中的app安装个数,安装带有行业标签Apppkg总量,
            安装带有行业标签Apppkg在总安装量中的占比；
输出结果：表名（根据传入参数$targetTable 确定）
          字段：device                      ----设备
                不同标签的Apppkg安装个数    ----（配置文件中的读取）
                total                       ----安装带有行业标签Apppkg总量
                percent                     ----安装带有行业标签Apppkg在总安装量中的占比
'

hive -e "SET hive.exec.compress.output=true;
         SET mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;
         set mapred.output.compression.type=BLOCK;
         set hive.exec.parallel=true;
         set hive.exec.parallel.thread.number=100;
         set hive.exec.compress.intermediate=true;
         set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
         set hive.intermediate.compression.type=BLOCK;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';

insert overwrite table ${appdb}.${targetTable} partition(day=${date})
select all_tmp.device,sum(all_tmp.borrowing) as borrowing,sum(all_tmp.bank) as bank,sum(all_tmp.investment) as investment
,sum(all_tmp.insurance) as insurance,sum(all_tmp.securities) as securities,sum(all_tmp.finaces) as finaces,
 sum(all_tmp.credit_card) as credit_card ,count(*) as total,count(*)/all_tmp.count as percent
 from
(SELECT t.device${param},
       app_count.count as count
FROM ${tmpdb}.${tmptable} t
 join
(select all_device_apppkg.device,
       count(*) as count
     from
   (SELECT device_filter.device AS device,
        NVL(app_pkg.apppkg,device_filter.pkg) as apppkg,
        device_filter.day as day
        from (
        select device.day,device.pkg,device.device from $dws_device_install_app_status_40d_di device
        WHERE device.final_flag <> -1
              AND device.day =${date}
	) device_filter
        LEFT  JOIN
	(
	  select apppkg,pkg from $dim_app_pkg_mapping_par
	    where version='1000'
	) app_pkg
       ON app_pkg.pkg = device_filter.pkg
        GROUP BY  device_filter.device, device_filter.day,NVL(app_pkg.apppkg,device_filter.pkg)) all_device_apppkg  group by all_device_apppkg.device) app_count
  on t.device = app_count.device
group by t.device,t.apppkg,app_count.count) all_tmp group by all_tmp.device,all_tmp.count;
"
#~/jdk1.8.0_45/bin/java -cp /home/dba/mobdi_center/lib/mysql-utils-1.0-jar-with-dependencies.jar  com.mob.TagUpdateTime -d rp_mobdi_app -t rp_device_financial_installed_profile

#clear tmp table
hive -e "truncate table ${tmpdb}.${tmptable}"