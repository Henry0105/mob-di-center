#!/bin/sh

: '
@owner:xdzhang
@describe:行业标签-App新装量标签(根据近7天新装量计算)
@projectName:MobDI 
@BusinessName:profile_online
@SourceTable:dm_sdk_mapping.app_tag_system_mapping_par,dm_sdk_mapping.tag_cat_mapping_dmp_par,dm_sdk_mapping.app_pkg_mapping_par,dm_mobdi_master.master_reserved_new
@TargetTable:rp_mobdi_app.rp_device_financial_install_profile
@TableRelation:dm_sdk_mapping.app_tag_system_mapping_par,dm_sdk_mapping.tag_cat_mapping_dmp_par,dm_sdk_mapping.app_pkg_mapping_par,dm_mobdi_master.master_reserved_new->rp_mobdi_app.rp_device_financial_install_profile
'
set -x -e

: '
@parameters
@date1:传入日期参数，为脚本运行日期（重跑不同）
@targetTable:结果表名称（不带库名）
'
 if [ $# -lt 2 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: '<date>,<targetTable>'"
    exit 1
  fi
date1=$1
#计算数据开始分区，date1-2（t-2）
date=`date -d "$date1" "+%Y%m%d"`
#计算数据结束分区，date-7（近七天数据）
b7day=`date -d "$date -7 days" "+%Y%m%d"`
targetTable=$2

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#databases
appdb=dm_mobdi_report
tmpdb=dw_mobdi_tmp

#input
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di

#mapping
#dim_app_tag_system_mapping_par=dim_sdk_mapping.dim_app_tag_system_mapping_par
#app_tag_system_mapping_par=dim_sdk_mapping.app_tag_system_mapping_par
#dim_tag_cat_mapping_dmp_par=dim_sdk_mapping.dim_tag_cat_mapping_dmp_par
#tag_cat_mapping_dmp_par=dim_sdk_mapping.tag_cat_mapping_dmp_par
#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par
#app_pkg_mapping_par=dim_sdk_mapping.app_pkg_mapping_par

: '
@part_1:读config.conf配置文件，拼接所输出表的列名、标签权重。
配置文件示例：借贷=borrowing=2.73656580503609，
                    ----借贷 为tag的二级分类（dm_sdk_mapping.tag_cat_mapping_dmp中cat2字段）；
					----borrowing 为输出表列名；
					----2.73656580503609 为app在借贷类标签的总权重最小值，低于此值的App不列入计算；
功能：根据配置文件内容，拼接sql
	             param3    ----Having函数处理条件   
				 param2        ----输出字段列表 
				 param4        ----输出字段列表 
'
cd `dirname $0`
FILENAME=./config.conf
int=0
while read LINE
do

   LD_IFS="$IFS";
   IFS="=";
   arr=($LINE);
   IFS="$OLD_IFS";
   if [ ${arr[0]} != '信用卡' ]; then
     if [ ${int} = 0 ];then
	param4="sum(all_tmp.${arr[1]}) as ${arr[1]}"
        param2="MAX(CASE app_install.cat WHEN '${arr[0]}' THEN 1 ELSE 0 END) AS ${arr[1]}"
        param3="SUM(case when b.cat2='${arr[0]}' then a.norm_tfidf else 0 end) > ${arr[2]}"
        int=1
     else
       param4="${param4},sum(all_tmp.${arr[1]}) as ${arr[1]}"
       param2="${param2}, MAX(CASE app_install.cat WHEN '${arr[0]}' THEN 1 ELSE 0 END) AS ${arr[1]}"
       param3="${param3} OR SUM(case when b.cat2='${arr[0]}' then a.norm_tfidf else 0 end) > ${arr[2]}"
     fi
   fi  
done <$FILENAME

: '
@part_2:
实现功能:在新安装表(dm_mobdi_topic.dws_device_install_app_re_status_di)中统计近七天，二级标签符合配置文件中信息的，每个设备在每个标签中安装app的个数
实现逻辑:1.找出Apppkg在每一tag上的最大tfidf（dm_sdk_mapping.app_tag_system_mapping_par）；
         2.关联tag对应cat2（二级分类）,保留配置文件中的二级分类标签(若为tag则处理不用再cat2上汇总),
		   对Apppkg在cat2上进行sum(tfidf),过滤掉权重较小的App（dm_sdk_mapping.app_tag_system_mapping_par，
		   dm_sdk_mapping.tag_cat_mapping_dmp_par）；
		 3.提取每一用户近7天安装的pkg(dm_mobdi_topic.dws_device_install_app_re_status_di,final_flag =1),对
		   pkg进行渠道清理（dm_sdk_mapping.app_pkg_mapping_par，pkg->apppkg）;
         4.将第2和第3部结构进行关联，得到每一device安装的每一Apppkg以及Apppkg对应标签；
		 5.对每一device，tag上对apppkg进行求和，得出每个设备安装在各个标签中的app安装个数，
		   安装带有行业标签Apppkg总量；
输出结果：表名（根据传入参数$targetTable 确定）
         字段：device                      ----设备
		       不同标签的Apppkg安装个数    ----（配置文件中的读取）
			   total                       ----安装带有行业标签Apppkg总量
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
select all_tmp.device,
        ${param4},
	   count(*) as total
from
(SELECT device_install.device AS device, 
        ${param2},
       device_install.apppkg AS apppkg
FROM 
  (SELECT a.apppkg AS apppkg,
          b.cat2 AS cat
    FROM (select rank.apppkg as apppkg, rank.tag as tag,rank.norm_tfidf as norm_tfidf 
	       from (select n.apppkg as apppkg,n.tag as tag ,n.norm_tfidf as norm_tfidf , 
		           Row_number() over(partition by n.apppkg,n.tag ORDER BY  n.norm_tfidf DESC ) as rank 
            from $dim_app_tag_system_mapping_par n
			where version ='1000'
			) rank where rank.rank =1 ) a
    JOIN  (select cat2,tag from $dim_tag_cat_mapping_dmp_par
	         where version='1000'
	     ) b
        ON a.tag = b.tag
    GROUP BY  a.apppkg,b.cat2
    HAVING ${param3} ) app_install
  
  JOIN 
  
         ( SELECT NVL(app_pkg.apppkg,device_filter.pkg) AS apppkg,
                  device_filter.device AS device,
                  device_filter.day AS day
			FROM 
				(SELECT device.device AS device,
						device.pkg AS pkg,
						device.final_flag AS final_flag,
						device.day AS day
        
						FROM $dws_device_install_app_re_status_di device
						WHERE device.refine_final_flag=1
							AND device.day <=${date} AND device.day >${b7day} ) device_filter
					LEFT  JOIN (
					    select apppkg,pkg from $dim_app_pkg_mapping_par
						   where version='1000'
					   ) app_pkg
					ON app_pkg.pkg = device_filter.pkg
					GROUP BY  NVL(app_pkg.apppkg,device_filter.pkg), device_filter.device, device_filter.day ) device_install

     ON device_install.apppkg = app_install.apppkg  group by device_install.apppkg,device_install.device ) all_tmp group by all_tmp.device;
"
~/jdk1.8.0_45/bin/java -cp /home/dba/lib/mysql-utils-1.0-jar-with-dependencies.jar  com.mob.TagUpdateTime -d rp_mobdi_app -t rp_device_financial_install_profile
