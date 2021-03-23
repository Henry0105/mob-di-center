#! /bin/sh 

 : '
@owner:xdzhang
@describe:行业标签-App在活跃标签(根据传入的第二个参数$2统计对应天数的各个设备中标签的活跃天数)
@projectName:MobDI 
@BusinessName:profile_online
@SourceTable:rp_mobdi_app.rp_device_active_label_profile,dm_sdk_mapping.tag_cat_mapping_dmp_par
@TargetTable:rp_mobdi_app.rp_device_financial_active_week_profile,rp_mobdi_app.rp_device_financial_active_month_profile
@TableRelation:rp_mobdi_app.rp_device_active_label_profile,dm_sdk_mapping.tag_cat_mapping_dmp_par->rp_mobdi_app.rp_device_financial_active_week_profile|rp_mobdi_app.rp_device_active_label_profile,dm_sdk_mapping.tag_cat_mapping_dmp_par->rp_mobdi_app.rp_device_financial_active_month_profile
'
set -x -e

 if [ $# -lt 3 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: '<date>,<days>,<outputtable>'"
    exit 1
fi
  
: '
@parameters
@date1:传入日期参数，为脚本运行日期（重跑不同）
@days:参数决定统计多少天的数据，一般为7天或者30天
@TargetTable:结果表名称（不带库名）
 '
date1=$1
#计算数据开始的分区，date1-3（t-3）
day=`date -d "$date1" "+%Y%m%d"`
days=$2
#计算数据结束分区，day-$days
before_day=`date -d "$day -$days days" "+%Y%m%d"`
TargetTable=$3

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#databases
appdb=dm_mobdi_report

#input
#rp_device_active_label_profile=dm_mobdi_report.rp_device_active_label_profile

#mapping
#tag_cat_mapping_dmp_par=dim_sdk_mapping.tag_cat_mapping_dmp_par

: '
@part_1:读config.conf配置文件，拼接所输出表的列名,查询条件等
配置文件示例：借贷=borrowing=2.73656580503609，
                    ----借贷 为tag的二级分类（dm_sdk_mapping.tag_cat_mapping_dmp中cat2字段）；
                    ----borrowing 为输出表列名；
                    ----2.73656580503609 为app在借贷类标签的总权重最小值，低于此值的App不列入计算；
功能：根据配置文件内容，拼接sql
       param        ----输出字段列表 
       param1       ----输出字段列表
       param2       ----sql查询条件
	   total        ----行业标签汇总值计算方式
'

cd `dirname $0`
FILENAME=./config.conf

int=0;
totalN=0
while read LINE
do

   LD_IFS="$IFS";
   IFS="=";
   arr=($LINE);
   IFS="$OLD_IFS";
   if [ ${arr[0]} != '信用卡' ]; then
     if [ ${int} = 0 ];then
        total="max(case WHEN active_tag.cat='${arr[0]}' then 1 else 0 END)"
        let totalN+=1
        param2="dmp.cat2 ='${arr[0]}'"
     else
      total="${total}+max(case WHEN active_tag.cat='${arr[0]}' then 1 else 0 END)"
      let totalN+=1
      param2="${param2} OR dmp.cat2 ='${arr[0]}'"
     fi
	param="${param},SUM(device_day_count.${arr[1]}) AS  ${arr[1]}"
        param1="${param1} ,sum(CASE active_tag.cat WHEN '${arr[0]}' THEN 1 ELSE 0 END) AS ${arr[1]}"
   fi
   int=1
done <$FILENAME

: '
 @part_2:
 实现功能：根据传入的第二个参数$2统计对应天数的各个设备中标签的活跃天数
 实现逻辑：1.根据表dm_sdk_mapping.tag_cat_mapping_dmp找到符合配置文件中的标签
       
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

insert overwrite table ${appdb}.${TargetTable} partition(day=${day})
select device_day_count.device ${param},
       count(*) AS total
FROM
(select active_tag.device as device,
        active_tag.day as day ${param1}
    from
(SELECT 
         device_active.device as device,
         tag_dmp.cat2 as cat,
         device_active.day as day
FROM 
  (SELECT device_profile.device as device,
          device_profile.day as day,
          device_profile.tag as tag
          FROM $rp_device_active_label_profile device_profile
          WHERE device_profile.day >${before_day}
          and device_profile.day <=${day}) device_active
   JOIN 
   (select dmp.cat2,dmp.tag_id from $tag_cat_mapping_dmp_par dmp where dmp.version ='1000' and ${param2}) tag_dmp
   ON tag_dmp.tag_id = device_active.tag
   group by tag_dmp.cat2,device_active.device ,device_active.day) active_tag group by active_tag.device,active_tag.day) device_day_count group by device_day_count.device;
"
~/jdk1.8.0_45/bin/java -cp /home/dba/lib/mysql-utils-1.0-jar-with-dependencies.jar  com.mob.TagUpdateTime -d rp_mobdi_app -t ${TargetTable}
