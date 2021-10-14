#!/bin/sh

: '
@owner:xdzhang
@describe:行业标签-App标签slope(根据（近七天标签的活跃次数-前一日近七天标签的活跃次数+1）/(前一日近七天标签的活跃次数+1)计算)
@projectName:MobDI 
@BusinessName:profile_online
@SourceTable:rp_mobdi_app.rp_device_financial_active_week_profile
@TargetTable:rp_mobdi_app.rp_device_financial_slope_week_profile
@TableRelation:rp_mobdi_app.rp_device_financial_active_week_profile->rp_mobdi_app.rp_device_financial_slope_week_profile
'
set -x

 if [ $# -lt 3 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: '<date>,<sourcetable>,<outputtable>'"
    exit 1
  fi

: '
@parameters
@date1:传入日期参数，为脚本运行日期（重跑不同）
@source:源表表名称（不带库名）
@outtable:结果表名称（不带库名）
@re_run:是否重跑，0-不重跑  1-重跑
'

date1=$1
#计算数据分区，date1-3 （t-3）
day=`date -d "$date1" "+%Y%m%d"`
source=$2
outtable=$3
#re_run=$4

#databases
appdb=$dm_mobdi_report

: '
@part_1:读config.conf配置文件，拼接所输出表的列名、标签权重。
配置文件示例：借贷=borrowing=2.73656580503609，
                    ----借贷 为tag的二级分类（dm_sdk_mapping.tag_cat_mapping_dmp中cat2字段）；
                    ----borrowing 为输出表列名；
                    ----2.73656580503609 为app在借贷类标签的总权重最小值，低于此值的App不列入计算；
功能：根据配置文件内容，拼接sql
                param1        ----输出字段列表 
				param2        ----输出字段列表 
                param3        ----输出字段列表 
'
cd `dirname $0`
FILENAME=./config.conf

while read LINE
do

   LD_IFS="$IFS";
   IFS="=";
   arr=($LINE);
   IFS="$OLD_IFS";
   if [ ${arr[0]} != '信用卡' ]; then
       param1="${param1},(NVL(now_rp.${arr[1]},0) - NVL(bef_rp.${arr[1]},0) +1)/(NVL(bef_rp.${arr[1]},0)+1) as ${arr[1]}"
       param2="${param2},new.${arr[1]} as ${arr[1]}"
       param3="${param3},bef.${arr[1]} as ${arr[1]}"
   fi
done <$FILENAME

: '
 @part_2:
 实现功能：对表$source中的活跃数据选择需要计算的那天($day)的分区数据和前一天的数据($bday)，分区不存在调用脚本生成数据。
 
 实现逻辑：1.计算出表$source中保存的需要计算的那天（$day）和前一天所需的分区日期（$bday）
           2.查询外部表$source中是否有这两个分区的数据
           3，如果分区没有数据那么调用脚本 sdk_professionLabel_active_profile.sh统计出数据
 输出结果：表名（根据传入参数$source确定）
'
bday=`date -d "$day -1 days" "+%Y%m%d"`
p_bday=`date -d "$date1 -1 days" "+%Y%m%d"`
   hdfs dfs -test -e /user/hive/warehouse/rp_mobdi_app.db/$source/day=$day
   a=$?   
   if [ ${a} -ne 0 ] ;then    
     sh ./sdk_professionLabel_active_profile.sh ${date1} 7 $source
   fi 
  
   hdfs dfs -test -e /user/hive/warehouse/rp_mobdi_app.db/$source/day=$bday
   b=$?
   if [ ${b} -ne 0 ] ;then
     sh ./sdk_professionLabel_active_profile.sh ${p_bday} 7 $source
   fi

: '
  @part_2:
  实现功能: 根据源表$source 计算出每个在device在各个标签的slope（根据公式（近七天标签的活跃次数-前一日近七天标签的活跃次数+1）/(前一日近七天标签的活跃次数+1) 计算）。

  实现逻辑：1.在表$source中取出需要计算的那天（$day）和前一天的分区（$bday）数据
            2.将两个分区的数据进行full join
            3，根据device进行分组，然后用公式对每个标签进行统计
  输出结果：表名（根据传入参数$outtable确定）
'
hive -e "SET hive.exec.compress.output=true;
         SET mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;
         set mapred.output.compression.type=BLOCK;
         set hive.exec.parallel=true;
         set hive.exec.parallel.thread.number=100;
         set hive.exec.compress.intermediate=true;
         set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
         set hive.intermediate.compression.type=BLOCK;   

insert overwrite table ${appdb}.${outtable} partition(day=${day})
SELECT NVL(now_rp.device,bef_rp.device) as device ${param1},
       (NVL(now_rp.total,0) - NVL(bef_rp.total,0)+1)/(NVL(bef_rp.total,0)+1) as total
 from
    (select new.device as device ${param2},
            new.total as total  
     FROM ${appdb}.${source} new
        WHERE new.day =${day}) now_rp
 full join 
   (select bef.device as device ${param3},bef.total as total FROM ${appdb}.${source} bef
       WHERE bef.day =${bday})bef_rp
  on bef_rp.device = now_rp.device
"
~/jdk1.8.0_45/bin/java -cp /home/dba/lib/mysql-utils-1.0-jar-with-dependencies.jar  com.mob.TagUpdateTime -d rp_mobdi_app -t rp_device_financial_slope_week_profile