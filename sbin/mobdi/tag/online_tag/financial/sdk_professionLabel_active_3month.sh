#! /bin/sh 

: '
@owner:xdzhang
@describe:行业标签-App在活跃标签(根据$source源表中每个分区近30天活跃天数计算，选取三天汇总成90天的数据)
@projectName:MobDI 
@BusinessName:profile_online
@SourceTable:rp_mobdi_app.rp_device_financial_active_month_profile
@TargetTable:rp_mobdi_app.rp_device_financial_active_3month_profile
@TableRelation:rp_mobdi_app.rp_device_financial_active_month_profile->rp_mobdi_app.rp_device_financial_active_3month_profile
'
set -x 
: '
@parameters
@date1:传入日期参数，为脚本运行日期（重跑不同）
@source:源表名称（不带库名）
@outtable:结果表名称（不带库名）
@re_run:是否重跑标志   0-不重跑 1-重跑
'
 if [ $# -lt 3 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: '<date>,<source>,<outtable>'"
    exit 1
  fi

date1=$1
#计算数据分区，date1-3（t-3）
day=`date -d "$date1" "+%Y%m%d"`
source=$2
outtable=$3
#re_run=$4

#databases
appdb=dm_mobdi_report

: '
@part_1:读config.conf配置文件，拼接所输出表的列名
配置文件示例：借贷=borrowing=2.73656580503609，
                    ----借贷 为tag的二级分类（dm_sdk_mapping.tag_cat_mapping_dmp中cat2字段）；
                    ----borrowing 为输出表列名；
                    ----2.73656580503609 为app在借贷类标签的总权重最小值，低于此值的App不列入计算；
功能：根据配置文件内容，拼接sql
                param1        ----输出字段列表 
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
       param1="${param1},SUM(rp.${arr[1]}) as ${arr[1]}"
   fi
done <$FILENAME

: '
@part_2:
实现功能：对表$source中的活跃数据选择三天的数据，分区不存在调用脚本生成数据。

实现逻辑：1.计算出表$source中保存近90天所需的分区日期（day,b30day,b90day）
          2.查询外部表$source中是否有这三个分区的数据
		  3，如果分区没有数据那么调用rp_device_financial_active_profile.sh脚本 统计出数据
输出结果：表名（根据传入参数$source确定）
'
   #计算30天前的数据分区，day-30
   b30day=`date -d "$day -30 days" "+%Y%m%d"`
	 p_b30day=`date -d "$date1 -30 days" "+%Y%m%d"`
   #计算90天前的数据分区，b30day-30   
   b90day=`date -d "$b30day -30 days" "+%Y%m%d"`
   p_b90day=`date -d "$p_b30day -30 days" "+%Y%m%d"`  
   hdfs dfs -test -e /user/hive/warehouse/rp_mobdi_app.db/$source/day=$day
   a=$?   
   if [ ${a} -ne 0 ] ;then    
      sh ./sdk_professionLabel_active_profile.sh ${date1} 30 $source
   fi 
  
   hdfs dfs -test -e /user/hive/warehouse/rp_mobdi_app.db/$source/day=$b30day
   b=$?
   if [ ${b} -ne 0 ] ;then
      sh ./sdk_professionLabel_active_profile.sh ${p_b30day} 30 $source
   fi
 
   hdfs dfs -test -e /user/hive/warehouse/rp_mobdi_app.db/$source/day=$b90day
   c=$?
   if [ ${c} -ne 0 ] ;then
      sh ./sdk_professionLabel_active_profile.sh ${p_b90day} 30 $source
   fi
 
 : '
 @part_3:
 实现功能：对表$source中的活跃数据选择三天聚合成90天的数据。 
 实现逻辑：1.选择源表（$source）day，b30day和b90day的分区数据          
           2.汇总三个分区的数据，将每个标签获取天数进行相加，得到90天的活跃天数
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

insert overwrite table rp_mobdi_app.${outtable} partition(day=${day})
SELECT rp.device ${param1},
       SUM(rp.total) as total
FROM ${appdb}.${source} rp
WHERE rp.day =${day}
OR rp.day = ${b30day}
OR rp.day = ${b90day}
group by rp.device;
"
~/jdk1.8.0_45/bin/java -cp /home/dba/lib/mysql-utils-1.0-jar-with-dependencies.jar  com.mob.TagUpdateTime -d rp_mobdi_app -t ${outtable}