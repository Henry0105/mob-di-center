#!/bin/bash

###参数详解
# -p, --partition <value>   必须参数,结果表分区字段day的值和读取源数据分区范围的上界 示例: -p 20180928或者-partition 20180928
# -n, --filenum <value>     可选参数(默认200，若小于200或者不传将使用默认值）,当前分区结果生成文件个数 ,示例: -n 300 或者 --file_num 300
# -f, --fields <value>     （必须参数） 需要计算的标签名称和重命名（仅允许添加一个在源表中不存在的字段total,用于计算device活跃天数）和分隔符(只支持单字符分隔符。
#                           在碰到中英文的逗号的任意一种，均按照中英文各切分一次)，示例：-f [a:a1,b:b1:,] 或者 --fields [a:a1,b:b1:,]    
#                           ,其中a,b为原标签在feature字段中的名称，并且b字段按照分隔符逗号（中英文）切分
#                           a1，b1为a，b在feautre字段中的重命名名称
# -t, --lbstype <value>     (必须参数）代表业务代码, 示例 :-t 1 或者 --lbstype 1
# -w, --windowtime <value>  (必须参数,支持[1-1000]之间的整数）计算的时间窗口 ,示例 -w 7 或者 --window_time 7
# -s, --sourcetable <value> (必须参数） 数据源表,示例 -s test.table1 或者 --source_table test.table1
# -g, --targetable <value>  (必须参数） 结果表 ， 示例 -g test.table2 或者 --taget_table test.table2

set -x -e

source /home/dba/mobdi_center/conf/hive-env.sh

#input
#dm_mobdi_topic.dws_device_lbs_poi_android_sec_di
#out
#dm_mobdi_report.timewindow_offline_profile_v2

day=$1
timewindow=$2

/usr/bin/spark2-submit  --executor-memory 15G   \
    --master yarn   \
	--executor-cores 4      \
	--driver-cores 3  \
    --name cateringLbs  \
    --deploy-mode cluster \
    --class com.mobsec.feature.OfflineUniversalToolsSec \
	--driver-memory 10G      \
	--conf "spark.default.parallelism=11001" \
    --conf "spark.dynamicAllocation.executorIdleTimeout=300"   \
	--conf "spark.shuffle.file.buffer=16k"  \
	--conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45"     \
	--conf "spark.dynamicAllocation.minExecutors=20" \
	--conf "spark.dynamicAllocation.maxExecutors=30" \
    --conf "spark.speculation.quantile=0.95"        \
	--conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45"   \
	--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:+UseG1GC "     \
	--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC "     \
	--conf "spark.speculation=true" \
	--conf "spark.rpc.askTimeout=400" \
    --conf "spark.shuffle.service.enabled=true"     \
    --conf "spark.sql.shuffle.partitions=11001" \
	 /home/dba/mobdi_center/lib/offlineLabel-v0.1.0-jar-with-dependencies.jar \
	--partition ${day} \
	--filenum 8000 \
	--fields [name:name,price:price,brand:brand,zone:zone,type:type:,,total:total] \
	--lbstype 3 \
	--pk device \
	--windowtime ${timewindow} \
	--sourcetable $dws_device_lbs_poi_android_sec_di \
	--targetable $timewindow_offline_profile_v2

# 合并小文件
hive -e"
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $timewindow_offline_profile_v2 partition(flag=3,day=$day,timewindow=$timewindow)
select device,feature,cnt from $timewindow_offline_profile_v2 where flag=3 and day=$day and timewindow=$timewindow
"

    #./offlineLabel-1.0-SNAPSHOT-jar-with-dependencies.jar -p 20180927 -n 2 -f [brand:brand1,taste:taste1,type1:category1,type2:category2,time:time1,total:total1] -t 1 -w 7 -s dm_mobdi_master.device_catering_dinein_detail -g test.timewindow_offline_profile
