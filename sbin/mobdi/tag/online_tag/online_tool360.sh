#!/bin/sh

set -x -e

day=$1
timewindow=$2
type=$3
computer_type=$4
test_flag=$5
pdays=`date -d "$day -$timewindow days" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive-env.sh

tmp_table="dm_mobdi_tmp.timewindow_tmp"
tmp_table_total="dm_mobdi_tmp.timewindow_total"
output_table="dm_mobdi_tmp.timewindow_online_profile"
device_install_app_master="dm_mobdi_master.device_install_app_master"
online_category_mapping_par_replace=dm_sdk_mapping.online_category_mapping_par_replace
app_pkg_mapping_par=dm_sdk_mapping.app_pkg_mapping_par
: '
inPutTable:
    dm_sdk_mapping.online_category_mapping_par_replace
    dw_mobdi_md.timewindow_tmp
    dw_mobdi_md.timewindow_total
    dm_sdk_mapping.app_pkg_mapping_par
    rp_mobdi_app.app_active_daily
    dm_mobdi_master.device_install_app_master
    rp_mobdi_app.app_active_monthly
    dm_mobdi_topic.dws_device_install_app_re_status_di
outPutTable:
    dw_mobdi_md.timewindow_online_profile
'

#增加分区处理$5
function data_exists2(){
  start=$1
  end=$2
  table=$3
  time_flag=$4
  partition=$5
  db=`echo ${table} |awk -F. '{print $1}'`
  tb=`echo ${table} |awk -F. '{print $2}'`

  if [ $partition ];then
    partition=${partition}"/"
  fi

  while [ ${start} -ne ${end} ]
  do

    if [ "$time_flag"x = "month"x ];then
      start=`date -d "${start}01 1 months" +%Y%m`
    else
      start=`date -d "${start} 1 days" +%Y%m%d`
    fi

    count=`hadoop fs -ls /user/hive/warehouse/${db}.db/${tb}/${partition}${time_flag}=${start}|wc -l`
    if [ ${count} -eq 0 ];then
      echo "${time_flag}=${start} partition of table ${table} is not found !!"
      return 1
    else
      echo ${start}" exist"
    fi
  done
}

#判断refine_final_flag条件,以及根据timewindow，筛选需要的cate_id
# 0为计算app个数 and refine_final_flag in (${refine_final_flag})
function get_flag_condition_and_cate_id(){
if [ ${type} -eq 0 ]; then
  flagCondition="refine_final_flag = 1"

  if [ ${timewindow} -eq 14 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6) or cate_id like '7019%'"
  elif [ ${timewindow} -eq 90 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6)"
  else
    cate_id_regx="1=1"
  fi
elif [ ${type} -eq 1 ]; then
  flagCondition="refine_final_flag = -1"

  cate_id_regx="1=1"
elif [ ${type} -eq 2 ]; then
  flagCondition="1=1"
  if [ ${timewindow} -eq 14 ];then
    cate_id_regx="cate_id like '7019%'"
  else
    cate_id_regx="1=1"
  fi
elif [ ${type} -eq 3 ]; then
  flagCondition="1=1"

  if [ ${timewindow} -eq 14 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6)"
  elif [ ${timewindow} -eq 90 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6,598) or cate_id like 'fin%'"
  else
    cate_id_regx="1=1"
  fi
elif [ ${type} -eq 4 ]; then
  flagCondition="refine_final_flag in (1,0,-1,2)"

  if [ ${timewindow} -eq 1 ];then
    cate_id_regx="cate_id =598 or cate_id like 'pay%'"
  elif [ ${timewindow} -eq 14 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6,598)"
  else
    cate_id_regx="1=1"
  fi
elif [ ${type} -eq 5 ]; then
  flagCondition="1=1"

  if [ ${timewindow} -eq 14 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6,598)"
  elif [ ${timewindow} -eq 30 ];then
    cate_id_regx="cate_id in (1,2,3,4,5,6,598)"
  else
    cate_id_regx="1=1"
  fi
elif [ ${type} -eq 6 ]; then
bday=`date -d "$day -1 days" "+%Y%m%d"`

hive -e "
SET hive.exec.compress.output=true;
SET mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;
set mapred.output.compression.type=BLOCK;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=100;
set hive.exec.compress.intermediate=true;
set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set hive.intermediate.compression.type=BLOCK;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapreduce.job.queuename=root.yarn_data_compliance1;

insert overwrite table ${output_table} partition(flag=${type},day=${day},timewindow=${timewindow})
SELECT NVL(now_rp.device,bef_rp.device) as device,NVL(now_rp.feature,bef_rp.feature) as feature,
       round ( (NVL(now_rp.cnt,0) - NVL(bef_rp.cnt,0)+1)/(NVL(bef_rp.cnt,0)+1),4 ) as cnt
from
(
  select time_online.device,concat(time_online.cate_id,'_','6','_',time_online.time_window) as feature,time_online.cnt
  from
  (
   select device,split(feature,'_')[0] as cate_id,split(feature,'_')[2] as time_window,cnt
   from ${output_table}
   where flag=3 and day=${day} and timewindow=${timewindow}
  )time_online
  inner join
  (
   select cate_id from ${online_category_mapping_par_replace} where type=${type} and version = ${mapping_version} group by cate_id
   union all
   select total as cate_id from ${online_category_mapping_par_replace} where type=${type} and version = ${mapping_version} and total <>'0' group by total
   union all
   select percent as cate_id from ${online_category_mapping_par_replace} where type=${type} and version = ${mapping_version} and percent <>'0' group by percent
  )mapping
  on time_online.cate_id = mapping.cate_id
) now_rp
full join
(
  select time_online2.device,concat(time_online2.cate_id,'_','6','_',time_online2.time_window) as feature,time_online2.cnt
  from
  (
   select device,split(feature,'_')[0] as cate_id,split(feature,'_')[2] as time_window,cnt
   from ${output_table}
   where flag=3 and day=${bday} and timewindow=${timewindow}
  )time_online2
  inner join
  (
   select cate_id from ${online_category_mapping_par_replace} where type=${type} and version = ${mapping_version} group by cate_id
   union all
   select total as cate_id from ${online_category_mapping_par_replace} where type=${type} and version = ${mapping_version} and total <>'0' group by total
"
 exit 0
elif [ $type -eq 10 ];then
  flagCondition="refine_final_flag = 1"
elif [ $type -eq 11 ];then
  flagCondition="refine_final_flag = -1"
else
  flagCondition="1=1"
  cate_id_regx="1=1"
fi
}
get_flag_condition_and_cate_id


##############
## @author:guanyt
## @time:2019-3-1
## @description: 1.对timewindow=360,180的支持
##               2.对新安装，新卸载的天数的支持，添加type
##


#由于有个mapping的关系，type要找到对应的map_type
function gen_map_type(){
   map_type=${type}
   if [ $type -eq 10 ];then
	   map_type=0
   elif [ $type -eq 11 ];then
	   map_type=1
   fi
}
gen_map_type

#tools

function prop(){
    props="
2_1=7001003,7001004,7001009,7001011,7001013,7002001,7002002,7002003,7002004,7002005,7002006,7002007,7002008,7002009,7005001,7005003,7005004,7005006,7005007,7005009,7005010,7016006,7002,7005
0_1=7001001,7002001,7002002,7002003,7002004,7002005,7002006,7002007,7002008,7002009,7005001,7005003,7005004,7005006,7005007,7005009,7005010,7016006,7005
1_1=7001001,7002001,7002002,7002003,7002004,7002005,7002006,7002007,7002008,7002009,7002
2_0=7001,7003,7004,7006,7008,7009,7011,7012,7013,7014,7015,7016,7019
0_0=7001,7003,7004,7005,7006,7007,7008,7011,7013,7014,7016
1_0=7001,7005,7011,7013
	"
	key=$1
    while read t;do grep "${key}=" $t|cut -d '=' -f2;done <<< "$props"
	#echo $props |grep "${key}="  | cut -d '=' -f2 | sed 's/\r//'
}

#timewindow处理的cate_id也不一样，所以加个修正
#根据type去不同的source_type下去取数据
function filter_cate_id_and_source(){
source_type=${type}
if [ $type -eq 3 ];then
	source_type=2
	computer_type=1
elif [ $type -eq 11 ];then
	source_type=1
	computer_type=1
elif [ $type -eq 10 ];then
	source_type=0
	computer_type=1
fi

if [[ $timewindow -eq 360 ]]||[[ $timewindow -eq 180 ]]||[[ $test_flag ]];then
	cate_id_list=`prop ${source_type}_${computer_type}`
	cate_id_regx="cate_id in (${cate_id_list})"
fi
}
filter_cate_id_and_source


echo "=================>start job============="
echo "day:"$day
echo "type:"$type
echo "cate_id:"$cate_id_regx
echo "flagCond:"$flagCondition
echo "compute_type:"$computer_type
echo "timewindow:"$timewindow
echo "map_type:"$map_type
echo "source_type:"$source_type

mapping_version=`hive -e"
                 set mapreduce.job.queuename=root.yarn_data_compliance1;
                 select max(version)
                 from ${online_category_mapping_par_replace}
                 where type = ${map_type}"`
#mapping_version=20181205

category_mapping="select relation,cate_id,total,percent
  from ${online_category_mapping_par_replace}
  where version=${mapping_version}
  and type=${map_type}
  and (${cate_id_regx})"

apppkg_mapping="
   select apppkg,pkg
   from ${app_pkg_mapping_par}
   where version='1000'
"

#description:生成中间表
function gen_tmp_table(){
    theday=$1
    is_force=$2
    #type定义inputTable,然后定义device表

    if [[ $type -eq 2 ]]||[[ $type -eq 3 ]];then
      #echo "计算活跃"
      inputTable=${app_active_daily}
      deviceSql="
           select device,relation,${theday} as day,count(1) as num
           from
           (
           select device,apppkg as relation from ${inputTable} where day=${theday}
           )t
           group by device,relation
      "

    elif [ $type -eq 4 ];then
      #好像是第40天加上前40天union，待修正todo
      inputTable="${dws_device_install_app_re_status_di}"
      deviceSql="
          select device,pkg as relation,${theday} as day,count(1) as num from ${inputTable}
          where day=${theday} and ${flagCondition}
          union all
          select device,pkg
          from ${device_install_app_master}
          where day=${beforeDate}
      "
    elif [[ $type -eq 0 ]]||[[ $type -eq 1 ]]||[[ $type -eq 10 ]]||[[ $type -eq 11 ]];then
      #echo "计算安装"
      inputTable="${dws_device_install_app_re_status_di}"
      deviceSql="
          select device,coalesce(mapping.apppkg, device.pkg) as relation,${theday} as day,count(1) as num
          from
          (
            select device,pkg from
            ${inputTable}
            where day=${theday} and ${flagCondition}
          )device
          left join
          (${apppkg_mapping}) mapping
          on device.pkg = mapping.pkg
          group by device,coalesce(mapping.apppkg, device.pkg)
      "
    elif [ $type -eq 5 ];then
      echo "计算累计活跃"
      #todo
      inputTable="${app_active_daily}"
      deviceSql="
           select device,relation,${theday} as day,count(1) as num
           from
           (
           select device,apppkg as relation from ${inputTable} where day=${theday}
           )t
           group by device,relation
      "
    elif [ $type -eq 6 ];then
      echo "计算slope"
      exit 0
    else
      echo "new type"
	  exit 1
    fi
    #如果分区存在，则不用生成，跳过
    yesterday=`date -d "${theday} -1 days" +%Y%m%d`
    data_exists2 ${yesterday} ${theday} ${tmp_table} day "type=${source_type}/*"
    if [[ $? -eq 0 ]]&&[[ '${is_force}' != '1' ]];then
          echo "exist"

          return

    else
        if [[ $timewindow -eq 180 ]]||[[ $timewindow -eq 360 ]];then
           if [ $type -ne 10 ];then
               echo "head step gen"
               return
           fi
        else
          echo "not exist"
        fi

    fi


    #如果数据源不存在，则直接中断程序,任务失败
    yesterday=`date -d "${theday} -1 days" +%Y%m%d`
    data_exists2 ${yesterday} ${theday} ${inputTable} day
    if [ $? -eq 0 ];then
      echo "..."
    elif [[ $type -eq 2 ]]||[[ $type -eq 3 ]];then
      inputTable=${app_active_daily}
      varMonth=`date -d "${theday}" "+%Y%m"`
      deviceSql="
           select device,relation,${theday} as day,count(1) as num
           from
           (
           select device, relation
           from
           (
           select device,apppkg as relation,day from ${app_active_monthly}
                           LATERAL VIEW explode(split(day_list,',')) t AS day where month=${varMonth}
           )m where day=${theday}
           )t
           group by device,relation
      "
    else
      echo "ERROR,exit,data source not exist:  "${inputTable}
	  exit 1
    fi

	echo "====>gen_tmp_table:"${theday}${inputTable}


      hive -v -e "
	  SET hive.merge.mapfiles=true;
	  SET hive.merge.mapredfiles=true;
	  set mapred.max.split.size=250000000;
	  set mapred.min.split.size.per.node=128000000;
	  set mapred.min.split.size.per.rack=128000000;
	  set hive.merge.smallfiles.avgsize=250000000;
	  set hive.merge.size.per.task = 250000000;
      set hive.exec.dynamic.partition=true;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.exec.max.dynamic.partitions.pernode=400;
      set mapreduce.job.queuename=root.yarn_data_compliance1;

      insert overwrite table ${tmp_table} partition(type=${source_type},cate_id,day)
      select device_info.device,device_info.relation,mapping.percent,mapping.total,device_info.num,mapping.cate_id,${theday} as day from
      (
        ${category_mapping}
      )mapping
      inner join
      (
           ${deviceSql}
      )device_info
      on device_info.relation = mapping.relation
      distribute by device sort by device
      ;
    "
}

function check_timewindow_tmp_table(){
  pdays=`date -d "$day -$timewindow days" +%Y%m%d`
}

function timewindow_gen_tmp_table(){
  pdays=`date -d "$day -$timewindow days" +%Y%m%d`

  start=`date -d "$day -$(($timewindow+1)) days" +%Y%m%d`
  end=$day
  echo -e "\n===>start timewindow "${start}"-->"${end}

  i=1
  while [ ${start} -le ${end} ]
  do

    for j in `seq 1 10`
    do
      start=`date -d "${start} 1 days" +%Y%m%d`
      if [ ${start} \> ${end} ];then
          echo "timewindow over"${start}_${end}
          return
      fi
      {
        echo "gen_tmp_table "${start}
        gen_tmp_table $start $inputTable
      }&
    done
	#本来是等待所有完成，现在要改成等待所有且都成功
    #wait
    sleep 1
	for((j=1;j<11;j++));
	do
		k=$(echo "$j"|bc -l)
		wait %$k
        state=$?
        if [ $state -ne 0 ];then
            echo "have empty source"
            exit 1
		fi
	done
    #循环次数限制
    if [ $i -gt $(($timewindow+3)) ];then
	  break
    else
	  i=$(($i+1))
    fi
  done
}

function filter_timewindow_cate_id(){
if [[ $type -eq 0 ]]&&[[ $computer_type -eq 0 ]]&&[[ $timewindow -eq 180 ]];then
   cate_id_regx="cate_id in (7001,7006 )"
elif [[ $type -eq 0 ]]&&[[ $computer_type -eq 0 ]]&&[[ $timewindow -eq 90 ]];then
   cate_id_regx="cate_id in (7001)"
elif [[ $type -eq 0 ]]&&[[ $computer_type -eq 0 ]]&&[[ $timewindow -eq 60 ]];then
   cate_id_regx="cate_id in (7001)"
elif [[ $type -eq 0 ]]&&[[ $computer_type -eq 0 ]]&&[[ $timewindow -eq 7 ]];then
   cate_id_regx="cate_id in (7001)"
elif [[ $type -eq 1 ]]&&[[ $computer_type -eq 0 ]]&&[[ $timewindow -eq 180 ]];then
   cate_id_regx="cate_id in (7001)"
elif [[ $type -eq 1 ]]&&[[ $computer_type -eq 0 ]]&&[[ $timewindow -eq 90 ]];then
   cate_id_regx="cate_id in (7001)"
elif [[ $type -eq 1 ]]&&[[ $computer_type -eq 0 ]]&&[[ $timewindow -eq 60 ]];then
   cate_id_regx="cate_id in (7001)"
elif [[ $type -eq 2 ]]&&[[ $computer_type -eq 0 ]]&&[[ $timewindow -eq 60 ]];then
   cate_id_regx="cate_id in (7015)"
elif [[ $type -eq 3 ]]&&[[ $computer_type -eq 1 ]]&&[[ $timewindow -eq 90 ]];then
   cate_id_regx="cate_id in ('7002004','7002007','7002008')"
else
   echo "no small reduce cate_id"
fi
}


#根据compute_type来生成不同的timewindow_total
function gen_timewindow_total(){
    #减少标签计算的cate_id
    filter_timewindow_cate_id

    export HADOOP_HEAPSIZE=8192
	export HADOOP_CLIENT_OPTS="-Xmx8g"
    if [ ${computer_type} -eq 0 ];then
        #device,pkg,cate_id三元组,relation字段存pkg

        #type=4计算在装需要特别处理
        if [ $type -eq 4 ];then

          hive -v -e "
          set mapreduce.job.queuename=root.yarn_data_compliance1;

          insert overwrite table ${tmp_table_total} partition(type=${type},timewindow=${timewindow},day=${day})
          select device,relation,percent,total,cate_id,sum(num) as num
          from
          (
            select device,relation,percent,total,cate_id,sum(num) as num
            from ${tmp_table}
            where type=${type} and ${cate_id_regx} and day<=${day} and day>${pdays}
            union all

            select device_info.device,device_info.relation,mapping.percent,mapping.total,device_info.num,mapping.cate_id,${theday} as day from
            (
               ${category_mapping}
            )mapping
            inner join
            (
              select device,pkg
              from ${device_install_app_master}
              where day=${pdays}
            )device_info
            on device_info.relation = mapping.relation
          )

          group by device,relation,percent,total,cate_id
          "
          return
        fi

        echo "计算个数"
        echo $HADOOP_HEAPSIZE
        {
        hive -v -e "
        SET mapreduce.map.memory.mb=4096;
        set mapreduce.map.java.opts='-Xmx4096M';
        set mapreduce.child.map.java.opts='-Xmx4096M';
        set mapreduce.job.queuename=root.yarn_data_compliance1;
        insert overwrite table ${tmp_table_total} partition(type=${type},timewindow=${timewindow},day=${day})
        select device,relation,percent,total,cate_id,sum(num) as num
        from ${tmp_table}
        where type=${source_type} and ${cate_id_regx} and day<=${day} and day>${pdays}
        group by device,relation,percent,total,cate_id
        "
	    }

    elif [ ${computer_type} -eq 1 ];then
        #计算天数
        #device,day,cate_id三元组,用relation字段存day
        echo "计算天数"
        hive -v -e "
        SET mapreduce.map.memory.mb=4096;
        set mapreduce.map.java.opts='-Xmx4096M';
        set mapreduce.child.map.java.opts='-Xmx4096M';
        set mapreduce.job.queuename=root.yarn_data_compliance1;

        insert overwrite table ${tmp_table_total} partition(type=${type},timewindow=${timewindow},day=${day})
        select device,day,percent,total,cate_id,count(1) as num
        from ${tmp_table}
        where type=${source_type} and ${cate_id_regx} and day<=${day} and day>${pdays}
        group by device,day,percent,total,cate_id
        "

    else
        echo "unknown compute_type,exit"
        exit 1
    fi

    if [ $? -ne 0 ];then
	  echo "gen_timewindow_total fail,exit"
	  exit 1
    fi
}

function gen_timewindow_total2(){
    spark2-submit --master yarn \
	--driver-memory 9G \
	--executor-memory 9G \
	--executor-cores 3 \
	--queue root.yarn_data_compliance1 \
        --conf spark.network.timeout=300 \
	--conf spark.default.parallelism=2000 \
	--conf spark.sql.shuffle.partitions=3000 \
	--conf spark.yarn.executor.memoryOverhead=2048 \
	--conf spark.shuffle.service.enabled=true \
	--conf spark.dynamicAllocation.enabled=true \
	--conf spark.dynamicAllocation.minExecutors=20 \
	--conf spark.dynamicAllocation.maxExecutors=70 \
	--conf spark.dynamicAllocation.initialExecutors=20 \
	--conf spark.dynamicAllocation.executorIdleTimeout=15s \
	--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
	--conf spark.kryoserializer.buffer.max=1024 \
	--conf spark.driver.maxResultSize=4g \
	--conf spark.sql.autoBroadcastJoinThreshold=104857600 \
	--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
	--driver-java-options "-XX:MaxPermSize=1024m" \
	online_tool.py $day $timewindow $type $computer_type $pdays
}
#滑动窗口方法，待完成
function iter_gen_timewindow_total(){

  pdays=`date -d "$day -$timewindow days" +%Y%m%d`
  yesterday=`date -d "$day -1 days" +%Y%m%d`
  beforeyesterday=`date -d "$day -2 days" +%Y%m%d`

  #check yesterday total
  data_exists2 ${beforeyesterday} ${yesterday} ${tmp_table_total} day "type=${type}/timewindow=${timewindow}"
  if [ $? -ne 0 ];then
      echo "yesterday timewindow total not exist:  "${tmp_table_total}
      gen_timewindow_total
	  return
  fi

  #滑动窗口，用最近N天的total，减去第前N天的device,pkg,然后加上当前日期的device,pkg
  echo "start window iter"
  if [ ${computer_type} -eq 0 ];then
    hive -v -e "
    SET mapreduce.map.memory.mb=4096;
    set mapreduce.map.java.opts='-Xmx4096M';
    set mapreduce.child.map.java.opts='-Xmx4096M';
    set mapreduce.job.queuename=root.yarn_data_compliance1;

    insert overwrite table ${tmp_table_total} partition(type=${type},timewindow=${timewindow},day=${day})
    select device,relation,percent,total,cate_id,sum(num) as num from
    (
      select device,relation,percent,total,-1*num as num,type,cate_id from ${tmp_table} where type=${type} and ${cate_id_regx} and day=${pdays}
      union all
      select device,relation,percent,total,num,type,cate_id  from ${tmp_table_total} where type=${type} and timewindow=${timewindow} and day=${yesterday}
      union all
      select device,relation,percent,total,num,type,cate_id from ${tmp_table} where type=${type} and ${cate_id_regx} and day=${day}
    ) iteration
    group by device,relation,percent,total,cate_id having sum(num)>0
    cluster by device
    "
  elif [ ${computer_type} -eq 1 ];then
    hive -v -e "
    SET mapreduce.map.memory.mb=4096;
    set mapreduce.map.java.opts='-Xmx4096M';
    set mapreduce.child.map.java.opts='-Xmx4096M';
    set mapreduce.job.queuename=root.yarn_data_compliance1;

    insert overwrite table ${tmp_table_total} partition(type=${type},timewindow=${timewindow},day=${day})
    select device,relation,percent,total,cate_id,sum(num) as num from
    (
      select device,day as relation,percent,total,-1 as num,type,cate_id from ${tmp_table} where type=${type} and ${cate_id_regx} and day=${pdays}
      union all
      select device,relation,percent,total,num,type,cate_id  from ${tmp_table_total} where type=${type} and timewindow=${timewindow} and day=${yesterday}
      union all
      select device,day as relation,percent,total,1 as num,type,cate_id from ${tmp_table} where type=${type} and ${cate_id_regx} and day=${day}
    )iteration
    group by device,relation,percent,total,cate_id having sum(num)>0
    cluster by device
    "
  else
     echo "unknown computer_type"
     exit 1
  fi
}


function gen_timewindow_profile(){

  if [ ${computer_type} -eq 0 ]; then
    hive -v -e "
    SET hive.merge.mapfiles=true;
    SET hive.merge.mapredfiles=true;
    set mapred.max.split.size=250000000;
    set mapred.min.split.size.per.node=128000000;
    set mapred.min.split.size.per.rack=128000000;
    set hive.merge.smallfiles.avgsize=250000000;
    set hive.merge.size.per.task = 250000000;
    set mapreduce.job.queuename=root.yarn_data_compliance1;

    insert overwrite table ${output_table} partition(flag=${type},day='${day}',timewindow=${timewindow})
    select device,concat(cate_id,'_',${type},'_',${timewindow}) as feature,
    count(1) as cnt
    from ${tmp_table_total}
    where type=${type} and timewindow=${timewindow} and day=${day}
    group by device,concat(cate_id,'_',${type},'_',${timewindow})
    --算total
    union all
    select device,
           concat(total,'_',${type},'_',${timewindow}) as feature,
           count(1) as cnt
    from ${tmp_table_total}
    where total != '0' and type=${type} and timewindow=${timewindow} and day=${day}
    group by device,concat(total,'_',${type},'_',${timewindow})
    --算percent
    union all
    select percent.device,
           feature,
           cast(round(percent.cnt/all.count,6) as String) as cnt
    from
    (
       select device,
            concat(percent,'_',${type},'_',${timewindow}) as feature,
            count(1) as cnt
       from ${tmp_table_total}
       where type=${type} and timewindow=${timewindow} and day=${day} and percent != '0'
       group by device,concat(percent,'_',${type},'_',${timewindow})
    ) percent
    left join
    (
       select device,count(1) as count
       from ${tmp_table_total}
       where type=${type} and timewindow=${timewindow} and day=${day}
       group by device
    ) all on all.device = percent.device;
  "
  else

   hive -v -e "
   SET hive.merge.mapfiles=true;
   SET hive.merge.mapredfiles=true;
   set mapred.max.split.size=250000000;
   set mapred.min.split.size.per.node=128000000;
   set mapred.min.split.size.per.rack=128000000;
   set hive.merge.smallfiles.avgsize=250000000;
   set hive.merge.size.per.task = 250000000;
   set mapreduce.job.queuename=root.yarn_data_compliance1;

    insert overwrite table ${output_table} partition(flag=${type},day='${day}',timewindow=${timewindow})
    select device,feature,count(1) as cnt
    from
    (
      select device,concat(cate_id,'_',${type},'_',${timewindow}) as feature,relation
      from ${tmp_table_total} where type=${type} and timewindow=${timewindow} and day=${day}
      group by device,concat(cate_id,'_',${type},'_',${timewindow}),relation
    )t group by device,feature
    --算total
    union all
    select device,feature,count(1) as cnt
    from
    (
      select device,
           concat(total,'_',${type},'_',${timewindow}) as feature,
           relation
      from ${tmp_table_total}
      where type=${type} and timewindow=${timewindow} and day=${day} and  total != '0'
      group by device,concat(total,'_',${type},'_',${timewindow}),relation
    )t group by device,feature
    --算percent
    union all
    select percent.device,
           feature,
           cast(round(percent.cnt/all.count,6) as String) as cnt
    from
    (
      select device,feature,count(1) as cnt
      from
      (
        select device,
            concat(percent,'_',${type},'_',${timewindow}) as feature,
            relation
       from ${tmp_table_total}
       where percent != '0' and  type=${type} and timewindow=${timewindow} and day=${day}
       group by device,concat(percent,'_',${type},'_',${timewindow}),relation
      )t group by device,feature
    ) percent
    left join
    (
       select device,count(1) as count
       from
       ( select device,relation
         from ${tmp_table_total}
         where type=${type} and timewindow=${timewindow} and day=${day}
         group by device,relation
      )t
      group by device
    ) all on all.device = percent.device;

   "
  fi
}
gen_tmp_table $day 1
#check_timewindow_tmp_table
timewindow_gen_tmp_table
#gen_timewindow_total
gen_timewindow_total
#gen_timewindow_total2
gen_timewindow_profile


:<<!
spark2-submit --master yarn --deploy-mode client \
--class com.mob.Online \
--driver-memory 12G \
--executor-memory 9G \
--executor-cores 3 \
--conf spark.network.timeout=300 \
--conf spark.default.parallelism=2000 \
--conf spark.sql.shuffle.partitions=3000 \
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=20 \
--conf spark.dynamicAllocation.maxExecutors=70 \
--conf spark.dynamicAllocation.initialExecutors=20 \
--conf spark.dynamicAllocation.executorIdleTimeout=15s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.driver.maxResultSize=4g \
--conf spark.sql.autoBroadcastJoinThreshold=104857600 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--driver-java-options "-XX:MaxPermSize=1024m" \
/home/dba/lib/OnlineUniversalTool-v0.1.0-jar-with-dependencies.jar \
"
{
	\"category_mapping\":\"${category_mapping}\",
	\"apppkg_mapping\":\"${apppkg_mapping}\",
  \"source_sql\":\"${source_sql}\",
  \"day\":\"${day}\",
  \"type\":\"${type}\",
  \"timewindow\":\"${timewindow}\",
  \"computer_type\":\"${computer_type}\",
  \"output_table\":\"${output_table}\"
}
"
!

day3=`date -d "$day -3 days" +%Y%m%d`
hive -e "alter table  ${tmp_table_total} drop partition(day=$day3);"
