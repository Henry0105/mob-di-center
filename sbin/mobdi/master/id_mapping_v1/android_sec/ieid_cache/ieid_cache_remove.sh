#!/bin/bash

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

day=$1
p1day=`date -d "$day -1 days" +%Y%m%d`

# input
dim_ieid_buffer_blacklist_sec=dim_mobdi_mapping.dim_ieid_buffer_blacklist_sec

# mid
ieid_device_every_day_total_sec=dm_mobdi_tmp.ieid_device_every_day_total_sec

# output (自依赖)
dim_id_mapping_android_sec_df=dim_mobdi_mapping.dim_id_mapping_android_sec_df

# view
dim_id_mapping_android_sec_df_view=dim_mobdi_mapping.dim_id_mapping_android_sec_df_view


full_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dim_mobdi_mapping', 'dim_id_mapping_android_sec_df', 'version');
drop temporary function GET_LAST_PARTITION;
"
full_last_version=(`hive -e "$full_partition_sql"`)


HADOOP_USER_NAME=dba hive -v -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function cache_remove as 'com.youzu.mob.java.udf.CacheRemove';

set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=15360;
set mapreduce.map.java.opts='-Xmx12288m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx12288m';
set mapreduce.reduce.memory.mb=15360;
set mapreduce.reduce.java.opts='-Xmx12288m' -XX:+UseG1GC;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;


drop table if exists $ieid_device_every_day_total_sec;
create table $ieid_device_every_day_total_sec stored as orc as
select device,concat_ws(',',collect_set(ieid)) as ieids
from
(select device,ieid from  $dim_ieid_buffer_blacklist_sec where day='${day}' group by device,ieid) t1
group by device;


insert overwrite table $dim_id_mapping_android_sec_df partition(version='${day}.1001')
select
t1.device,mcid,mcid_tm,mcid_ltm,
nvl(if(t2.ieids is not null,split(cache_remove(t1.ieid,ieid_tm,ieid_ltm,t2.ieids),'_')[0],ieid),'') as ieid,
nvl(if(t2.ieids is not null,split(cache_remove(t1.ieid,ieid_tm,ieid_ltm,t2.ieids),'_')[1],ieid_tm),'') as ieid_tm,
nvl(if(t2.ieids is not null,split(cache_remove(t1.ieid,ieid_tm,ieid_ltm,t2.ieids),'_')[2],ieid_ltm),'') as ieid_ltm,
snid,snid_tm,snid_ltm,isid,isid_tm,isid_ltm,pid,pid_tm,pid_ltm,oiid,oiid_tm,oiid_ltm,
mcid_abnormal_tm,ieid_abnormal_tm,snid_abnormal_tm,isid_abnormal_tm,pid_abnormal_tm,oiid_abnormal_tm
from
(select * from $dim_id_mapping_android_sec_df where version='${day}.1000') t1
left join
$ieid_device_every_day_total_sec t2
on t1.device=t2.device;
"

# qc 数据条数
function qc_id_mapping(){

  cd `dirname $0`
  sh /home/dba/mobdi/qc/real_time_mobdi_qc/qc_id_mapping_view.sh  "$dim_id_mapping_android_sec_df" "${day}.1001" "$dim_id_mapping_android_sec_df_view" || qc_fail_flag=1
  if [[ ${qc_fail_flag} -eq 1 ]]; then
    echo 'qc失败，阻止生成view'
    exit 1
  fi
  echo "qc_id_mapping success!"
}

qc_id_mapping $day

# 检查新分区磁盘波动
CHECK_DATA()
{
  local insert_path=$1
  local p1day_path=$2
  # 检查路径上的文件是否存在
  hadoop fs -test -e $insert_path
  if [[ $? -eq 0 ]] ; then
    # path存在
    insert_data_du=`hadoop fs -du -s $insert_path | awk '{print $1}'`
    p1day_data_du=`hadoop fs -du -s $p1day_path | awk '{print $1}'`
    # 阈值,为了方便比较,加1
    threshold=1.1
    # 文件夹大小不为0
    if [ $insert_data_du -gt 0 -a $p1day_data_du -gt 0 ] ;then
	if [ $insert_data_du -gt $p1day_data_du  ] ; then
	    diff=`expr $insert_data_du - $p1day_data_du`
	else
	    diff=`expr $p1day_data_du - $insert_data_du`
	fi
	diff_rate=`echo "scale=10; ($diff+$p1day_data_du)/$p1day_data_du" | bc`
	if [ `expr $diff_rate \> $threshold` -eq 1 ] ; then
		echo "qc fail!!!!!!!, 磁盘波动率超过阈值"
		return 1
	else
		echo "qc success 切换视图 !!!!!!"
		return 0
	fi
    else
      echo "qc fail!!!!, 磁盘大小为0"
      return 1
    fi
  fi
}

# CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dim_mobdi_mapping.db/dim_id_mapping_android_sec_df/version=${day}.1001" "hdfs://ShareSdkHadoop/user/hive/warehouse/dim_mobdi_mapping.db/dim_id_mapping_android_sec_df/version=${p1day}.1001"


# 创建/替换视图
HADOOP_USER_NAME=dba hive -e "
create or replace view $dim_id_mapping_android_sec_df_view
as
select * from $dim_id_mapping_android_sec_df
where version='${day}.1001'
"