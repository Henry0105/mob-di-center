#!/bin/sh
set -x -e

day=$1
source_type=$2
computer_type=$3
timewindow=$4


pdays=`date -d "$day -$timewindow days" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

tmp_table="dw_mobdi_md.timewindow_tmp"
tmp_table_total="dw_mobdi_md.timewindow_total"

: '
inPutTable:
    dm_sdk_mapping.online_category_mapping_v3
    dm_sdk_mapping.online_profile_mapping_v3
    dw_mobdi_md.timewindow_tmp
    dw_mobdi_md.timewindow_total
    dm_mobdi_topic.dws_device_install_app_status_40d_di
    dm_sdk_mapping.app_pkg_mapping_par
outPutTable:
    rp_mobdi_app.timewindow_online_profile_v3
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
    #else
      #echo ${start}" exist"
    fi
  done
}


##############
## @author:guanyt
## @time:2019-3-1
## @description: 1.对timewindow=360,180的支持
##               2.对新安装，新卸载的天数的支持，添加type
##



#检查
function timewindow_check_tmp_table(){
  source_type=$1
  timewindow=$2

  pdays=`date -d "$day -$timewindow days" +%Y%m%d`
  end=$day

  #start=`date -d "$day -$(($timewindow)) days" +%Y%m%d`
  start=$pdays

  echo -e "\n===>start timewindow:"${pdays}"-->"${end}
  i=1
  while [ ${start} -le ${end} ]
  do

    for j in `seq 1 10`
    do
      start=`date -d "${start} 1 days" +%Y%m%d`
      if [ ${start} \> ${end} ];then
          echo -e "\n===>timewindow over:"${start}_${end}
          return
      fi
      {
	     yesterday=`date -d "${start} -1 days" +%Y%m%d`
        echo "check_tmp_table "${start}
        data_exists2 ${yesterday} ${start} ${tmp_table} day "type=${source_type}/*"
		if [[ $? -ne 0 ]]&&[[ $source_type -lt 3 ]];then
           echo ${start}":tmp not exist"
		   exit 1
		else
		   echo ${start}":tmp exist"
        fi
      }
    done
    wait
    sleep 1
    #循环次数限制
    if [ $i -gt $(($timewindow+3)) ];then
	  break
    else
	  i=$(($i+1))
    fi
  done
echo "success"
}

#根据compute_type来生成不同的timewindow_total
function gen_timewindow_total(){
    #减少标签计算的cate_id
    source_type=$1
    computer_type=$2
    timewindow=$3

    pdays=`date -d "$day -$timewindow days" +%Y%m%d`
    profile_sql="
        select cate_id_1 as cate_id,profile_id from ${online_profile_mapping_v3}
        lateral view explode(split(cate_id,',')) t_cate as cate_id_1
        where version=${profile_version} and source_type=${source_type} and timewindow=${timewindow} and compute_type=${computer_type}
    "

    export HADOOP_HEAPSIZE=8192
    export HADOOP_CLIENT_OPTS="-Xmx8g"
    if [ ${computer_type} -eq 0 ];then
        #device,pkg,cate_id三元组,relation字段存pkg

        echo "计算个数"
        {
        #如果是8，装过，那么计算个数的时候，使用
        if [ ${source_type} -eq 8 ];then
            category_mapping_table="${online_category_mapping_v3}"
            profile_mapping_table="${online_profile_mapping_v3}"
            mapping_version=20190507
            profile_version=20190513


            hive -v -e "
            SET mapreduce.map.memory.mb=2048;
            set mapreduce.map.java.opts='-Xmx2048M';
            set mapreduce.child.map.java.opts='-Xmx2048M';
            set mapreduce.reduce.memory.mb=2048;
            set mapreduce.reduce.java.opts=-Xmx2048m;
            SET hive.merge.mapfiles=true;
            SET hive.merge.mapredfiles=true;
            set mapred.max.split.size=250000000;
            set mapred.min.split.size.per.node=128000000;
            set mapred.min.split.size.per.rack=128000000;
            set hive.merge.smallfiles.avgsize=250000000;
            set hive.merge.size.per.task = 250000000;
            set hive.exec.dynamic.partition=true;
            set hive.exec.dynamic.partition.mode=nonstrict;

			with device_pkg as
			(
			select device,relation from
			(
			select device,coalesce(mapping.apppkg, d.pkg) as relation
			from
			(
			  select device,pkg
			  from ${dws_device_install_app_status_40d_di}
			  where day=${pdays}
			)d
			left join
		    (
			  select apppkg,pkg
			  from ${app_pkg_mapping_par}
			  where version='1000'
			)mapping
			on d.pkg = mapping.pkg
			)data
			),

            cates as
			(
			select relation,cate_id,total,percent  from
			(
			  select relation,cate_id,total,percent
			  from ${online_category_mapping_v3}
			  where version=${mapping_version}
			)k
			left semi join
			(
			  ${profile_sql}
			)profile
			on k.cate_id=profile.cate_id
            )

            insert overwrite table ${tmp_table_total} partition(type='${source_type}_${computer_type}',timewindow=${timewindow},day=${day})
            select c.device,c.relation,c.percent,c.total,d.profile_id as cate_id,sum(num) as num
			from
			(
            select device,relation,percent,total,cate_id,sum(num) as num
            from
            (
              select device,relation,percent,total,cate_id,num
              from ${tmp_table}
              where type=${source_type} and day<=${day} and day>${pdays}

              union all
			  select device,relation,percent,total,cate_id,num from
			  (
			  select device,relation,percent,total,cate_id,count(1) as num
			  from(
			    select device,device_pkg.relation,percent,total,cate_id from
				device_pkg
				left join
				cates
				on device_pkg.relation=cates.relation
			  )s group by device,relation,percent,total,cate_id
              )installed
            )b
            group by device,relation,percent,total,cate_id
			)c
            join(
               ${profile_sql}
            )d
            on c.cate_id=d.cate_id
            group by device,relation,c.percent,c.total,profile_id
            "

        else
            hive -v -e "
            SET mapreduce.map.memory.mb=2048;
            set mapreduce.map.java.opts='-Xmx2048M';
            set mapreduce.child.map.java.opts='-Xmx2048M';
            set mapreduce.reduce.memory.mb=2048;
            set mapreduce.reduce.java.opts=-Xmx2048m;
            SET hive.merge.mapfiles=true;
            SET hive.merge.mapredfiles=true;
            set mapred.max.split.size=250000000;
            set mapred.min.split.size.per.node=128000000;
            set mapred.min.split.size.per.rack=128000000;
            set hive.merge.smallfiles.avgsize=250000000;
            set hive.merge.size.per.task = 250000000;
            set hive.exec.dynamic.partition=true;
            set hive.exec.dynamic.partition.mode=nonstrict;

            insert overwrite table ${tmp_table_total} partition(type='${source_type}_${computer_type}',timewindow=${timewindow},day=${day})
            select a.device,a.relation,a.percent,a.total,b.profile_id as cate_id,sum(num) as num
            from
            (
              select device,relation,percent,total,cate_id,num
              from ${tmp_table}
              where type=${source_type} and day<=${day} and day>${pdays}
            )a
            join(
               ${profile_sql}
            )b
            on a.cate_id=b.cate_id
            group by device,relation,a.percent,a.total,profile_id
            "
        fi

	    }

    elif [ ${computer_type} -eq 1 ];then
        #计算天数
        #device,day,cate_id三元组,用relation字段存day
        echo "计算天数"
        hive -v -e "
        SET mapreduce.map.memory.mb=2048;
        set mapreduce.map.java.opts='-Xmx2048M';
        set mapreduce.child.map.java.opts='-Xmx2048M';
        set mapreduce.reduce.memory.mb=2048;
        set mapreduce.reduce.java.opts=-Xmx2048m;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

		insert overwrite table ${tmp_table_total} partition(type='${source_type}_${computer_type}',timewindow=${timewindow},day=${day})
        select a.device,a.relation,a.percent,a.total,b.profile_id as cate_id,sum(num) as num
        from
        (
          select device,day as relation,percent,total,cate_id,num
          from ${tmp_table}
          where type=${source_type} and day<=${day} and day>${pdays}
        )a
        join(
           ${profile_sql}
        )b
        on a.cate_id=b.cate_id
        group by device,relation,a.percent,a.total,profile_id
        "
    elif [ ${computer_type} -eq 2 ];then
        #计算percent占比
        echo "计算个数占比"
        {
        hive -v -e "
        SET mapreduce.map.memory.mb=2048;
        set mapreduce.map.java.opts='-Xmx2048M';
        set mapreduce.child.map.java.opts='-Xmx2048M';
        set mapreduce.reduce.memory.mb=2048;
        set mapreduce.reduce.java.opts=-Xmx2048m;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table ${tmp_table_total} partition(type='${source_type}_${computer_type}',timewindow=${timewindow},day=${day})
        select device,relation,percent,total,cate_id,sum(num) as num
		from
		(
        select a.device,a.relation,a.percent,a.total,b.profile_id as cate_id,num
        from
        (
          select device,relation,percent,total,cate_id,num
          from ${tmp_table}
          where type=${source_type} and day<=${day} and day>${pdays}
        )a
        join(
           ${profile_sql}
        )b
        on a.cate_id=b.cate_id
		)a
        group by device,relation,percent,total,cate_id

        union all
        select c.device,c.relation,d.percent as percent,c.total,d.profile_id as cate_id,sum(num) as num
        from(
          select device,relation,percent,total,cate_id,num
          from ${tmp_table}
          where type=${source_type} and day<=${day} and day>${pdays}
        )c join (
          select percent,profile_id from ${online_profile_mapping_v3}
          where version=${profile_version} and source_type=${source_type} and timewindow=${timewindow} and compute_type=${computer_type}

        )d
		on c.cate_id=d.percent
        group by c.device,c.relation,d.percent,c.total,d.profile_id

        "
        }
    else
        echo "unknown compute_type,exit"
        exit 1
    fi

    if [ $? -ne 0 ];then
	  echo "gen_timewindow_total fail,exit"
	  exit 1
    fi

	#timewindow_total不需要保存很久，仅保留5天，删除历史数据
    #pre_5_days=`date -d "$day -5 days" +%Y%m%d`
	#hive -v -e "
	#alter table ${tmp_table_total} drop partition (type='${source_type}_${computer_type}',timewindow=${timewindow},day=${pre_5_days});
	#"
}

#滑动窗口方法，完成
function iter_gen_timewindow_total(){

  pdays=`date -d "$day -$timewindow days" +%Y%m%d`
  yesterday=`date -d "$day -1 days" +%Y%m%d`
  beforeyesterday=`date -d "$day -2 days" +%Y%m%d`

  #check yesterday total
  data_exists2 ${beforeyesterday} ${yesterday} ${tmp_table_total} day "type='${source_type}_${computer_type}'/timewindow=${timewindow}"
  if [ $? -ne 0 ];then
      echo "yesterday timewindow total not exist:  "${tmp_table_total}
      gen_timewindow_total $source_type $computer_type $timewindow
	  return
  fi

  #滑动窗口，用最近N天的total，减去第前N天的device,pkg,然后加上当前日期的device,pkg
  echo "start window iter"
  if [ ${computer_type} -eq 0 ];then
    hive -v -e "
        SET mapreduce.map.memory.mb=2048;
        set mapreduce.map.java.opts='-Xmx2048M';
        set mapreduce.child.map.java.opts='-Xmx2048M';
        set mapreduce.reduce.memory.mb=2048;
        set mapreduce.reduce.java.opts=-Xmx2048m;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

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
        SET mapreduce.map.memory.mb=2048;
        set mapreduce.map.java.opts='-Xmx2048M';
        set mapreduce.child.map.java.opts='-Xmx2048M';
        set mapreduce.reduce.memory.mb=2048;
        set mapreduce.reduce.java.opts=-Xmx2048m;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

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
  source_type=$1
  computer_type=$2
  timewindow=$3

  if [ ${computer_type} -eq 0 ]; then
    hive -v -e "
        SET mapreduce.map.memory.mb=2048;
        set mapreduce.map.java.opts='-Xmx2048M';
        set mapreduce.child.map.java.opts='-Xmx2048M';
        set mapreduce.reduce.memory.mb=2048;
        set mapreduce.reduce.java.opts=-Xmx2048m;
        SET hive.merge.mapfiles=true;
        SET hive.merge.mapredfiles=true;
        set mapred.max.split.size=250000000;
        set mapred.min.split.size.per.node=128000000;
        set mapred.min.split.size.per.rack=128000000;
        set hive.merge.smallfiles.avgsize=250000000;
        set hive.merge.size.per.task = 250000000;

    insert overwrite table ${timewindow_online_profile_v3} partition(day=${day},feature)
    select device,count(1) as cnt,cate_id as feature
    from ${tmp_table_total}
    where type='${source_type}_${computer_type}' and timewindow=${timewindow} and day=${day}
    group by device,cate_id
  "
  elif [ ${computer_type} -eq 1 ];then

   hive -v -e "
        SET mapreduce.map.memory.mb=2048;
        set mapreduce.map.java.opts='-Xmx2048M';
        set mapreduce.child.map.java.opts='-Xmx2048M';
        set mapreduce.reduce.memory.mb=2048;
        set mapreduce.reduce.java.opts=-Xmx2048m;
		SET hive.merge.mapfiles=true;
		SET hive.merge.mapredfiles=true;
		set mapred.max.split.size=250000000;
		set mapred.min.split.size.per.node=128000000;
		set mapred.min.split.size.per.rack=128000000;
		set hive.merge.smallfiles.avgsize=250000000;
		set hive.merge.size.per.task = 250000000;
		set hive.exec.dynamic.partition=true;
		set hive.exec.dynamic.partition.mode=nonstrict;

	    insert overwrite table ${timewindow_online_profile_v3} partition(day=${day},feature)
    select device,count(1) as cnt,cate_id as feature
    from ${tmp_table_total}
    where type='${source_type}_${computer_type}' and timewindow=${timewindow} and day=${day}
    group by device,cate_id
   "
   elif [ ${computer_type} -eq 2 ];then
   hive -v -e "
        SET mapreduce.map.memory.mb=2048;
        set mapreduce.map.java.opts='-Xmx2048M';
        set mapreduce.child.map.java.opts='-Xmx2048M';
        set mapreduce.reduce.memory.mb=2048;
        set mapreduce.reduce.java.opts=-Xmx2048m;
		SET hive.merge.mapfiles=true;
		SET hive.merge.mapredfiles=true;
		set mapred.max.split.size=250000000;
		set mapred.min.split.size.per.node=128000000;
		set mapred.min.split.size.per.rack=128000000;
		set hive.merge.smallfiles.avgsize=250000000;
		set hive.merge.size.per.task = 250000000;
		set hive.exec.dynamic.partition=true;
		set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.exec.max.dynamic.partitions=10000;

    insert overwrite table ${timewindow_online_profile_v3} partition(day=${day},feature)
    select percent.device,
    cast(round(percent.cnt/all.cnt,6) as String) as cnt,
	percent.feature
	from
    (
        select device,count(1) as cnt,cate_id as feature
        from ${tmp_table_total}
        where type='${source_type}_${computer_type}' and timewindow=${timewindow} and day=${day}
        and percent='0'
        group by device,cate_id
    )percent
    left join (
        select device,cate_id as feature,count(1) as cnt
        from ${tmp_table_total}
        where type='${source_type}_${computer_type}' and timewindow=${timewindow} and day=${day}
        and percent!='0'
        group by device,cate_id
    )all
    on percent.device=all.device and percent.feature=all.feature
   "

  else
    echo "unknown computer type,exit 1"
    exit 1
  fi
}


echo "=================>start job============="
echo "day:"$day
echo "compute_type:"$computer_type
echo "timewindow:"$timewindow
echo "source_type:"$source_type

profile_version='20190513'

#timewindow_check_tmp_table $source_type $timewindow
gen_timewindow_total $source_type $computer_type $timewindow
gen_timewindow_profile $source_type $computer_type $timewindow


day3=`date -d "$day -3 days" +%Y%m%d`
hive -e "alter table  ${tmp_table_total} drop partition(day=$day3);"