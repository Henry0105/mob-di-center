#!/usr/bin/env bash

set -e -x
: '
@owner:guanyt
@describe:生成标签的中间表dm_mobdi_md.timewindow_tmp
@projectName:MobDI
@BusinessName:online_tool
@SourceTable:rp_mobdi_app.app_active_daily,dm_mobdi_master.master_reserved_new,dm_mobdi_master.device_install_app_master
@TargetTable:dm_mobdi_md.timewindow_tmp
'

#入参
day=$1
source_type=$2

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#input
timewindow_tmp=dm_mobdi_tmp.timewindow_tmp
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di
#app_active_daily=rp_mobdi_app.app_active_daily
#dws_device_install_app_status_40d_di=dm_mobdi_topic.dws_device_install_app_status_40d_di

#mapping
#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par
#dim_online_category_mapping_v3=dim_sdk_mapping.online_category_mapping_v3
#dim_online_profile_mapping_v3=dim_sdk_mapping.online_profile_mapping_v3

#ouput


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
      echo "${time_flag}=${start} partition of table ${table} is not found >>"
      echo "_-------------------"
      return 1
    else
      echo ${start}" exist"
    fi
  done
}

#后面会修改为filter字段的值
function gen_flag_condition(){
if [ ${source_type} -eq 0 ]; then
  flagCondition="refine_final_flag = 1"
elif [ ${source_type} -eq 1 ]; then
  flagCondition="refine_final_flag = -1"
elif [ ${source_type} -eq 8 ]; then
  flagCondition="refine_final_flag in (1,0,-1,2)"
else
  flagCondition="1=1"
fi
}



function gen_tmp_table(){
    theday=$1
    source_type=$2
    #source_type定义inputTable,然后定义device表

# 改为保存所有的cate，这个以后方便扩展
    category_mapping="
      select a.relation,a.cate_id,a.total,a.percent from
      (select relation,cate_id,total,percent
      from ${dim_online_category_mapping_v3}
      where version=${mapping_version}
      )a
      left semi join
      (
      select cate_id_1 as cate_id from ${dim_online_profile_mapping_v3}
      lateral view explode(split(cate_id,',')) t_cate as cate_id_1
      where version=${profile_version} and source_type=${source_type}
      ) b
      on a.cate_id=b.cate_id

      union all

      select a.relation,a.cate_id,a.total,a.percent from
      (select relation,cate_id,total,percent
      from ${dim_online_category_mapping_v3}
      where version=${mapping_version}
      )a
      left semi join
      (
      select percent as cate_id from ${dim_online_profile_mapping_v3}
      where version=${profile_version} and source_type=${source_type}
      ) b
      on a.cate_id=b.cate_id
    "

    apppkg_mapping="
      select apppkg,pkg
      from $dim_app_pkg_mapping_par
      where version='1000'
    "

    if [[ $source_type -eq 0 ]]||[[ $source_type -eq 1 ]]||[[ $source_type -eq 8 ]];then
      #echo "计算安装"
      inputTable=$dws_device_install_app_re_status_di
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
    elif [ $source_type -eq 2 ];then
      #echo "计算活跃"
      inputTable=$app_active_daily
      deviceSql="
           select device,relation,${theday} as day,count(1) as num
           from
           (
           select device,apppkg as relation from ${inputTable} where day=${theday}
           )t
           group by device,relation
      "

    elif [ $source_type -eq 4 ];then
      echo "计算工作时间apppkg安装"

      #todo
      inputTable=$dws_device_install_app_status_40d_di
      deviceSql="
	       select device,coalesce(mapping.apppkg, device.pkg) as relation,${theday} as day,count(1) as num
           from
           (
           select device,pkg from ${inputTable}
           where day=${theday}
           and install_flag=1
           and notvacation(${theday},0)
           and hour(from_unixtime(unix_timestamp(install_datetime),'yyyy-MM-dd HH:mm:ss'))>=8
           and hour(from_unixtime(unix_timestamp(install_datetime),'yyyy-MM-dd HH:mm:ss'))<19
		   )device
		   left join
		    (${apppkg_mapping}) mapping
			on device.pkg = mapping.pkg
			group by device,coalesce(mapping.apppkg, device.pkg)
      "
    elif [ $source_type -eq 5 ];then
      echo "计算工作时间apppkg卸载"

      #todo
      inputTable=$dws_device_install_app_status_40d_di
      deviceSql="
	       select device,coalesce(mapping.apppkg, device.pkg) as relation,${theday} as day,count(1) as num
           from
           (
           select device,pkg from ${inputTable}
           where day=${theday}
           and unstall_flag=1
           and notvacation(${theday},0)
           and hour(from_unixtime(unix_timestamp(unstall_datetime),'yyyy-MM-dd HH:mm:ss'))>=8
           and hour(from_unixtime(unix_timestamp(unstall_datetime),'yyyy-MM-dd HH:mm:ss'))<19
		   )device
		   left join
		    (${apppkg_mapping}) mapping
			on device.pkg = mapping.pkg
			group by device,coalesce(mapping.apppkg, device.pkg)
      "

    elif [ $source_type -eq 6 ];then
      echo "计算居住时间apppkg安装"
      #todo
      inputTable=$dws_device_install_app_status_40d_di
      deviceSql="
	       select device,coalesce(mapping.apppkg, device.pkg) as relation,${theday} as day,count(1) as num
           from
           (
           select device,pkg from ${inputTable}
           where day=${theday}
           and install_flag=1
           and
           (
           (
           notvacation(${theday},0)
           and
           (hour(from_unixtime(unix_timestamp(install_datetime),'yyyy-MM-dd HH:mm:ss'))<7
           or hour(from_unixtime(unix_timestamp(install_datetime),'yyyy-MM-dd HH:mm:ss'))>=20
           )
           )
           or
           (
           notvacation(${theday},1)
           and
           (hour(from_unixtime(unix_timestamp(install_datetime),'yyyy-MM-dd HH:mm:ss'))<7
           or hour(from_unixtime(unix_timestamp(install_datetime),'yyyy-MM-dd HH:mm:ss'))>=20
           )
           )
           )
		   )device
		   left join
		    (${apppkg_mapping}) mapping
			on device.pkg = mapping.pkg
			group by device,coalesce(mapping.apppkg, device.pkg)
      "
    elif [ $source_type -eq 7 ];then
      echo "计算居住时间apppkg卸载"
      #todo
      inputTable=$dws_device_install_app_status_40d_di
      deviceSql="
	       select device,coalesce(mapping.apppkg, device.pkg) as relation,${theday} as day,count(1) as num
           from
           (
           select device,pkg from ${inputTable}
           where day=${theday}
           and unstall_flag=1
           and
           (
           (
           notvacation(${theday},0)
           and
           (hour(from_unixtime(unix_timestamp(unstall_datetime),'yyyy-MM-dd HH:mm:ss'))<7
           or hour(from_unixtime(unix_timestamp(unstall_datetime),'yyyy-MM-dd HH:mm:ss'))>=20
           )
           )
           or
           (
           notvacation(${theday},1)
           and
           (hour(from_unixtime(unix_timestamp(unstall_datetime),'yyyy-MM-dd HH:mm:ss'))<7
           or hour(from_unixtime(unix_timestamp(unstall_datetime),'yyyy-MM-dd HH:mm:ss'))>=20
           )
           )
           )
		   )device
		   left join
		    (${apppkg_mapping}) mapping
			on device.pkg = mapping.pkg
			group by device,coalesce(mapping.apppkg, device.pkg)
      "
    elif [ $source_type -eq 10 ];then
		echo "计算装过，final_flag 0,1,装过为8和10结合"
      inputTable=$dws_device_install_app_status_40d_di
      deviceSql="
	       select device,coalesce(mapping.apppkg, device.pkg) as relation,${theday} as day,count(1) as num
           from
           (
           select device,pkg from ${inputTable}
           where day=${theday}
		   and final_flag in (0,1)
		   )device
		   left join
		    (${apppkg_mapping}) mapping
			on device.pkg = mapping.pkg
			group by device,coalesce(mapping.apppkg, device.pkg)
			"
    elif [ $source_type -eq 9 ];then
		echo "计算在装，final_flag <> -1"
      inputTable=$dws_device_install_app_status_40d_di
      deviceSql="
	       select device,coalesce(mapping.apppkg, device.pkg) as relation,${theday} as day,count(1) as num
           from
           (
           select device,pkg from ${inputTable}
           where day=${theday}
		   and final_flag in (0,1)
		   )device
		   left join
		    (${apppkg_mapping}) mapping
			on device.pkg = mapping.pkg
			group by device,coalesce(mapping.apppkg, device.pkg)
			"

    else
      echo "new type"
	  exit 1
    fi
    #如果分区存在，则不用生成，跳过
    yesterday=`date -d "${theday} -1 days" +%Y%m%d`
    #data_exists2 ${yesterday} ${theday} ${tmp_table} day "type=${source_type}/*"
    echo "--------------------"
    if [ $? -eq 0 ];then
      echo "exist"
    else
      echo "not exist"
    fi


    #如果数据源不存在，则直接中断程序,任务失败
    yesterday=`date -d "${theday} -1 days" +%Y%m%d`
    data_exists2 ${yesterday} ${theday} ${inputTable} day
    if [ $? -eq 0 ];then
      echo "..."
    elif [ $source_type -eq 2 ];then
      inputTable=$app_active_daily
      varMonth=`date -d "${theday}" "+%Y%m"`
      deviceSql="
           select device,relation,${theday} as day,count(1) as num
           from
           (
           select device, relation
           from
           (
           select device,apppkg as relation,day from $app_active_monthly
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
      set hive.exec.max.dynamic.partitions.pernode=4000;
      set mapreduce.job.queuename=root.dba;


      add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
      create temporary function notvacation as 'com.youzu.mob.java.udf.NotVacation';

      insert overwrite table ${timewindow_tmp} partition(type=${source_type},cate_id,day)
      select device_info.device,device_info.relation,mapping.percent,mapping.total,device_info.num,mapping.cate_id,${theday} as day from
      (
        ${category_mapping}
      )mapping
      inner join
      (
           ${deviceSql}
      )device_info
      on device_info.relation = mapping.relation
      cluster by cate_id
      ;
    "
}

gen_flag_condition
echo "flag"$flagCondition
#mapping_version=`hive -e "select max(version)
#                 from ${dim_online_category_mapping_v3}
#                 where type = ${source_type}"`
#目前指定mapping表的分区，表为配置表，暂不更新
mapping_version=20190507

profile_version=20190513

gen_tmp_table $day $source_type

