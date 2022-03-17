#!/bin/bash

set -e -x

day=$1

#input:
#dwd_device_location_di_v2=dm_mobdi_master.dwd_device_location_di_v2
dwd_device_location_di_v2=dm_mobdi_master.dwd_device_location_info_di_v2
#out:
ads_device_location_aggr_statiscs2_w=mob_dashboard.ads_muid_location_aggr_statiscs2_w

#3执行hql代码
HADOOP_USER_NAME=dba hive -v -e "
set hive.exec.parallel=true ;
set hive.exec.parallel.thread.number=6;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=32000000;
set mapred.min.split.size.per.rack=32000000;
set hive.merge.mapfiles = true ;
set hive.merge.mapredfiles = true ;
set hive.merge.size.per.task = 268435456;
set hive.merge.smallfiles.avgsize=32000000 ;
set hive.auto.convert.join=true;
set hive.exec.reducers.bytes.per.reducer=300000000;



INSERT OVERWRITE TABLE $ads_device_location_aggr_statiscs2_w
PARTITION(day = $day)
	select
	    from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as crtd_tmst
	   ,from_unixtime(unix_timestamp(),'yyyy-MM-dd')          as data_dt
	   ,type
	   ,country
		 ,province
		 ,city
		 ,count(1)
	 FROM $dwd_device_location_di_v2
	 WHERE     day = ${day}
	      AND  type in ('gps','wifi','base')
				group by
				    type
				   ,country
				   ,province
					 ,city

					 ;
					 "

