#!/bin/bash

set -e -x

day=$1

#input
#dwd_device_location_di_v2=dm_mobdi_master.dwd_device_location_di_v2
dwd_device_location_di_v2=dm_mobdi_master.dwd_device_location_info_di_v2
#out
ads_bi_device_location_aggr_statiscs_w=mob_dashboard.ads_bi_muid_location_aggr_statiscs_w

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


INSERT OVERWRITE TABLE  $ads_bi_device_location_aggr_statiscs_w
PARTITION(day = $day)

	--1.按照国家country聚合
	SELECT
	 from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as crtd_tmst
	 ,from_unixtime(unix_timestamp(),'yyyy-MM-dd')          as data_dt
	 ,'1'                                                   as label
	 ,country                                               as anals_dim1
	 ,'不限'                                                as anals_dim2
	 ,COUNT(DISTINCT device)                                as anals_value
	 FROM (
		 SELECT
		  country
		  ,device
		  FROM $dwd_device_location_di_v2
		  WHERE     day =${day}
			     AND  type in ('gps','wifi','base')
	 ) t
	 GROUP BY country
UNION ALL
	 --2.按照省province聚合
	 SELECT
	  from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as crtd_tmst
	  ,from_unixtime(unix_timestamp(),'yyyy-MM-dd')          as data_dt
	  ,'2'                                                   as label
	  ,province                                              as anals_dim1
	  ,'不限'                                                as anals_dim2
	  ,COUNT(DISTINCT device)                                as anals_value
	  FROM  (
		  SELECT
		   province
		   ,device
		   FROM $dwd_device_location_di_v2
		   WHERE   day =${day}
			     AND  type in ('gps','wifi','base')
	  ) t
	  GROUP BY province
UNION ALL
	  --3.按照市city聚合
	  SELECT
	   from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as crtd_tmst
	   ,from_unixtime(unix_timestamp(),'yyyy-MM-dd')          as data_dt
	   ,'3'                                                   as label
	   ,city                                                  as  anals_dim1
	   ,'不限'                                                as anals_dim2
	   ,COUNT(DISTINCT device)                                as anals_value
	   FROM  (
		   SELECT
		    city
			,device
			FROM $dwd_device_location_di_v2
			WHERE    day =${day}
			      AND  type in ('gps','wifi','base')
	   ) t
	   GROUP BY city
UNION ALL
	   --4.按照type+全国进行聚合
	   SELECT
	     from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as crtd_tmst
		  ,from_unixtime(unix_timestamp(),'yyyy-MM-dd')          as data_dt
		  ,'4'                                                   as label
		  ,type                                                  as  anals_dim1
		  ,country                                               as  anals_dim2
		  ,count(distinct device)                                as  anals_value
		from  (
			SELECT
			  type
			 ,country
			 ,device
			 FROM $dwd_device_location_di_v2
			 WHERE   day =${day}
			    AND  type in ('gps','wifi','base')
		) t
		GROUP BY
		 type
		 ,country
UNION ALL
		 --5.按照type+省进行聚合
		 SELECT
		  from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as crtd_tmst
		  ,from_unixtime(unix_timestamp(),'yyyy-MM-dd')          as data_dt
		  ,'5'                                                   as label
		  ,type                                                  as  anals_dim1
		  ,province                                              as  anals_dim2
		  ,count(distinct device)                                as anals_value
		  FROM  (
			  SELECT
			    type
			   ,province
			   ,device
			   FROM $dwd_device_location_di_v2
			   WHERE   day =${day}
				     AND  type in ('gps','wifi','base')
		  ) t
		  GROUP BY
		   type
		   ,province
UNION ALL
		   --6.按照type+市进行聚合
		   SELECT
		     from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as crtd_tmst
			  ,from_unixtime(unix_timestamp(),'yyyy-MM-dd')          as data_dt
			  ,'6'                                                   as label
			  ,type                                                  as  anals_dim1
			  ,city                                                  as  anals_dim2
			  ,COUNT(DISTINCT device)                                as anals_value
			FROM  (
				SELECT
				  type
				 ,city
				 ,device
				 FROM $dwd_device_location_di_v2
				 WHERE   day =${day}
					     AND  type in ('gps','wifi','base')
			) t
			GROUP BY
			 type
			 ,city
			 ;
			 "


