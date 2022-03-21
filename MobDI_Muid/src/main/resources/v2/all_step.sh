#!/bin/bash
set -x -e

sh ./ids_mapping.sh 20210901 20211001 > logs/ids_mapping/ids_mapping_20210901_20211001.log 2>&1 &
sh ./ids_mapping.sh 20210801 20210901 > logs/ids_mapping/ids_mapping_20210801_20210901.log 2>&1 &
sh ./ids_mapping.sh 20210701 20210801 > logs/ids_mapping/ids_mapping_20210701_20210801.log 2>&1 &
sh ./ids_mapping.sh 20210601 20210701 > logs/ids_mapping/ids_mapping_20210601_20210701.log 2>&1 &
wait
sh ./ids_mapping.sh 20210501 20210601 > logs/ids_mapping/ids_mapping_20210501_20210601.log 2>&1 &
sh ./ids_mapping.sh 20210401 20210501 > logs/ids_mapping/ids_mapping_20210401_20210501.log 2>&1 &
sh ./ids_mapping.sh 20210301 20210401 > logs/ids_mapping/ids_mapping_20210301_20210401.log 2>&1 &
sh ./ids_mapping.sh 20210201 20210301 > logs/ids_mapping/ids_mapping_20210201_20210301.log 2>&1 &
wait
sh ./ids_mapping.sh 20210101 20210201 > logs/ids_mapping/ids_mapping_20210101_20210201.log 2>&1 &
sh ./ids_mapping.sh 20201201 20210101 > logs/ids_mapping/ids_mapping_20201201_20210101.log 2>&1 &
sh ./ids_mapping.sh 20201101 20201201 > logs/ids_mapping/ids_mapping_20201101_20201201.log 2>&1 &
sh ./ids_mapping.sh 20201001 20201101 > logs/ids_mapping/ids_mapping_20201001_20201101.log 2>&1 &
wait
sh ./ids_mapping.sh 20200901 20201001 > logs/ids_mapping/ids_mapping_20200901_20201001.log 2>&1 &
sh ./ids_mapping.sh 20200801 20200901 > logs/ids_mapping/ids_mapping_20200801_20200901.log 2>&1 &
sh ./ids_mapping.sh 20200701 20200801 > logs/ids_mapping/ids_mapping_20200701_20200801.log 2>&1 &
sh ./ids_mapping.sh 20200601 20200701 > logs/ids_mapping/ids_mapping_20200601_20200701.log 2>&1 &
wait
sh ./ids_mapping.sh 20200501 20200601 > logs/ids_mapping/ids_mapping_20200501_20200601.log 2>&1 &
sh ./ids_mapping.sh 20200401 20200501 > logs/ids_mapping/ids_mapping_20200401_20200501.log 2>&1 &
sh ./ids_mapping.sh 20200301 20200401 > logs/ids_mapping/ids_mapping_20200301_20200401.log 2>&1 &
sh ./ids_mapping.sh 20200201 20200301 > logs/ids_mapping/ids_mapping_20200201_20200301.log 2>&1 &
wait
sh ./ids_mapping.sh 20200101 20200201 > logs/ids_mapping/ids_mapping_20200101_20200201.log 2>&1 &
sh ./ids_mapping.sh 20191201 20200101 > logs/ids_mapping/ids_mapping_20191201_20200101.log 2>&1 &
sh ./ids_mapping.sh 20191101 20191201 > logs/ids_mapping/ids_mapping_20191101_20191201.log 2>&1 &
wait
sh ./ids_mapping.sh 20191001 20191101 > logs/ids_mapping/ids_mapping_20191001_20191101.log 2>&1 &
sh ./ids_mapping.sh 20190901 20191001 > logs/ids_mapping/ids_mapping_20190901_20191001.log 2>&1 &
sh ./ids_mapping.sh 20190801 20190901 > logs/ids_mapping/ids_mapping_20190801_20190901.log 2>&1 &
wait
sh ./ids_mapping.sh 20190701 20190801 > logs/ids_mapping/ids_mapping_20190701_20190801.log 2>&1 &
sh ./ids_mapping.sh 20190601 20190701 > logs/ids_mapping/ids_mapping_20190601_20190701.log 2>&1 &
sh ./ids_mapping.sh 20190501 20190601 > logs/ids_mapping/ids_mapping_20190501_20190601.log 2>&1 &
sh ./ids_mapping.sh 20190401 20190501 > logs/ids_mapping/ids_mapping_20190401_20190501.log 2>&1 &
wait

sh ./ids_mapping.sh 20190301 20190401 > logs/ids_mapping/ids_mapping_20190301_20190401.log 2>&1 &
sh ./ids_mapping.sh 20190201 20190301 > logs/ids_mapping/ids_mapping_20190201_20190301.log 2>&1 &
sh ./ids_mapping.sh 20190101 20190201 > logs/ids_mapping/ids_mapping_20190101_20190201.log 2>&1 &
sh ./ids_mapping.sh 20181201 20190101 > logs/ids_mapping/ids_mapping_20181201_20190101.log 2>&1 &
sh ./ids_mapping.sh 20181101 20181201 > logs/ids_mapping/ids_mapping_20181101_20181201.log 2>&1 &
sh ./ids_mapping.sh 20181001 20181101 > logs/ids_mapping/ids_mapping_20181001_20181101.log 2>&1 &
wait
sh ./ids_mapping.sh 20180901 20181001 > logs/ids_mapping/ids_mapping_20180901_20181001.log 2>&1 &
sh ./ids_mapping.sh 20180801 20180901 > logs/ids_mapping/ids_mapping_20180801_20180901.log 2>&1 &
sh ./ids_mapping.sh 20180701 20180801 > logs/ids_mapping/ids_mapping_20180701_20180801.log 2>&1 &
sh ./ids_mapping.sh 20180601 20180701 > logs/ids_mapping/ids_mapping_20180601_20180701.log 2>&1 &
sh ./ids_mapping.sh 20180501 20180601 > logs/ids_mapping/ids_mapping_20180501_20180601.log 2>&1 &
sh ./ids_mapping.sh 20180401 20180501 > logs/ids_mapping/ids_mapping_20180401_20180501.log 2>&1 &
wait
sh ./ids_mapping.sh 20180301 20180401 > logs/ids_mapping/ids_mapping_20180301_20180401.log 2>&1 &
sh ./ids_mapping.sh 20180201 20180301 > logs/ids_mapping/ids_mapping_20180201_20180301.log 2>&1 &
sh ./ids_mapping.sh 20180101 20180201 > logs/ids_mapping/ids_mapping_20180101_20180201.log 2>&1 &
sh ./ids_mapping.sh 20171201 20180101 > logs/ids_mapping/ids_mapping_20171201_20180101.log 2>&1 &
sh ./ids_mapping.sh 20171101 20171201 > logs/ids_mapping/ids_mapping_20171101_20171201.log 2>&1 &
sh ./ids_mapping.sh 20171001 20171101 > logs/ids_mapping/ids_mapping_20171001_20171101.log 2>&1 &
wait

tmp_db=dm_mid_master

id_source="$tmp_db.dwd_all_id_detail"

hive -e "
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.support.quoted.identifiers=None;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
set mapreduce.map.java.opts=-Xmx15000m;
set mapreduce.map.memory.mb=14000;
set mapreduce.reduce.java.opts=-Xmx15000m;
set mapreduce.reduce.memory.mb=14000;

insert overwrite table $id_source partition(day='all_with_blacklist')
select duid,oiid,ieid,factory,model,unid,mid,flag
from $id_source where day between 20190501 and 20211001
group by duid,oiid,ieid,factory,model,unid,mid,flag
"
#生成并删除黑名单
sh delete_blacklist.sh
sh all_fsid.sh
sh id_mapping_unid.sh 'all'
sh graph.sh
sh mapping_duid_final.sh 'all'
sh ids_mid_mapping.sh 'all'
sh gen_flag.sh
sh analysis.sh
