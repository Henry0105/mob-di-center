#!/bin/bash

function hive_setting(){

sql=$1
full_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;

SET mapreduce.map.memory.mb=3072;
set mapreduce.map.java.opts='-Xmx2g';
set mapreduce.child.map.java.opts='-Xmx2g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';

set hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=8;
set hive.exec.dynamic.partition=false;
set mapred.max.split.size=256000000;
set hive.merge.smallfiles.avgsize=25000000;
set hive.merge.mapredfiles=true;
set hive.groupby.skewindata=true;
set hive.auto.convert.join=true;

set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
$sql
;
"
##
HADOOP_USER_NAME=dba hive -v -e "$full_partition_sql"

}

