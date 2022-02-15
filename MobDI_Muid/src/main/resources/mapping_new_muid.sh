#!/bin/bash
set -x -e

if [[ $# -lt 2 ]]
then
echo "参数不足: 1.分区日期 2.原表 3.目标表(可不填,默认为原表)"
exit 1
fi

yesterday=$1
raw_table=$2
dw_table=$3

if [[ -z $3 ]]
then
dw_table=${raw_table}
fi

mid_field="mid"
queue="root.yarn_etl.etl"

duid_col=$(grep "^$dw_table " ./table.conf |awk '{print $2}')
oiid_col=$(grep "^$dw_table " ./table.conf |awk '{print $3}')
ieid_col=$(grep "^$dw_table " ./table.conf |awk '{print $4}')

if [[ -z ${duid_col} ]]
then
duid_col="duid"
fi

if [[ -z ${oiid_col} ]]
then
oiid_col="oiid"
fi

if [[ -z ${ieid_col} ]]
then
ieid_col="ieid"
fi


select=$(hive -e "desc ${dw_table}" | awk -F '\t' '{print $1,","}' | xargs echo | sed s/[[:space:]]//g|awk -F ',,#' '{print $1}')

par=$(echo ${select}|awk -F ',' '{print $(NF)}')

select_muid=","$(echo ${select}|sed "s/,$par//g")","

muid_flag=0
if [[ ${select_muid} == *,${mid_field},* ]] ;then
  muid_flag=1
else
  echo "没有muid,无需执行该脚本"
  exit 1
fi

mid_db="dm_mid_master"

duid_mid_mapping_par="$mid_db.duid_mid_mapping_par"
oiid_mid_mapping_par="$mid_db.oiid_mid_mapping_par"
ieid_mid_mapping_par="$mid_db.ieid_mid_mapping_par"

mapping_par=$(hive -e "show partitions $duid_mid_mapping_par" |tail -1 |awk -F '=' '{print $2}')

duid_flag=0
if [[ ${select_muid} == *,${duid_col},* ]] ;then
duid_flag=1
fi

oiid_flag=0
if [[ ${select_muid} == *,${oiid_col},* && ${select_muid} == *,factory,* ]] ;then
oiid_flag=1
fi

ieid_flag=0
if [[ ${select_muid} == *,${ieid_col},* ]] ;then
ieid_flag=1
fi

if [[ ${duid_flag} -eq 0 && ${oiid_flag} -eq 0 && ${ieid_col} -eq 0 ]]
then
echo "没有duid,oiid,ieid3个字段,无法进行匹配"
exit 1
fi


select_raw=$(echo ${select_muid}|awk -F '#' '{print substr($1,2,(length($0)-2))}')
select_raw_no_mid=$(echo ${select_muid}|sed "s/,$mid_field,/,null $mid_field,/g" \
|awk -F '#' '{print substr($1,2,(length($0)-2))}')

staged_table="raw_table"
sql="
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set mapreduce.job.queuename=$queue;
set hive.merge.size.per.task = 128000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager.jar;
create temporary function sha as 'com.youzu.mob.java.udf.SHA1Hashing';
with $staged_table as (
select $select_raw_no_mid
from ${raw_table} where $par = $yesterday
)
"

if [[ ${duid_flag} -gt 0 ]]
then

muid_col_select="if(coalesce(a.${mid_field},'')='',b.mid,a.${mid_field}) ${mid_field}"

select_duid=$(echo ${select_muid}|sed "s/,$duid_col,/,a.$duid_col,/g"|sed "s/,$mid_field,/,$muid_col_select,/g" \
| awk -F '#' '{print substr($1,2,(length($0)-2))}')

sql=${sql}"
,duid_stage as(
select $select_duid
from $staged_table a left join $duid_mid_mapping_par b
on a.$duid_col=b.duid and b.day=$mapping_par
where coalesce(a.$duid_col,'')<>''

union all

select $select_raw from ${staged_table} where coalesce($duid_col,'')=''
)
"
staged_table="duid_stage"

fi


if [[ ${oiid_flag} -gt 0 ]]
then
muid_col_select="if(coalesce(a.${mid_field},'')='',b.mid,a.${mid_field}) ${mid_field}"
select_oiid=$(echo ${select_muid}|sed "s/,$oiid_col,/,a.$oiid_col,/g"|sed "s/,factory,/,a.factory,/g" \
|sed "s/,$mid_field,/,$muid_col_select,/g" \
| awk -F '#' '{print substr($1,2,(length($0)-2))}')


sql=${sql}"
,oiid_stage as(
select $select_oiid
from ${staged_table} a left join $oiid_mid_mapping_par b
on a.$oiid_col=b.oiid and a.factory=b.factory and b.day=$mapping_par
where coalesce(a.$oiid_col,'')<>''

union all

select $select_raw from ${staged_table} where coalesce($oiid_col,'')=''
)
"

staged_table="oiid_stage"

fi

if [[ ${ieid_flag} -gt 0 ]]
then
muid_col_select="if(coalesce(a.${mid_field},'')='',b.mid,a.${mid_field}) ${mid_field}"
select_ieid=$(echo ${select_muid}|sed "s/,$ieid_col,/,a.$ieid_col,/g"|sed "s/,$mid_field,/,$muid_col_select,/g" \
| awk -F '#' '{print substr($1,2,(length($0)-2))}')

sql=${sql}"
,ieid_stage as(
select $select_ieid
from ${staged_table} a left join $ieid_mid_mapping_par b
on a.$ieid_col=b.ieid and b.day=$mapping_par
where coalesce(a.$ieid_col,'')<>''

union all

select $select_raw from ${staged_table} where coalesce($ieid_col,'')=''
)
"

staged_table="ieid_stage"

fi

select_sha_duid=${select_raw}

if [[ ${duid_flag} -gt 0 ]]
then
sha_duid_col_select="if(coalesce(${mid_field},'')='',if(coalesce($duid_col,'')='','',concat(sha($duid_col),'_0')),${mid_field}) ${mid_field}"
select_sha_duid=$(echo ${select_muid}|sed "s/,$mid_field,/,$sha_duid_col_select,/g" \
| awk -F '#' '{print substr($1,2,(length($0)-2))}')
fi

sql=${sql}"
insert overwrite table $dw_table partition($par=$yesterday)
select $select_sha_duid from $staged_table
"

echo -e "$sql"
hive -e "$sql"
