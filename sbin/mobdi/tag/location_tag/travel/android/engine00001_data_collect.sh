#!/bin/bash

source ../../../../util/util.sh

## 文旅标签 出行人群

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

tmpdb=$dm_mobdi_tmp
## 源表
tmp_engine00001_datapre=$tmpdb.tmp_engine00001_datapre

## 目标表
engine00001_data_collect=$tmpdb.engine00001_data_collect


day=$1


sql_final="
insert overwrite table $engine00001_data_collect partition(day='$day')
select device
from $tmp_engine00001_datapre
where city <> city_home and city <> city_work and (length(city_home) > 0 or length(city_work) > 0)
group by device;
"

hive_setting "$sql_final"
