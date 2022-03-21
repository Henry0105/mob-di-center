#!/bin/bash

set -x -e

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;

set mapreduce.map.memory.mb=8096;
set mapreduce.map.java.opts='-Xmx6g';
set mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8096;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';
set hive.optimize.skewjoin=true;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=30;
set hive.exec.reducers.bytes.per.reducer=1073741824;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;

drop table if exists mobdi_test.gai_external_all;
create table mobdi_test.gai_external_all stored as orc as 
select device, ext_phoneno, max(ext_phoneno_ltm) as ext_phoneno_ltm
from
(
  select device, ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_mac   ----mac匹配numRows=505942632
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_3   ----新外采numRows=8112429323
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_bu_0   ----补充0numRows=8413628109
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_bu_1   ----补充1numRows=8411224910
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_bu_2   ----补充2numRows=8411334948
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_bu_3   ----补充3numRows=8395855474
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_100   ----旧外采1numRows=6801226699
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_101   ----旧外采2numRows=6801391797
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_102   ----旧外采3numRows=6821553923
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_103   ----旧外采4numRows=6774063892
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_104   ----旧外采5numRows=6792230111
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_105   ----旧外采6numRows=6819120366
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_106   ----旧外采7numRows=6819098883
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_107   ----旧外采8numRows=6823899050
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_108   ----旧外采9numRows=6833105848
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_109   ----旧外采10numRows=6816483477
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_110   ----旧外采11numRows=6822295702
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_111   ----旧外采12numRows=6816217132
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_112   ----旧外采13numRows=6832936218
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_113  ----旧外采14numRows=6824618413
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_114_1   ----旧外采15numRows=6794626002
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
  union all
  select device, split(ext_phoneno,'\\|')[0] as ext_phoneno, ext_phoneno_ltm
  from mobdi_test.gai_external_imei_115   ----旧外采16numRows=6809983787
  where length(ext_phoneno) > 0 and length(ext_phoneno_ltm) > 0
  group by device,ext_phoneno,ext_phoneno_ltm
) as a
group by device, ext_phoneno;
"