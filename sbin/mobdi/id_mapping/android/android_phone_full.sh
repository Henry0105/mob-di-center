#!/bin/sh
#phone不在限制个数

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

insert_day=$1
pday=`date +%Y%m%d -d "${insert_day} -1 day"`

:<<!
取dm_mobdi_mapping.phone_mapping_full最后一个分区的全量数据与dw_mobdi_md.phone_mapping_incr的分区数据进行全量更新
!

hive -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function mobdi_array_udf as 'com.youzu.mob.java.udf.MobdiArrayUtilUDF2';


SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=10;
set mapreduce.reduce.memory.mb=6144;

insert overwrite table dm_mobdi_mapping.phone_mapping_full partition (version='${insert_day}.1000',plat=1)
select
coalesce(a.device, b.device) as device,
 mobdi_array_udf('field', a.phoneno, a.phoneno_tm, b.phoneno, b.phoneno_tm,'min') as phoneno,
 mobdi_array_udf('date', a.phoneno, a.phoneno_tm, b.phoneno, b.phoneno_tm,'min') as phoneno_tm,
 mobdi_array_udf('date', a.phoneno, a.phoneno_ltm, b.phoneno, b.phoneno_tm,'max') as phoneno_ltm,

mobdi_array_udf('field', a.ext_phoneno, a.ext_phoneno_tm, b.ext_phoneno, b.ext_phoneno_tm,'min') as ext_phoneno,
mobdi_array_udf('date', a.ext_phoneno, a.ext_phoneno_tm, b.ext_phoneno, b.ext_phoneno_tm,'min') as ext_phoneno_tm,
mobdi_array_udf('date', a.ext_phoneno, a.ext_phoneno_ltm, b.ext_phoneno, b.ext_phoneno_tm,'max') as ext_phoneno_ltm,

mobdi_array_udf('field', a.sms_phoneno, a.sms_phoneno_tm, b.sms_phoneno, b.sms_phoneno_tm,'min') as sms_phoneno,
mobdi_array_udf('date', a.sms_phoneno, a.sms_phoneno_tm, b.sms_phoneno, b.sms_phoneno_tm,'min') as sms_phoneno_tm,
mobdi_array_udf('date', a.sms_phoneno, a.sms_phoneno_ltm, b.sms_phoneno, b.sms_phoneno_tm,'max') as sms_phoneno_ltm,

mobdi_array_udf('field', a.imsi, a.imsi_tm, b.imsi, b.imsi_tm,'min') as imsi,
mobdi_array_udf('date', a.imsi, a.imsi_tm, b.imsi, b.imsi_tm,'min') as imsi_tm,
mobdi_array_udf('date', a.imsi, a.imsi_ltm, b.imsi, b.imsi_tm,'max') as imsi_ltm,

mobdi_array_udf('field', a.ext_imsi, a.ext_imsi_tm, b.ext_imsi, b.ext_imsi_tm,'min') as ext_imsi,
mobdi_array_udf('date', a.ext_imsi, a.ext_imsi_tm, b.ext_imsi, b.ext_imsi_tm,'min') as ext_imsi_tm,
mobdi_array_udf('date', a.ext_imsi, a.ext_imsi_ltm, b.ext_imsi, b.ext_imsi_tm,'max') as ext_imsi_ltm
from (
  select * from dm_mobdi_mapping.phone_mapping_full where version = '${pday}.1000' and plat=1
  and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
) a
full join (select * from  dw_mobdi_md.phone_mapping_incr where day='$insert_day' and plat=1) b
on a.device = b.device
"


# full表分区清理，保留最近10天数据，同时保留每月最后一天的数据
delete_day=`date +%Y%m%d -d "${insert_day} -10 day"`
#上个月
LastMonth=`date -d "last month" +"%Y%m"`
#这个月
_todayYM=`date +"%Y%m"`
#本月第一天
CurrentMonthFirstDay=$_todayYM"01"
#本月第一天时间戳
_CurrentMonthFirstDaySeconds=`date -d "$CurrentMonthFirstDay" +%s`
#上月最后一天时间戳
_LastMonthLastDaySeconds=`expr $_CurrentMonthFirstDaySeconds - 86400`
#上个月第一天
LastMonthFistDay=`date -d @$_LastMonthLastDaySeconds "+%Y%m"`"01"
#上个月最后一天
LastMonthLastDay=`date -d @$_LastMonthLastDaySeconds "+%Y%m%d"`

if [[ "$delete_day" -ne "$LastMonthLastDay" ]]; then
  # 保留每月最后一天的数据
  # do delete thing
  echo "deleting version: ${delete_day}.1000 if exists"
  hive -e "alter table dm_mobdi_mapping.phone_mapping_full  DROP IF EXISTS PARTITION (version='${delete_day}.1000',plat=1);"
  echo "deleting version: ${delete_day}.1001 if exists"
  hive -e "alter table  dm_mobdi_mapping.phone_mapping_full  DROP IF EXISTS PARTITION (version='${delete_day}.1001',plat=1);"
  echo "deleting version: ${delete_day}.1002 if exists"
  hive -e "alter table  dm_mobdi_mapping.phone_mapping_full  DROP IF EXISTS PARTITION (version='${delete_day}.1002',plat=1);"
fi
# ### END DELETE
