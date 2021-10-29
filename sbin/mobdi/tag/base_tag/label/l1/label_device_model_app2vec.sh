#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 计算设备每日模型运行需要的app2vec
@projectName:MOBDI
'

source /home/dba/mobdi_center/conf/hive-env.sh

day=$1

tmp_db=$dm_mobdi_tmp

##input
device_applist_new=${dim_device_applist_new_di}

##mapping
#app2vec_mapping_par="tp_mobdi_model.app2vec_mapping_par"

##tmp
tmp_label_app2vec=${tmp_db}.tmp_label_app2vec

##output
label_l1_device_model_app2vec=${label_l1_device_model_app2vec}

sql="
select device,
       avg(d1) as d1,avg(d2) as d2,avg(d3) as d3,avg(d4) as d4,avg(d5) as d5,avg(d6) as d6,avg(d7) as d7,avg(d8) as d8,avg(d9) as d9,
       avg(d10) as d10,avg(d11) as d11,avg(d12) as d12,avg(d13) as d13,avg(d14) as d14,avg(d15) as d15,avg(d16) as d16,avg(d17) as d17,
       avg(d18) as d18,avg(d19) as d19,avg(d20) as d20,avg(d21) as d21,avg(d22) as d22,avg(d23) as d23,avg(d24) as d24,avg(d25) as d25,
       avg(d26) as d26,avg(d27) as d27,avg(d28) as d28,avg(d29) as d29,avg(d30) as d30,avg(d31) as d31,avg(d32) as d32,avg(d33) as d33,
       avg(d34) as d34,avg(d35) as d35,avg(d36) as d36,avg(d37) as d37,avg(d38) as d38,avg(d39) as d39,avg(d40) as d40,avg(d41) as d41,
       avg(d42) as d42,avg(d43) as d43,avg(d44) as d44,avg(d45) as d45,avg(d46) as d46,avg(d47) as d47,avg(d48) as d48,avg(d49) as d49,
       avg(d50) as d50,avg(d51) as d51,avg(d52) as d52,avg(d53) as d53,avg(d54) as d54,avg(d55) as d55,avg(d56) as d56,avg(d57) as d57,
       avg(d58) as d58,avg(d59) as d59,avg(d60) as d60,avg(d61) as d61,avg(d62) as d62,avg(d63) as d63,avg(d64) as d64,avg(d65) as d65,
       avg(d66) as d66,avg(d67) as d67,avg(d68) as d68,avg(d69) as d69,avg(d70) as d70,avg(d71) as d71,avg(d72) as d72,avg(d73) as d73,
       avg(d74) as d74,avg(d75) as d75,avg(d76) as d76,avg(d77) as d77,avg(d78) as d78,avg(d79) as d79,avg(d80) as d80,avg(d81) as d81,
       avg(d82) as d82,avg(d83) as d83,avg(d84) as d84,avg(d85) as d85,avg(d86) as d86,avg(d87) as d87,avg(d88) as d88,avg(d89) as d89,
       avg(d90) as d90,avg(d91) as d91,avg(d92) as d92,avg(d93) as d93,avg(d94) as d94,avg(d95) as d95,avg(d96) as d96,avg(d97) as d97,
       avg(d98) as d98,avg(d99) as d99,
       avg(d100) as d100,avg(d101) as d101,avg(d102) as d102,avg(d103) as d103,avg(d104) as d104,avg(d105) as d105,avg(d106) as d106,
       avg(d107) as d107,avg(d108) as d108,avg(d109) as d109,avg(d110) as d110,avg(d111) as d111,avg(d112) as d112,avg(d113) as d113,
       avg(d114) as d114,avg(d115) as d115,avg(d116) as d116,avg(d117) as d117,avg(d118) as d118,avg(d119) as d119,avg(d120) as d120,
       avg(d121) as d121,avg(d122) as d122,avg(d123) as d123,avg(d124) as d124,avg(d125) as d125,avg(d126) as d126,avg(d127) as d127,
       avg(d128) as d128,avg(d129) as d129,avg(d130) as d130,avg(d131) as d131,avg(d132) as d132,avg(d133) as d133,avg(d134) as d134,
       avg(d135) as d135,avg(d136) as d136,avg(d137) as d137,avg(d138) as d138,avg(d139) as d139,avg(d140) as d140,avg(d141) as d141,
       avg(d142) as d142,avg(d143) as d143,avg(d144) as d144,avg(d145) as d145,avg(d146) as d146,avg(d147) as d147,avg(d148) as d148,
       avg(d149) as d149,avg(d150) as d150,avg(d151) as d151,avg(d152) as d152,avg(d153) as d153,avg(d154) as d154,avg(d155) as d155,
       avg(d156) as d156,avg(d157) as d157,avg(d158) as d158,avg(d159) as d159,avg(d160) as d160,avg(d161) as d161,avg(d162) as d162,
       avg(d163) as d163,avg(d164) as d164,avg(d165) as d165,avg(d166) as d166,avg(d167) as d167,avg(d168) as d168,avg(d169) as d169,
       avg(d170) as d170,avg(d171) as d171,avg(d172) as d172,avg(d173) as d173,avg(d174) as d174,avg(d175) as d175,avg(d176) as d176,
       avg(d177) as d177,avg(d178) as d178,avg(d179) as d179,avg(d180) as d180,avg(d181) as d181,avg(d182) as d182,avg(d183) as d183,
       avg(d184) as d184,avg(d185) as d185,avg(d186) as d186,avg(d187) as d187,avg(d188) as d188,avg(d189) as d189,avg(d190) as d190,
       avg(d191) as d191,avg(d192) as d192,avg(d193) as d193,avg(d194) as d194,avg(d195) as d195,avg(d196) as d196,avg(d197) as d197,
       avg(d198) as d198,avg(d199) as d199,avg(d200) as d200,avg(d201) as d201,avg(d202) as d202,avg(d203) as d203,avg(d204) as d204,
       avg(d205) as d205,avg(d206) as d206,avg(d207) as d207,avg(d208) as d208,avg(d209) as d209,avg(d210) as d210,avg(d211) as d211,
       avg(d212) as d212,avg(d213) as d213,avg(d214) as d214,avg(d215) as d215,avg(d216) as d216,avg(d217) as d217,avg(d218) as d218,
       avg(d219) as d219,avg(d220) as d220,avg(d221) as d221,avg(d222) as d222,avg(d223) as d223,avg(d224) as d224,avg(d225) as d225,
       avg(d226) as d226,avg(d227) as d227,avg(d228) as d228,avg(d229) as d229,avg(d230) as d230,avg(d231) as d231,avg(d232) as d232,
       avg(d233) as d233,avg(d234) as d234,avg(d235) as d235,avg(d236) as d236,avg(d237) as d237,avg(d238) as d238,avg(d239) as d239,
       avg(d240) as d240,avg(d241) as d241,avg(d242) as d242,avg(d243) as d243,avg(d244) as d244,avg(d245) as d245,avg(d246) as d246,
       avg(d247) as d247,avg(d248) as d248,avg(d249) as d249,avg(d250) as d250,avg(d251) as d251,avg(d252) as d252,avg(d253) as d253,
       avg(d254) as d254,avg(d255) as d255,avg(d256) as d256,avg(d257) as d257,avg(d258) as d258,avg(d259) as d259,avg(d260) as d260,
       avg(d261) as d261,avg(d262) as d262,avg(d263) as d263,avg(d264) as d264,avg(d265) as d265,avg(d266) as d266,avg(d267) as d267,
       avg(d268) as d268,avg(d269) as d269,avg(d270) as d270,avg(d271) as d271,avg(d272) as d272,avg(d273) as d273,avg(d274) as d274,
       avg(d275) as d275,avg(d276) as d276,avg(d277) as d277,avg(d278) as d278,avg(d279) as d279,avg(d280) as d280,avg(d281) as d281,
       avg(d282) as d282,avg(d283) as d283,avg(d284) as d284,avg(d285) as d285,avg(d286) as d286,avg(d287) as d287,avg(d288) as d288,
       avg(d289) as d289,avg(d290) as d290,avg(d291) as d291,avg(d292) as d292,avg(d293) as d293,avg(d294) as d294,avg(d295) as d295,
       avg(d296) as d296,avg(d297) as d297,avg(d298) as d298,avg(d299) as d299,avg(d300) as d300
from seed as x
inner join
$app2vec_mapping_par y on y.day='20200214' and x.pkg = y.apppkg
group by device
"


spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.newscore.App2Vec \
--driver-memory 4G \
--executor-memory 12G \
--executor-cores 4 \
--queue root.yarn_data_compliance \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=100 \
--conf spark.dynamicAllocation.maxExecutors=200 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.speculation=true \
--conf spark.driver.maxResultSize=4g \
--conf spark.default.parallelism=2000 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=1024m -XX:PermSize=256m" \
/home/dba/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar \
--inputTable $device_applist_new \
--outputTable $tmp_label_app2vec \
--day $day \
--sql "$sql" \
--flag "label_app2vec"

#去小文件
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=250000000;
set mapred.min.split.size.per.rack=250000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task=250000000;
set hive.exec.reducers.bytes.per.reducer=256000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers=None;

INSERT OVERWRITE TABLE $label_l1_device_model_app2vec PARTITION(day)
SELECT \`(stage)?+.+\`
FROM $tmp_label_app2vec
WHERE day = '$day';
"

#只保留最近7个分区
hive -v -e "alter table ${tmp_label_app2vec} drop if exists partition(day='$b7day');"

