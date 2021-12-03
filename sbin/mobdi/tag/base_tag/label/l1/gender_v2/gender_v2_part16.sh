#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi
source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
p7=$(date -d "$day -7 days" "+%Y%m%d")
insertday=${day}_muid
#device_applist_new="dm_mobdi_mapping.device_applist_new"

gender_feature_v2_part16="${dm_mobdi_tmp}.gender_feature_v2_part16"

#app_category_mapping_par="dm_sdk_mapping.app_category_mapping_par"

#gender_app2vec_cate_l1_center="dm_sdk_mapping.gender_app2vec_cate_l1_center"

#gender_app2vec_cate_l1_center_vec_m="dm_sdk_mapping.gender_app2vec_cate_l1_center_vec_m"

#gender_app_cate_index1="dm_sdk_mapping.gender_app_cate_index1"

#apppkg_app2vec_par_wi="rp_mobdi_app.apppkg_app2vec_par_wi"

last_par=20210418

##app2vec
sql="
select device, cate_l1_id, cate_l1,
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
avg(d98) as d98,avg(d99) as d99,avg(d100) as d100
from seed x
join (select * from $apppkg_app2vec_par_wi where day = '$last_par') y
on x.pkg = y.apppkg
join (select apppkg, cate_l1_id, cate_l1 from $dim_app_category_mapping_par where version='1000' group by apppkg, cate_l1_id, cate_l1) z
on x.pkg=z.apppkg
group by device, cate_l1_id, cate_l1
"

#tmp
gender_app2vec_vec2_score_test_2k=${dm_mobdi_tmp}.gender_app2vec_vec2_score_test_2k

spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.newscore.App2Vec \
--driver-memory 4G \
--queue root.yarn_data_compliance \
--executor-memory 15G \
--executor-cores 4 \
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
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar \
--inputTable $dim_device_applist_new_di \
--outputTable $gender_app2vec_vec2_score_test_2k \
--day $day \
--sql "$sql" \
--flag "gender_v2_16_app2vec_$day"

hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;

insert overwrite table $gender_feature_v2_part16 partition(day='$insertday')
select device, 
max(index1) index1,
max(index2) index2,
max(index3) index3,
max(index4) index4,
max(index5) index5,
max(index6) index6,
max(index7) index7,
max(index8) index8,
max(index9) index9,
max(index10) index10,
max(index11) index11,
max(index12) index12,
max(index13) index13,
max(index14) index14,
max(index15) index15,
max(index16) index16,
max(index17) index17,
max(index18) index18,
max(index19) index19,
max(index1_female) index1_female,
max(index2_female) index2_female,
max(index3_female) index3_female,
max(index4_female) index4_female,
max(index5_female) index5_female,
max(index6_female) index6_female,
max(index7_female) index7_female,
max(index8_female) index8_female,
max(index9_female) index9_female,
max(index10_female) index10_female,
max(index11_female) index11_female,
max(index12_female) index12_female,
max(index13_female) index13_female,
max(index14_female) index14_female,
max(index15_female) index15_female,
max(index16_female) index16_female,
max(index17_female) index17_female,
max(index18_female) index18_female,
max(index19_female) index19_female
from 
(
  select tz.device,
  case when index= 55 and tag=0 then cos_sim else -99 end index1,
  case when index= 56 and tag=0 then cos_sim else -99 end index2,
  case when index= 57 and tag=0 then cos_sim else -99 end index3,
  case when index= 58 and tag=0 then cos_sim else -99 end index4,
  case when index= 59 and tag=0 then cos_sim else -99 end index5,
  case when index= 60 and tag=0 then cos_sim else -99 end index6,
  case when index= 61 and tag=0 then cos_sim else -99 end index7,
  case when index= 62 and tag=0 then cos_sim else -99 end index8,
  case when index= 63 and tag=0 then cos_sim else -99 end index9,
  case when index= 64 and tag=0 then cos_sim else -99 end index10,
  case when index= 65 and tag=0 then cos_sim else -99 end index11,
  case when index= 66 and tag=0 then cos_sim else -99 end index12,
  case when index= 67 and tag=0 then cos_sim else -99 end index13,
  case when index= 68 and tag=0 then cos_sim else -99 end index14,
  case when index= 69 and tag=0 then cos_sim else -99 end index15,
  case when index= 70 and tag=0 then cos_sim else -99 end index16,
  case when index= 71 and tag=0 then cos_sim else -99 end index17,
  case when index= 72 and tag=0 then cos_sim else -99 end index18,
  case when index= 73 and tag=0 then cos_sim else -99 end index19,
  case when index= 55 and tag=1 then cos_sim else -99 end index1_female,
  case when index= 56 and tag=1 then cos_sim else -99 end index2_female,
  case when index= 57 and tag=1 then cos_sim else -99 end index3_female,
  case when index= 58 and tag=1 then cos_sim else -99 end index4_female,
  case when index= 59 and tag=1 then cos_sim else -99 end index5_female,
  case when index= 60 and tag=1 then cos_sim else -99 end index6_female,
  case when index= 61 and tag=1 then cos_sim else -99 end index7_female,
  case when index= 62 and tag=1 then cos_sim else -99 end index8_female,
  case when index= 63 and tag=1 then cos_sim else -99 end index9_female,
  case when index= 64 and tag=1 then cos_sim else -99 end index10_female,
  case when index= 65 and tag=1 then cos_sim else -99 end index11_female,
  case when index= 66 and tag=1 then cos_sim else -99 end index12_female,
  case when index= 67 and tag=1 then cos_sim else -99 end index13_female,
  case when index= 68 and tag=1 then cos_sim else -99 end index14_female,
  case when index= 69 and tag=1 then cos_sim else -99 end index15_female,
  case when index= 70 and tag=1 then cos_sim else -99 end index16_female,
  case when index= 71 and tag=1 then cos_sim else -99 end index17_female,
  case when index= 72 and tag=1 then cos_sim else -99 end index18_female,
  case when index= 73 and tag=1 then cos_sim else -99 end index19_female
  from (
    select t1.device, t1.tag, t1.cate_l1, t1.cate_l1_id, 
    t1.distance, t2.device_m, t3.cate_l1_m, distance/device_m/cate_l1_m cos_sim
    from (
      select a.device, b.cate_l1, b.cate_l1_id, b.tag, 
      sum(a.d1*b.d1+a.d2*b.d2+a.d3*b.d3+a.d4*b.d4+a.d5*b.d5+a.d6*b.d6+a.d7*b.d7+a.d8*b.d8+a.d9*b.d9+a.d10*b.d10+a.d11*b.d11+a.d12*b.d12+a.d13*b.d13+a.d14*b.d14+a.d15*b.d15+a.d16*b.d16+a.d17*b.d17+a.d18*b.d18+a.d19*b.d19+a.d20*b.d20+a.d21*b.d21+a.d22*b.d22+a.d23*b.d23+a.d24*b.d24+a.d25*b.d25+a.d26*b.d26+a.d27*b.d27+a.d28*b.d28+a.d29*b.d29+a.d30*b.d30+a.d31*b.d31+a.d32*b.d32+a.d33*b.d33+a.d34*b.d34+a.d35*b.d35+a.d36*b.d36+a.d37*b.d37+a.d38*b.d38+a.d39*b.d39+a.d40*b.d40+a.d41*b.d41+a.d42*b.d42+a.d43*b.d43+a.d44*b.d44+a.d45*b.d45+a.d46*b.d46+a.d47*b.d47+a.d48*b.d48+a.d49*b.d49+a.d50*b.d50+a.d51*b.d51+a.d52*b.d52+a.d53*b.d53+a.d54*b.d54+a.d55*b.d55+a.d56*b.d56+a.d57*b.d57+a.d58*b.d58+a.d59*b.d59+a.d60*b.d60+a.d61*b.d61+a.d62*b.d62+a.d63*b.d63+a.d64*b.d64+a.d65*b.d65+a.d66*b.d66+a.d67*b.d67+a.d68*b.d68+a.d69*b.d69+a.d70*b.d70+a.d71*b.d71+a.d72*b.d72+a.d73*b.d73+a.d74*b.d74+a.d75*b.d75+a.d76*b.d76+a.d77*b.d77+a.d78*b.d78+a.d79*b.d79+a.d80*b.d80+a.d81*b.d81+a.d82*b.d82+a.d83*b.d83+a.d84*b.d84+a.d85*b.d85+a.d86*b.d86+a.d87*b.d87+a.d88*b.d88+a.d89*b.d89+a.d90*b.d90+a.d91*b.d91+a.d92*b.d92+a.d93*b.d93+a.d94*b.d94+a.d95*b.d95+a.d96*b.d96+a.d97*b.d97+a.d98*b.d98+a.d99*b.d99+a.d100*b.d100) distance
      from $gender_app2vec_vec2_score_test_2k a
      join $dim_gender_app2vec_cate_l1_center b
      on a.cate_l1_id=b.cate_l1_id and a.day = '$day'
      group by a.device, b.cate_l1, b.cate_l1_id, b.tag
    ) t1 
    join (
      select device, cate_l1_id, sqrt(sum(d1*d1+d2*d2+d3*d3+d4*d4+d5*d5+d6*d6+d7*d7+d8*d8+d9*d9+d10*d10+d11*d11+d12*d12+d13*d13+d14*d14+d15*d15+d16*d16+d17*d17+d18*d18+d19*d19+d20*d20+d21*d21+d22*d22+d23*d23+d24*d24+d25*d25+d26*d26+d27*d27+d28*d28+d29*d29+d30*d30+d31*d31+d32*d32+d33*d33+d34*d34+d35*d35+d36*d36+d37*d37+d38*d38+d39*d39+d40*d40+d41*d41+d42*d42+d43*d43+d44*d44+d45*d45+d46*d46+d47*d47+d48*d48+d49*d49+d50*d50+d51*d51+d52*d52+d53*d53+d54*d54+d55*d55+d56*d56+d57*d57+d58*d58+d59*d59+d60*d60+d61*d61+d62*d62+d63*d63+d64*d64+d65*d65+d66*d66+d67*d67+d68*d68+d69*d69+d70*d70+d71*d71+d72*d72+d73*d73+d74*d74+d75*d75+d76*d76+d77*d77+d78*d78+d79*d79+d80*d80+d81*d81+d82*d82+d83*d83+d84*d84+d85*d85+d86*d86+d87*d87+d88*d88+d89*d89+d90*d90+d91*d91+d92*d92+d93*d93+d94*d94+d95*d95+d96*d96+d97*d97+d98*d98+d99*d99+d100*d100)) device_m
      from $gender_app2vec_vec2_score_test_2k
      where day = '$day'
      group by device, cate_l1_id
    ) t2 
    on t1.device=t2.device and t1.cate_l1_id=t2.cate_l1_id
    join $dim_gender_app2vec_cate_l1_center_vec_m t3
    on t1.cate_l1_id=t3.cate_l1_id and t1.tag=t3.tag
  ) tz join (select cate_l1_id, index from $dim_gender_app_cate_index1) tx
  on tz.cate_l1_id = tx.cate_l1_id
) z
group by device;
"