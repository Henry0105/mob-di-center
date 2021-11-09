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

gender_feature_v2_part18="${dm_mobdi_tmp}.gender_feature_v2_part18"

#gender_app_tgi_level_5="dm_sdk_mapping.gender_app_tgi_level_5"

#gender_app2vec_tgi5_center2="dm_sdk_mapping.gender_app2vec_tgi5_center2"

#gender_app2vec_tgi_level_vec_m2="dm_sdk_mapping.gender_app2vec_tgi_level_vec_m2"

#apppkg_app2vec_par_wi="rp_mobdi_app.apppkg_app2vec_par_wi"

last_par=20210418

sql="
select device, tgi_level,
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
join (select * from $apppkg_app2vec_par_wi where day = '$last_par') y on x.pkg = y.apppkg
join (select apppkg, tgi_level from $dim_gender_app_tgi_level_5 group by apppkg, tgi_level) z on x.pkg=z.apppkg
group by device, tgi_level
"

#output
gender_app2vec_device_tgi_vec2_score=${dm_mobdi_tmp}.gender_app2vec_device_tgi_vec2_score

spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.newscore.App2Vec \
--driver-memory 4G \
--queue root.yarn_data_compliance1 \
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
--outputTable $gender_app2vec_device_tgi_vec2_score \
--day $day \
--sql "$sql" \
--flag "gender_v2_18_app2vec"


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

insert overwrite table $gender_feature_v2_part18 partition(day='$insertday')
select device, 
max(cos_sim_tgi_male_high) cos_sim_tgi_male_high, 
max(cos_sim_tgi_male) cos_sim_tgi_male,
max(cos_sim_tgi_female_high) cos_sim_tgi_female_high,
max(cos_sim_tgi_female) cos_sim_tgi_female,
max(cos_sim_tgi_normal) cos_sim_tgi_normal,
max(cos_sim_tgi_male_high_female) cos_sim_tgi_male_high_female, 
max(cos_sim_tgi_male_female) cos_sim_tgi_male_female,
max(cos_sim_tgi_female_high_female) cos_sim_tgi_female_high_female,
max(cos_sim_tgi_female_female) cos_sim_tgi_female_female,
max(cos_sim_tgi_normal_female) cos_sim_tgi_normal_female
from (
  select device, 
  case when tgi_level='tgi_male_high' and tag=0 then cos_sim else -99 end cos_sim_tgi_male_high,
  case when tgi_level='tgi_male' and tag=0 then cos_sim else -99 end cos_sim_tgi_male,
  case when tgi_level='tgi_female_high' and tag=0 then cos_sim else -99 end cos_sim_tgi_female_high,
  case when tgi_level='tgi_female' and tag=0 then cos_sim else -99 end cos_sim_tgi_female,
  case when tgi_level='tgi_normal' and tag=0 then cos_sim else -99 end cos_sim_tgi_normal,
  case when tgi_level='tgi_male_high' and tag=1 then cos_sim else -99 end cos_sim_tgi_male_high_female,
  case when tgi_level='tgi_male' and tag=1 then cos_sim else -99 end cos_sim_tgi_male_female,
  case when tgi_level='tgi_female_high' and tag=1 then cos_sim else -99 end cos_sim_tgi_female_high_female,
  case when tgi_level='tgi_female' and tag=1 then cos_sim else -99 end cos_sim_tgi_female_female,
  case when tgi_level='tgi_normal' and tag=1 then cos_sim else -99 end cos_sim_tgi_normal_female
  from (
    select t1.device, t1.tag, t1.tgi_level, 
    t1.distance, t2.device_m, t3.tgi_level_m, distance/device_m/tgi_level_m cos_sim
    from (
      select aa.device, bb.tgi_level, bb.tag,
      sum(aa.d1*bb.d1+aa.d2*bb.d2+aa.d3*bb.d3+aa.d4*bb.d4+aa.d5*bb.d5+aa.d6*bb.d6+aa.d7*bb.d7+aa.d8*bb.d8+aa.d9*bb.d9+aa.d10*bb.d10+aa.d11*bb.d11+aa.d12*bb.d12+aa.d13*bb.d13+aa.d14*bb.d14+aa.d15*bb.d15+aa.d16*bb.d16+aa.d17*bb.d17+aa.d18*bb.d18+aa.d19*bb.d19+aa.d20*bb.d20+aa.d21*bb.d21+aa.d22*bb.d22+aa.d23*bb.d23+aa.d24*bb.d24+aa.d25*bb.d25+aa.d26*bb.d26+aa.d27*bb.d27+aa.d28*bb.d28+aa.d29*bb.d29+aa.d30*bb.d30+aa.d31*bb.d31+aa.d32*bb.d32+aa.d33*bb.d33+aa.d34*bb.d34+aa.d35*bb.d35+aa.d36*bb.d36+aa.d37*bb.d37+aa.d38*bb.d38+aa.d39*bb.d39+aa.d40*bb.d40+aa.d41*bb.d41+aa.d42*bb.d42+aa.d43*bb.d43+aa.d44*bb.d44+aa.d45*bb.d45+aa.d46*bb.d46+aa.d47*bb.d47+aa.d48*bb.d48+aa.d49*bb.d49+aa.d50*bb.d50+aa.d51*bb.d51+aa.d52*bb.d52+aa.d53*bb.d53+aa.d54*bb.d54+aa.d55*bb.d55+aa.d56*bb.d56+aa.d57*bb.d57+aa.d58*bb.d58+aa.d59*bb.d59+aa.d60*bb.d60+aa.d61*bb.d61+aa.d62*bb.d62+aa.d63*bb.d63+aa.d64*bb.d64+aa.d65*bb.d65+aa.d66*bb.d66+aa.d67*bb.d67+aa.d68*bb.d68+aa.d69*bb.d69+aa.d70*bb.d70+aa.d71*bb.d71+aa.d72*bb.d72+aa.d73*bb.d73+aa.d74*bb.d74+aa.d75*bb.d75+aa.d76*bb.d76+aa.d77*bb.d77+aa.d78*bb.d78+aa.d79*bb.d79+aa.d80*bb.d80+aa.d81*bb.d81+aa.d82*bb.d82+aa.d83*bb.d83+aa.d84*bb.d84+aa.d85*bb.d85+aa.d86*bb.d86+aa.d87*bb.d87+aa.d88*bb.d88+aa.d89*bb.d89+aa.d90*bb.d90+aa.d91*bb.d91+aa.d92*bb.d92+aa.d93*bb.d93+aa.d94*bb.d94+aa.d95*bb.d95+aa.d96*bb.d96+aa.d97*bb.d97+aa.d98*bb.d98+aa.d99*bb.d99+aa.d100*bb.d100) distance
      from $gender_app2vec_device_tgi_vec2_score aa
      join $dim_gender_app2vec_tgi5_center2 bb
      on aa.tgi_level=bb.tgi_level and aa.day = '$day'
      group by aa.device, bb.tgi_level, bb.tag
    ) t1 join 
    (
      select device, tgi_level, sqrt(sum(d1*d1+d2*d2+d3*d3+d4*d4+d5*d5+d6*d6+d7*d7+d8*d8+d9*d9+d10*d10+d11*d11+d12*d12+d13*d13+d14*d14+d15*d15+d16*d16+d17*d17+d18*d18+d19*d19+d20*d20+d21*d21+d22*d22+d23*d23+d24*d24+d25*d25+d26*d26+d27*d27+d28*d28+d29*d29+d30*d30+d31*d31+d32*d32+d33*d33+d34*d34+d35*d35+d36*d36+d37*d37+d38*d38+d39*d39+d40*d40+d41*d41+d42*d42+d43*d43+d44*d44+d45*d45+d46*d46+d47*d47+d48*d48+d49*d49+d50*d50+d51*d51+d52*d52+d53*d53+d54*d54+d55*d55+d56*d56+d57*d57+d58*d58+d59*d59+d60*d60+d61*d61+d62*d62+d63*d63+d64*d64+d65*d65+d66*d66+d67*d67+d68*d68+d69*d69+d70*d70+d71*d71+d72*d72+d73*d73+d74*d74+d75*d75+d76*d76+d77*d77+d78*d78+d79*d79+d80*d80+d81*d81+d82*d82+d83*d83+d84*d84+d85*d85+d86*d86+d87*d87+d88*d88+d89*d89+d90*d90+d91*d91+d92*d92+d93*d93+d94*d94+d95*d95+d96*d96+d97*d97+d98*d98+d99*d99+d100*d100)) device_m
      from $gender_app2vec_device_tgi_vec2_score
      where day = '$day'
      group by device, tgi_level
    ) t2 on t1.device=t2.device and t1.tgi_level=t2.tgi_level
    join $dim_gender_app2vec_tgi_level_vec_m2 t3 on t1.tgi_level=t3.tgi_level and t1.tag=t3.tag
  ) x
) y
group by device;
"

#hive -e "alter table $gender_feature_v2_part18 drop partition(day<$p7);"
hive -e "alter table $gender_app2vec_device_tgi_vec2_score drop partition(day<$p7);"
