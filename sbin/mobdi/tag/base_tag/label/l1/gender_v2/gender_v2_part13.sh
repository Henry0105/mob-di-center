#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi
source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
p7=$(date -d "$day -7 days" "+%Y%m%d")

#device_applist_new="dm_mobdi_mapping.device_applist_new"

gender_feature_v2_part13="$dm_mobdi_tmp.gender_feature_v2_part13"

#gender_app_tgi_level_5_filter="dm_sdk_mapping.gender_app_tgi_level_5_filter"

hive -e "
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
with gender_pre_app_index_tgi_male_female_score as (
select device, index,
case when tgi_male_high+tgi_male=0 and tgi_female+tgi_female_high>0 then 0 
when tgi_female+tgi_female_high=0 and tgi_male_high+tgi_male>0 then 2 
when tgi_female+tgi_female_high=0 and tgi_male_high+tgi_male>0 then -1 
else (tgi_male_high+tgi_male)/(cast ((tgi_female+tgi_female_high) as float)) end tgi_male_female 
from(
    select device, index,
    sum(tgi_male_high) tgi_male_high,
    sum(tgi_male) tgi_male,
    sum(tgi_normal) tgi_normal,
    sum(tgi_female) tgi_female,
    sum(tgi_female_high) tgi_female_high
    from (
        select device, index,
        case when tgi_level = 'tgi_male' then cnt else 0 end tgi_male,
        case when tgi_level = 'tgi_male_high' then cnt else 0 end tgi_male_high,
        case when tgi_level =  'tgi_normal' then cnt else 0 end tgi_normal,
        case when tgi_level =  'tgi_female' then cnt else 0 end tgi_female,
        case when tgi_level =  'tgi_female_high' then cnt else 0 end tgi_female_high
        from(
            select t1.device, t2.tgi_level, index, count(*) cnt
            from (select device,pkg apppkg from $dim_device_applist_new_di where day = '$day') t1
            join $dim_gender_app_tgi_level_5_filter t2
            on t1.apppkg=t2.apppkg
            group by t1.device, t2.tgi_level, index
        ) a 
    )b group by device, index
)c
)
insert overwrite table $gender_feature_v2_part13 partition(day=$day)
select device, 
max(cate1_male_female) cate1_male_female,
max(cate2_male_female) cate2_male_female,
max(cate3_male_female) cate3_male_female,
max(cate4_male_female) cate4_male_female,
max(cate5_male_female) cate5_male_female,
max(cate6_male_female) cate6_male_female,
max(cate7_male_female) cate7_male_female,
max(cate8_male_female) cate8_male_female,
max(cate9_male_female) cate9_male_female,
max(cate10_male_female) cate10_male_female,
max(cate11_male_female) cate11_male_female,
max(cate12_male_female) cate12_male_female,
max(cate13_male_female) cate13_male_female,
max(cate14_male_female) cate14_male_female,
max(cate15_male_female) cate15_male_female,
max(cate16_male_female) cate16_male_female,
max(cate17_male_female) cate17_male_female,
max(cate18_male_female) cate18_male_female,
max(cate19_male_female) cate19_male_female,
max(cate20_male_female) cate20_male_female,
max(cate21_male_female) cate21_male_female,
max(cate22_male_female) cate22_male_female,
max(cate23_male_female) cate23_male_female,
max(cate24_male_female) cate24_male_female,
max(cate25_male_female) cate25_male_female,
max(cate26_male_female) cate26_male_female,
max(cate27_male_female) cate27_male_female,
max(cate28_male_female) cate28_male_female,
max(cate29_male_female) cate29_male_female,
max(cate30_male_female) cate30_male_female,
max(cate31_male_female) cate31_male_female,
max(cate32_male_female) cate32_male_female,
max(cate33_male_female) cate33_male_female,
max(cate34_male_female) cate34_male_female,
max(cate35_male_female) cate35_male_female,
max(cate36_male_female) cate36_male_female,
max(cate37_male_female) cate37_male_female,
max(cate38_male_female) cate38_male_female,
max(cate39_male_female) cate39_male_female,
max(cate40_male_female) cate40_male_female,
max(cate41_male_female) cate41_male_female,
max(cate42_male_female) cate42_male_female,
max(cate43_male_female) cate43_male_female,
max(cate44_male_female) cate44_male_female,
max(cate45_male_female) cate45_male_female,
max(cate46_male_female) cate46_male_female,
max(cate47_male_female) cate47_male_female,
max(cate48_male_female) cate48_male_female,
max(cate49_male_female) cate49_male_female,
max(cate50_male_female) cate50_male_female,
max(cate1_avg_tgi)  cate1_avg_tgi,
max(cate2_avg_tgi) cate2_avg_tgi,
max(cate3_avg_tgi) cate3_avg_tgi,
max(cate4_avg_tgi) cate4_avg_tgi,
max(cate5_avg_tgi) cate5_avg_tgi,
max(cate6_avg_tgi) cate6_avg_tgi,
max(cate7_avg_tgi) cate7_avg_tgi,
max(cate8_avg_tgi) cate8_avg_tgi,
max(cate9_avg_tgi) cate9_avg_tgi,
max(cate10_avg_tgi) cate10_avg_tgi,
max(cate11_avg_tgi) cate11_avg_tgi,
max(cate12_avg_tgi) cate12_avg_tgi,
max(cate13_avg_tgi) cate13_avg_tgi,
max(cate14_avg_tgi) cate14_avg_tgi,
max(cate15_avg_tgi) cate15_avg_tgi,
max(cate16_avg_tgi) cate16_avg_tgi,
max(cate17_avg_tgi) cate17_avg_tgi,
max(cate18_avg_tgi) cate18_avg_tgi,
max(cate19_avg_tgi) cate19_avg_tgi,
max(cate20_avg_tgi) cate20_avg_tgi,
max(cate21_avg_tgi) cate21_avg_tgi,
max(cate22_avg_tgi) cate22_avg_tgi,
max(cate23_avg_tgi) cate23_avg_tgi,
max(cate24_avg_tgi) cate24_avg_tgi,
max(cate25_avg_tgi) cate25_avg_tgi,
max(cate26_avg_tgi) cate26_avg_tgi,
max(cate27_avg_tgi) cate27_avg_tgi,
max(cate28_avg_tgi) cate28_avg_tgi,
max(cate29_avg_tgi) cate29_avg_tgi,
max(cate30_avg_tgi) cate30_avg_tgi,
max(cate31_avg_tgi) cate31_avg_tgi,
max(cate32_avg_tgi) cate32_avg_tgi,
max(cate33_avg_tgi) cate33_avg_tgi,
max(cate34_avg_tgi) cate34_avg_tgi,
max(cate35_avg_tgi) cate35_avg_tgi,
max(cate36_avg_tgi) cate36_avg_tgi,
max(cate37_avg_tgi) cate37_avg_tgi,
max(cate38_avg_tgi) cate38_avg_tgi,
max(cate39_avg_tgi) cate39_avg_tgi,
max(cate40_avg_tgi) cate40_avg_tgi,
max(cate41_avg_tgi) cate41_avg_tgi,
max(cate42_avg_tgi) cate42_avg_tgi,
max(cate43_avg_tgi) cate43_avg_tgi,
max(cate44_avg_tgi) cate44_avg_tgi,
max(cate45_avg_tgi) cate45_avg_tgi,
max(cate46_avg_tgi) cate46_avg_tgi,
max(cate47_avg_tgi) cate47_avg_tgi,
max(cate48_avg_tgi) cate48_avg_tgi,
max(cate49_avg_tgi) cate49_avg_tgi,
max(cate50_avg_tgi) cate50_avg_tgi
from (
select t1.device, 
case when t1.index= 75  then coalesce(t1.tgi_male_female,-1)  else -1 end  cate1_male_female,
case when t1.index= 83  then coalesce(t1.tgi_male_female,-1)  else -1 end  cate2_male_female,
case when t1.index= 87  then coalesce(t1.tgi_male_female,-1)  else -1 end  cate3_male_female,
case when t1.index= 89  then coalesce(t1.tgi_male_female,-1)  else -1 end cate4_male_female,
case when t1.index= 93  then coalesce(t1.tgi_male_female,-1)  else -1 end cate5_male_female,
case when t1.index= 97  then coalesce(t1.tgi_male_female,-1)  else -1 end cate6_male_female,
case when t1.index= 115  then coalesce(t1.tgi_male_female,-1)  else -1 end cate7_male_female,
case when t1.index= 116  then coalesce(t1.tgi_male_female,-1)  else -1 end cate8_male_female,
case when t1.index= 120  then coalesce(t1.tgi_male_female,-1)  else -1 end cate9_male_female,
case when t1.index= 125  then coalesce(t1.tgi_male_female,-1)  else -1 end cate10_male_female,
case when t1.index= 130  then coalesce(t1.tgi_male_female,-1)  else -1 end cate11_male_female,
case when t1.index= 132  then coalesce(t1.tgi_male_female,-1)  else -1 end cate12_male_female,
case when t1.index= 137  then coalesce(t1.tgi_male_female,-1)  else -1 end cate13_male_female,
case when t1.index= 159  then coalesce(t1.tgi_male_female,-1)  else -1 end cate14_male_female,
case when t1.index= 160  then coalesce(t1.tgi_male_female,-1)  else -1 end cate15_male_female,
case when t1.index= 161  then coalesce(t1.tgi_male_female,-1)  else -1 end cate16_male_female,
case when t1.index= 164  then coalesce(t1.tgi_male_female,-1)  else -1 end cate17_male_female,
case when t1.index= 165  then coalesce(t1.tgi_male_female,-1)  else -1 end cate18_male_female,
case when t1.index= 166  then coalesce(t1.tgi_male_female,-1)  else -1 end cate19_male_female,
case when t1.index= 167  then coalesce(t1.tgi_male_female,-1)  else -1 end cate20_male_female,
case when t1.index= 169  then coalesce(t1.tgi_male_female,-1)  else -1 end cate21_male_female,
case when t1.index= 173  then coalesce(t1.tgi_male_female,-1)  else -1 end cate22_male_female,
case when t1.index= 175  then coalesce(t1.tgi_male_female,-1)  else -1 end cate23_male_female,
case when t1.index= 185  then coalesce(t1.tgi_male_female,-1)  else -1 end cate24_male_female,
case when t1.index= 188  then coalesce(t1.tgi_male_female,-1)  else -1 end cate25_male_female,
case when t1.index= 190  then coalesce(t1.tgi_male_female,-1)  else -1 end cate26_male_female,
case when t1.index= 193  then coalesce(t1.tgi_male_female,-1)  else -1 end cate27_male_female,
case when t1.index= 194  then coalesce(t1.tgi_male_female,-1)  else -1 end cate28_male_female,
case when t1.index= 197  then coalesce(t1.tgi_male_female,-1)  else -1 end cate29_male_female,
case when t1.index= 201  then coalesce(t1.tgi_male_female,-1)  else -1 end cate30_male_female,
case when t1.index= 203  then coalesce(t1.tgi_male_female,-1)  else -1 end cate31_male_female,
case when t1.index= 220  then coalesce(t1.tgi_male_female,-1)  else -1 end cate32_male_female,
case when t1.index= 225  then coalesce(t1.tgi_male_female,-1)  else -1 end cate33_male_female,
case when t1.index= 231  then coalesce(t1.tgi_male_female,-1)  else -1 end cate34_male_female,
case when t1.index= 232  then coalesce(t1.tgi_male_female,-1)  else -1 end cate35_male_female,
case when t1.index= 235  then coalesce(t1.tgi_male_female,-1)  else -1 end cate36_male_female,
case when t1.index= 236  then coalesce(t1.tgi_male_female,-1)  else -1 end cate37_male_female,
case when t1.index= 238  then coalesce(t1.tgi_male_female,-1)  else -1 end cate38_male_female,
case when t1.index= 239  then coalesce(t1.tgi_male_female,-1)  else -1 end cate39_male_female,
case when t1.index= 240  then coalesce(t1.tgi_male_female,-1)  else -1 end cate40_male_female,
case when t1.index= 243  then coalesce(t1.tgi_male_female,-1)  else -1 end cate41_male_female,
case when t1.index= 249  then coalesce(t1.tgi_male_female,-1)  else -1 end cate42_male_female,
case when t1.index= 250  then coalesce(t1.tgi_male_female,-1)  else -1 end cate43_male_female,
case when t1.index= 251  then coalesce(t1.tgi_male_female,-1)  else -1 end cate44_male_female,
case when t1.index= 253  then coalesce(t1.tgi_male_female,-1)  else -1 end cate45_male_female,
case when t1.index= 261  then coalesce(t1.tgi_male_female,-1)  else -1 end cate46_male_female,
case when t1.index= 270  then coalesce(t1.tgi_male_female,-1)  else -1 end cate47_male_female,
case when t1.index= 274  then coalesce(t1.tgi_male_female,-1)  else -1 end cate48_male_female,
case when t1.index= 280  then coalesce(t1.tgi_male_female,-1)  else -1 end cate49_male_female,
case when t1.index= 282  then coalesce(t1.tgi_male_female,-1)  else -1 end cate50_male_female,
case when t2.index= 75 then t2.avg_tgi else -1 end cate1_avg_tgi,
case when t2.index= 83 then t2.avg_tgi else -1 end cate2_avg_tgi,
case when t2.index= 87 then t2.avg_tgi else -1 end cate3_avg_tgi,
case when t2.index= 89 then t2.avg_tgi else -1 end cate4_avg_tgi,
case when t2.index= 93 then t2.avg_tgi else -1 end cate5_avg_tgi,
case when t2.index= 97 then t2.avg_tgi else -1 end cate6_avg_tgi,
case when t2.index= 115 then t2.avg_tgi else -1 end cate7_avg_tgi,
case when t2.index= 116 then t2.avg_tgi else -1 end cate8_avg_tgi,
case when t2.index= 120 then t2.avg_tgi else -1 end cate9_avg_tgi,
case when t2.index= 125 then t2.avg_tgi else -1 end cate10_avg_tgi,
case when t2.index= 130 then t2.avg_tgi else -1 end cate11_avg_tgi,
case when t2.index= 132 then t2.avg_tgi else -1 end cate12_avg_tgi,
case when t2.index= 137 then t2.avg_tgi else -1 end cate13_avg_tgi,
case when t2.index= 159 then t2.avg_tgi else -1 end cate14_avg_tgi,
case when t2.index= 160 then t2.avg_tgi else -1 end cate15_avg_tgi,
case when t2.index= 161 then t2.avg_tgi else -1 end cate16_avg_tgi,
case when t2.index= 164 then t2.avg_tgi else -1 end cate17_avg_tgi,
case when t2.index= 165 then t2.avg_tgi else -1 end cate18_avg_tgi,
case when t2.index= 166 then t2.avg_tgi else -1 end cate19_avg_tgi,
case when t2.index= 167 then t2.avg_tgi else -1 end cate20_avg_tgi,
case when t2.index= 169 then t2.avg_tgi else -1 end cate21_avg_tgi,
case when t2.index= 173 then t2.avg_tgi else -1 end cate22_avg_tgi,
case when t2.index= 175 then t2.avg_tgi else -1 end cate23_avg_tgi,
case when t2.index= 185 then t2.avg_tgi else -1 end cate24_avg_tgi,
case when t2.index= 188 then t2.avg_tgi else -1 end cate25_avg_tgi,
case when t2.index= 190 then t2.avg_tgi else -1 end cate26_avg_tgi,
case when t2.index= 193 then t2.avg_tgi else -1 end cate27_avg_tgi,
case when t2.index= 194 then t2.avg_tgi else -1 end cate28_avg_tgi,
case when t2.index= 197 then t2.avg_tgi else -1 end cate29_avg_tgi,
case when t2.index= 201 then t2.avg_tgi else -1 end cate30_avg_tgi,
case when t2.index= 203 then t2.avg_tgi else -1 end cate31_avg_tgi,
case when t2.index= 220 then t2.avg_tgi else -1 end cate32_avg_tgi,
case when t2.index= 225 then t2.avg_tgi else -1 end cate33_avg_tgi,
case when t2.index= 231 then t2.avg_tgi else -1 end cate34_avg_tgi,
case when t2.index= 232 then t2.avg_tgi else -1 end cate35_avg_tgi,
case when t2.index= 235 then t2.avg_tgi else -1 end cate36_avg_tgi,
case when t2.index= 236 then t2.avg_tgi else -1 end cate37_avg_tgi,
case when t2.index= 238 then t2.avg_tgi else -1 end cate38_avg_tgi,
case when t2.index= 239 then t2.avg_tgi else -1 end cate39_avg_tgi,
case when t2.index= 240 then t2.avg_tgi else -1 end cate40_avg_tgi,
case when t2.index= 243 then t2.avg_tgi else -1 end cate41_avg_tgi,
case when t2.index= 249 then t2.avg_tgi else -1 end cate42_avg_tgi,
case when t2.index= 250 then t2.avg_tgi else -1 end cate43_avg_tgi,
case when t2.index= 251 then t2.avg_tgi else -1 end cate44_avg_tgi,
case when t2.index= 253 then t2.avg_tgi else -1 end cate45_avg_tgi,
case when t2.index= 261 then t2.avg_tgi else -1 end cate46_avg_tgi,
case when t2.index= 270 then t2.avg_tgi else -1 end cate47_avg_tgi,
case when t2.index= 274 then t2.avg_tgi else -1 end cate48_avg_tgi,
case when t2.index= 280 then t2.avg_tgi else -1 end cate49_avg_tgi,
case when t2.index= 282 then t2.avg_tgi else -1 end cate50_avg_tgi
from gender_pre_app_index_tgi_male_female_score t1 
join (
    select t1.device, index, sum(tgi) sum_tgi, 
    count(*) cnt, avg(tgi) avg_tgi
    from (select device,pkg apppkg from $dim_device_applist_new_di where day = '$day') t1
    join $dim_gender_app_tgi_level_5_filter t2 on t1.apppkg=t2.apppkg
    group by t1.device, index
) t2 
on t1.device=t2.device)tt
group by device;
"

hive -e "alter table $gender_feature_v2_part13 drop partition(day<$p7);"
