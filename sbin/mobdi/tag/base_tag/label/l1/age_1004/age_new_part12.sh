#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi

day=$1
day_before_one_month=$(date -d "${day} -1 month" "+%Y%m%d")
source /home/dba/mobdi_center/conf/hive-env.sh
insertday=${day}_muid
#label_device_pkg_install_uninstall_year_info_mf="rp_mobdi_app.label_device_pkg_install_uninstall_year_info_mf"

tmpdb=$dm_mobdi_tmp
age_new_uninstall_avg_embedding="${tmpdb}.age_new_uninstall_avg_embedding"
age_new_uninstall_avg_embedding_cosin_temp="${tmpdb}.age_new_uninstall_avg_embedding_cosin_temp"
age_new_embedding_cosin_bycate="${tmpdb}.age_new_embedding_cosin_bycate"

#apppkg_app2vec_par_wi="rp_mobdi_app.apppkg_app2vec_par_wi"

#dim_age_cate_avg_embedding_all=dim_mobdi_mapping.dim_age_cate_avg_embedding_all
#age_cate_avg_embedding_all="dm_mobdi_mapping.age_cate_avg_embedding_all"

skew_pkgs_num=500
skew_pkgs_sql="
select collect_list(pkg) pkgs from (
select pkg,count(*) cnt from $label_device_pkg_install_uninstall_year_info_mf
where day = '$day' and pkg is not null and trim(pkg) <>'' group by pkg
order by cnt DESC limit $skew_pkgs_num
) t
"
skew_pkgs_arr=$(hive -e "$skew_pkgs_sql")
skew_pkgs=${skew_pkgs_arr//[\[\]]}

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
use dm_mobdi_tmp;
create temporary table apppkg_app2vec_par_wi_big_age_part12_$day as
select * from $apppkg_app2vec_par_wi where day='20210418' and apppkg in ($skew_pkgs);

create temporary table raw_table_small_$day as 
select device,pkg,update_day
from $label_device_pkg_install_uninstall_year_info_mf
where day='${day}' and update_day between '${day_before_one_month}'  and '${day}'
and refine_final_flag in (0,1) and (pkg is null or pkg not in ($skew_pkgs)) 
group by device,pkg,update_day;

create temporary table raw_table_skew_$day as 
select device,pkg,update_day
from $label_device_pkg_install_uninstall_year_info_mf
where day='${day}' and update_day between '${day_before_one_month}'  and '${day}'
and refine_final_flag in (0,1) and pkg in ($skew_pkgs)
group by device,pkg,update_day;

insert overwrite table $age_new_uninstall_avg_embedding partition (day='${insertday}')
select device,
avg(d1) as d1,avg(d2) as d2,avg(d3) as d3,avg(d4) as d4,avg(d5) as d5,avg(d6) as d6,avg(d7) as d7,avg(d8) as d8,avg(d9) as d9,avg(d10) as d10,
avg(d11) as d11,avg(d12) as d12,avg(d13) as d13,avg(d14) as d14,avg(d15) as d15,avg(d16) as d16,avg(d17) as d17,avg(d18) as d18,avg(d19) as d19,avg(d20) as d20,
avg(d21) as d21,avg(d22) as d22,avg(d23) as d23,avg(d24) as d24,avg(d25) as d25,avg(d26) as d26,avg(d27) as d27,avg(d28) as d28,avg(d29) as d29,avg(d30) as d30,
avg(d31) as d31,avg(d32) as d32,avg(d33) as d33,avg(d34) as d34,avg(d35) as d35,avg(d36) as d36,avg(d37) as d37,avg(d38) as d38,avg(d39) as d39,avg(d40) as d40,
avg(d41) as d41,avg(d42) as d42,avg(d43) as d43,avg(d44) as d44,avg(d45) as d45,avg(d46) as d46,avg(d47) as d47,avg(d48) as d48,avg(d49) as d49,avg(d50) as d50,
avg(d51) as d51,avg(d52) as d52,avg(d53) as d53,avg(d54) as d54,avg(d55) as d55,avg(d56) as d56,avg(d57) as d57,avg(d58) as d58,avg(d59) as d59,avg(d60) as d60,
avg(d61) as d61,avg(d62) as d62,avg(d63) as d63,avg(d64) as d64,avg(d65) as d65,avg(d66) as d66,avg(d67) as d67,avg(d68) as d68,avg(d69) as d69,avg(d70) as d70,
avg(d71) as d71,avg(d72) as d72,avg(d73) as d73,avg(d74) as d74,avg(d75) as d75,avg(d76) as d76,avg(d77) as d77,avg(d78) as d78,avg(d79) as d79,avg(d80) as d80,
avg(d81) as d81,avg(d82) as d82,avg(d83) as d83,avg(d84) as d84,avg(d85) as d85,avg(d86) as d86,avg(d87) as d87,avg(d88) as d88,avg(d89) as d89,avg(d90) as d90,
avg(d91) as d91,avg(d92) as d92,avg(d93) as d93,avg(d94) as d94,avg(d95) as d95,avg(d96) as d96,avg(d97) as d97,avg(d98) as d98,avg(d99) as d99,avg(d100) as d100,
update_day
from(
select * from raw_table_small_$day a 
inner join( select * from $apppkg_app2vec_par_wi where day='20210418' )b 
on trim(a.pkg)=trim(b.apppkg)

union all

select /*+ MAPJOIN(b) */ * from raw_table_skew_$day a 
inner join apppkg_app2vec_par_wi_big_age_part12_$day b 
on trim(a.pkg)=trim(b.apppkg)
) t1 

group by device,update_day
"

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

insert overwrite table $age_new_uninstall_avg_embedding_cosin_temp partition (day='${insertday}')
select /*+ MAPJOIN(a) */ device,a.cate_id,a.label,(a.d1* b.d1+a.d2* b.d2+a.d3* b.d3+a.d4* b.d4+a.d5* b.d5+a.d6* b.d6+a.d7* b.d7+a.d8* b.d8+a.d9* b.d9+a.d10* b.d10+a.d11* b.d11+a.d12* b.d12+a.d13* b.d13+a.d14* b.d14+a.d15* b.d15+a.d16* b.d16+a.d17* b.d17+a.d18* b.d18+a.d19* b.d19+a.d20* b.d20+a.d21* b.d21+a.d22* b.d22+a.d23* b.d23+a.d24* b.d24+a.d25* b.d25+a.d26* b.d26+a.d27* b.d27+a.d28* b.d28+a.d29* b.d29+a.d30* b.d30+a.d31* b.d31+a.d32* b.d32+a.d33* b.d33+a.d34* b.d34+a.d35* b.d35+a.d36* b.d36+a.d37* b.d37+a.d38* b.d38+a.d39* b.d39+a.d40* b.d40+a.d41* b.d41+a.d42* b.d42+a.d43* b.d43+a.d44* b.d44+a.d45* b.d45+a.d46* b.d46+a.d47* b.d47+a.d48* b.d48+a.d49* b.d49+a.d50* b.d50+a.d51* b.d51+a.d52* b.d52+a.d53* b.d53+a.d54* b.d54+a.d55* b.d55+a.d56* b.d56+a.d57* b.d57+a.d58* b.d58+a.d59* b.d59+a.d60* b.d60+a.d61* b.d61+a.d62* b.d62+a.d63* b.d63+a.d64* b.d64+a.d65* b.d65+a.d66* b.d66+a.d67* b.d67+a.d68* b.d68+a.d69* b.d69+a.d70* b.d70+a.d71* b.d71+a.d72* b.d72+a.d73* b.d73+a.d74* b.d74+a.d75* b.d75+a.d76* b.d76+a.d77* b.d77+a.d78* b.d78+a.d79* b.d79+a.d80* b.d80+a.d81* b.d81+a.d82* b.d82+a.d83* b.d83+a.d84* b.d84+a.d85* b.d85+a.d86* b.d86+a.d87* b.d87+a.d88* b.d88+a.d89* b.d89+a.d90* b.d90+a.d91* b.d91+a.d92* b.d92+a.d93* b.d93+a.d94* b.d94+a.d95* b.d95+a.d96* b.d96+a.d97* b.d97+a.d98* b.d98+a.d99* b.d99+a.d100* b.d100
)/(cate_vector_length*device_vector_length) as cosin_similarity,update_day
from( 
    select *, sqrt(d1*d1+d2*d2+d3*d3+d4*d4+d5*d5+d6*d6+d7*d7+d8*d8+d9*d9+d10*d10+d11*d11+d12*d12+d13*d13+d14*d14+d15*d15+d16*d16+d17*d17+d18*d18+d19*d19+d20*d20+d21*d21+d22*d22+d23*d23+d24*d24+d25*d25+d26*d26+d27*d27+d28*d28+d29*d29+d30*d30+d31*d31+d32*d32+d33*d33+d34*d34+d35*d35+d36*d36+d37*d37+d38*d38+d39*d39+d40*d40+d41*d41+d42*d42+d43*d43+d44*d44+d45*d45+d46*d46+d47*d47+d48*d48+d49*d49+d50*d50+d51*d51+d52*d52+d53*d53+d54*d54+d55*d55+d56*d56+d57*d57+d58*d58+d59*d59+d60*d60+d61*d61+d62*d62+d63*d63+d64*d64+d65*d65+d66*d66+d67*d67+d68*d68+d69*d69+d70*d70+d71*d71+d72*d72+d73*d73+d74*d74+d75*d75+d76*d76+d77*d77+d78*d78+d79*d79+d80*d80+d81*d81+d82*d82+d83*d83+d84*d84+d85*d85+d86*d86+d87*d87+d88*d88+d89*d89+d90*d90+d91*d91+d92*d92+d93*d93+d94*d94+d95*d95+d96*d96+d97*d97+d98*d98+d99*d99+d100*d100) as device_vector_length
    from $age_new_uninstall_avg_embedding
    where day='${insertday}' and update_day between '${day_before_one_month}'  and '${day}'
    )b 
cross join ( select * from  $dim_age_cate_avg_embedding_all )a;
"

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
insert overwrite table $age_new_embedding_cosin_bycate partition (day='${insertday}')
    select device
,max(case when cate_id='cate1' and label='18以下' then cosin_similarity else -1 end ) as cate1_18_install_trend
,max(case when cate_id='cate10' and label='18-24岁' then cosin_similarity else -1 end ) as cate10_18_24_install_trend
,max(case when cate_id='cate11' and label='0' then cosin_similarity else -1 end ) as cate11_install_trend
,max(case when cate_id='cate15' and label='55岁以上' then cosin_similarity else -1 end ) as cate15_55_install_trend
,max(case when cate_id='cate16' and label='35-44岁' then cosin_similarity else -1 end ) as cate16_35_44_install_trend
,max(case when cate_id='cate18' and label='55岁以上' then cosin_similarity else -1 end ) as cate18_55_install_trend
,max(case when cate_id='cate2' and label='55岁以上' then cosin_similarity else -1 end ) as cate2_55_install_trend
,max(case when cate_id='cate21' and label='45-54岁' then cosin_similarity else -1 end ) as cate21_45_54_install_trend
,max(case when cate_id='cate23' and label='0' then cosin_similarity else -1 end ) as cate23_install_trend
,max(case when cate_id='cate24' and label='0' then cosin_similarity else -1 end ) as cate24_install_trend
,max(case when cate_id='cate3' and label='18以下' then cosin_similarity else -1 end ) as cate3_18_install_trend
,max(case when cate_id='cate5' and label='55岁以上' then cosin_similarity else -1 end ) as cate5_55_install_trend
,max(case when cate_id='cate6' and label='55岁以上' then cosin_similarity else -1 end ) as cate6_55_install_trend
,max(case when cate_id='cate7' and label='55岁以上' then cosin_similarity else -1 end ) as cate7_55_install_trend
,max(case when cate_id='cate7001_011' and label='55岁以上' then cosin_similarity else -1 end ) as cate7001_011_55_install_trend
,max(case when cate_id='cate7002_001' and label='45-54岁' then cosin_similarity else -1 end ) as cate7002_001_45_54_install_trend
,max(case when cate_id='cate7002_002' and label='18以下' then cosin_similarity else -1 end ) as cate7002_002_18_install_trend
,max(case when cate_id='cate7002_003' and label='25-34岁' then cosin_similarity else -1 end ) as cate7002_003_25_34_install_trend
,max(case when cate_id='cate7002_004' and label='55岁以上' then cosin_similarity else -1 end ) as cate7002_004_55_install_trend
,max(case when cate_id='cate7002_005' and label='55岁以上' then cosin_similarity else -1 end ) as cate7002_005_55_install_trend
,max(case when cate_id='cate7002_007' and label='18以下' then cosin_similarity else -1 end ) as cate7002_007_18_install_trend
,max(case when cate_id='cate7002_008' and label='18-24岁' then cosin_similarity else -1 end ) as cate7002_008_18_24_install_trend
,max(case when cate_id='cate7002_010' and label='18-24岁' then cosin_similarity else -1 end ) as cate7002_010_18_24_install_trend
,max(case when cate_id='cate7002_010' and label='0' then cosin_similarity else -1 end ) as cate7002_010_install_trend
,max(case when cate_id='cate7003_002' and label='25-34岁' then cosin_similarity else -1 end ) as cate7003_002_25_34_install_trend
,max(case when cate_id='cate7003_004' and label='0' then cosin_similarity else -1 end ) as cate7003_004_install_trend
,max(case when cate_id='cate7003_006' and label='18-24岁' then cosin_similarity else -1 end ) as cate7003_006_18_24_install_trend
,max(case when cate_id='cate7003_006' and label='55岁以上' then cosin_similarity else -1 end ) as cate7003_006_55_install_trend
,max(case when cate_id='cate7003_008' and label='55岁以上' then cosin_similarity else -1 end ) as cate7003_008_55_install_trend
,max(case when cate_id='cate7003_008' and label='18以下' then cosin_similarity else -1 end ) as cate7003_008_18_install_trend
,max(case when cate_id='cate7004_002' and label='25-34岁' then cosin_similarity else -1 end ) as cate7004_002_25_34_install_trend
,max(case when cate_id='cate7004_003' and label='55岁以上' then cosin_similarity else -1 end ) as cate7004_003_55_install_trend
,max(case when cate_id='cate7004_004' and label='18-24岁' then cosin_similarity else -1 end ) as cate7004_004_18_24_install_trend
,max(case when cate_id='cate7004_006' and label='18以下' then cosin_similarity else -1 end ) as cate7004_006_18_install_trend
,max(case when cate_id='cate7004_007' and label='18以下' then cosin_similarity else -1 end ) as cate7004_007_18_install_trend
,max(case when cate_id='cate7005_001' and label='0' then cosin_similarity else -1 end ) as cate7005_001_install_trend
,max(case when cate_id='cate7005_002' and label='55岁以上' then cosin_similarity else -1 end ) as cate7005_002_55_install_trend
,max(case when cate_id='cate7005_003' and label='25-34岁' then cosin_similarity else -1 end ) as cate7005_003_25_34_install_trend
,max(case when cate_id='cate7005_005' and label='0' then cosin_similarity else -1 end ) as cate7005_005_install_trend
,max(case when cate_id='cate7005_007' and label='25-34岁' then cosin_similarity else -1 end ) as cate7005_007_25_34_install_trend
,max(case when cate_id='cate7005_008' and label='55岁以上' then cosin_similarity else -1 end ) as cate7005_008_55_install_trend
,max(case when cate_id='cate7006_001' and label='18-24岁' then cosin_similarity else -1 end ) as cate7006_001_18_24_install_trend
,max(case when cate_id='cate7006_003' and label='35-44岁' then cosin_similarity else -1 end ) as cate7006_003_35_44_install_trend
,max(case when cate_id='cate7006_005' and label='25-34岁' then cosin_similarity else -1 end ) as cate7006_005_25_34_install_trend
,max(case when cate_id='cate7007_001' and label='18-24岁' then cosin_similarity else -1 end ) as cate7007_001_18_24_install_trend
,max(case when cate_id='cate7007_002' and label='35-44岁' then cosin_similarity else -1 end ) as cate7007_002_35_44_install_trend
,max(case when cate_id='cate7007_003' and label='18-24岁' then cosin_similarity else -1 end ) as cate7007_003_18_24_install_trend
,max(case when cate_id='cate7007_005' and label='55岁以上' then cosin_similarity else -1 end ) as cate7007_005_55_install_trend
,max(case when cate_id='cate7008_001' and label='55岁以上' then cosin_similarity else -1 end ) as cate7008_001_55_install_trend
,max(case when cate_id='cate7008_001' and label='0' then cosin_similarity else -1 end ) as cate7008_001_install_trend
,max(case when cate_id='cate7008_004' and label='18-24岁' then cosin_similarity else -1 end ) as cate7008_004_18_24_install_trend
,max(case when cate_id='cate7008_006' and label='55岁以上' then cosin_similarity else -1 end ) as cate7008_006_55_install_trend
,max(case when cate_id='cate7008_007' and label='0' then cosin_similarity else -1 end ) as cate7008_007_install_trend
,max(case when cate_id='cate7008_008' and label='18以下' then cosin_similarity else -1 end ) as cate7008_008_18_install_trend
,max(case when cate_id='cate7008_009' and label='18以下' then cosin_similarity else -1 end ) as cate7008_009_18_install_trend
,max(case when cate_id='cate7009_002' and label='35-44岁' then cosin_similarity else -1 end ) as cate7009_002_35_44_install_trend
,max(case when cate_id='cate7009_003' and label='35-44岁' then cosin_similarity else -1 end ) as cate7009_003_35_44_install_trend
,max(case when cate_id='cate7009_005' and label='55岁以上' then cosin_similarity else -1 end ) as cate7009_005_55_install_trend
,max(case when cate_id='cate7009_007' and label='18以下' then cosin_similarity else -1 end ) as cate7009_007_18_install_trend
,max(case when cate_id='cate7010_001' and label='35-44岁' then cosin_similarity else -1 end ) as cate7010_001_35_44_install_trend
,max(case when cate_id='cate7010_003' and label='18-24岁' then cosin_similarity else -1 end ) as cate7010_003_18_24_install_trend
,max(case when cate_id='cate7010_005' and label='18以下' then cosin_similarity else -1 end ) as cate7010_005_18_install_trend
,max(case when cate_id='cate7010_006' and label='55岁以上' then cosin_similarity else -1 end ) as cate7010_006_55_install_trend
,max(case when cate_id='cate7011_991' and label='18-24岁' then cosin_similarity else -1 end ) as cate7011_991_18_24_install_trend
,max(case when cate_id='cate7011_993' and label='18以下' then cosin_similarity else -1 end ) as cate7011_993_18_install_trend
,max(case when cate_id='cate7011_998' and label='55岁以上' then cosin_similarity else -1 end ) as cate7011_998_55_install_trend
,max(case when cate_id='cate7011_999' and label='18-24岁' then cosin_similarity else -1 end ) as cate7011_999_18_24_install_trend
,max(case when cate_id='cate7012_002' and label='0' then cosin_similarity else -1 end ) as cate7012_002_install_trend
,max(case when cate_id='cate7012_005' and label='35-44岁' then cosin_similarity else -1 end ) as cate7012_005_35_44_install_trend
,max(case when cate_id='cate7012_008' and label='18-24岁' then cosin_similarity else -1 end ) as cate7012_008_18_24_install_trend
,max(case when cate_id='cate7012_008' and label='0' then cosin_similarity else -1 end ) as cate7012_008_install_trend
,max(case when cate_id='cate7012_009' and label='18以下' then cosin_similarity else -1 end ) as cate7012_009_18_install_trend
,max(case when cate_id='cate7012_012' and label='18以下' then cosin_similarity else -1 end ) as cate7012_012_18_install_trend
,max(case when cate_id='cate7013_001' and label='25-34岁' then cosin_similarity else -1 end ) as cate7013_001_25_34_install_trend
,max(case when cate_id='cate7013_006' and label='45-54岁' then cosin_similarity else -1 end ) as cate7013_006_45_54_install_trend
,max(case when cate_id='cate7013_007' and label='55岁以上' then cosin_similarity else -1 end ) as cate7013_007_55_install_trend
,max(case when cate_id='cate7014_002' and label='35-44岁' then cosin_similarity else -1 end ) as cate7014_002_35_44_install_trend
,max(case when cate_id='cate7014_002' and label='0' then cosin_similarity else -1 end ) as cate7014_002_install_trend
,max(case when cate_id='cate7014_006' and label='18以下' then cosin_similarity else -1 end ) as cate7014_006_18_install_trend
,max(case when cate_id='cate7014_007' and label='0' then cosin_similarity else -1 end ) as cate7014_007_install_trend
,max(case when cate_id='cate7014_008' and label='18以下' then cosin_similarity else -1 end ) as cate7014_008_18_install_trend
,max(case when cate_id='cate7014_011' and label='18以下' then cosin_similarity else -1 end ) as cate7014_011_18_install_trend
,max(case when cate_id='cate7014_014' and label='45-54岁' then cosin_similarity else -1 end ) as cate7014_014_45_54_install_trend
,max(case when cate_id='cate7014_015' and label='35-44岁' then cosin_similarity else -1 end ) as cate7014_015_35_44_install_trend
,max(case when cate_id='cate7014_016' and label='55岁以上' then cosin_similarity else -1 end ) as cate7014_016_55_install_trend
,max(case when cate_id='cate7015_001' and label='25-34岁' then cosin_similarity else -1 end ) as cate7015_001_25_34_install_trend
,max(case when cate_id='cate7015_004' and label='18以下' then cosin_similarity else -1 end ) as cate7015_004_18_install_trend
,max(case when cate_id='cate7015_005' and label='0' then cosin_similarity else -1 end ) as cate7015_005_install_trend
,max(case when cate_id='cate7015_006' and label='55岁以上' then cosin_similarity else -1 end ) as cate7015_006_55_install_trend
,max(case when cate_id='cate7015_007' and label='35-44岁' then cosin_similarity else -1 end ) as cate7015_007_35_44_install_trend
,max(case when cate_id='cate7015_010' and label='18以下' then cosin_similarity else -1 end ) as cate7015_010_18_install_trend
,max(case when cate_id='cate7015_015' and label='25-34岁' then cosin_similarity else -1 end ) as cate7015_015_25_34_install_trend
,max(case when cate_id='cate7015_016' and label='18以下' then cosin_similarity else -1 end ) as cate7015_016_18_install_trend
,max(case when cate_id='cate7015_019' and label='25-34岁' then cosin_similarity else -1 end ) as cate7015_019_25_34_install_trend
,max(case when cate_id='cate7015_020' and label='18以下' then cosin_similarity else -1 end ) as cate7015_020_18_install_trend
,max(case when cate_id='cate7015_021' and label='18以下' then cosin_similarity else -1 end ) as cate7015_021_18_install_trend
,max(case when cate_id='cate7016_003' and label='18-24岁' then cosin_similarity else -1 end ) as cate7016_003_18_24_install_trend
,max(case when cate_id='cate7016_004' and label='18-24岁' then cosin_similarity else -1 end ) as cate7016_004_18_24_install_trend
,max(case when cate_id='cate7016_004' and label='55岁以上' then cosin_similarity else -1 end ) as cate7016_004_55_install_trend
,max(case when cate_id='cate7017_001' and label='55岁以上' then cosin_similarity else -1 end ) as cate7017_001_55_install_trend
,max(case when cate_id='cate7017_002' and label='55岁以上' then cosin_similarity else -1 end ) as cate7017_002_55_install_trend
,max(case when cate_id='cate7018_003' and label='18-24岁' then cosin_similarity else -1 end ) as cate7018_003_18_24_install_trend
,max(case when cate_id='cate7019_101' and label='18-24岁' then cosin_similarity else -1 end ) as cate7019_101_18_24_install_trend
,max(case when cate_id='cate7019_102' and label='25-34岁' then cosin_similarity else -1 end ) as cate7019_102_25_34_install_trend
,max(case when cate_id='cate7019_107' and label='45-54岁' then cosin_similarity else -1 end ) as cate7019_107_45_54_install_trend
,max(case when cate_id='cate7019_109' and label='18以下' then cosin_similarity else -1 end ) as cate7019_109_18_install_trend
,max(case when cate_id='cate7019_110' and label='18-24岁' then cosin_similarity else -1 end ) as cate7019_110_18_24_install_trend
,max(case when cate_id='cate7019_111' and label='18-24岁' then cosin_similarity else -1 end ) as cate7019_111_18_24_install_trend
,max(case when cate_id='cate7019_114' and label='18以下' then cosin_similarity else -1 end ) as cate7019_114_18_install_trend
,max(case when cate_id='cate7019_115' and label='18以下' then cosin_similarity else -1 end ) as cate7019_115_18_install_trend
,max(case when cate_id='cate7019_116' and label='18-24岁' then cosin_similarity else -1 end ) as cate7019_116_18_24_install_trend
,max(case when cate_id='cate7019_116' and label='25-34岁' then cosin_similarity else -1 end ) as cate7019_116_25_34_install_trend
,max(case when cate_id='cate7019_117' and label='18-24岁' then cosin_similarity else -1 end ) as cate7019_117_18_24_install_trend
,max(case when cate_id='cate7019_118' and label='18以下' then cosin_similarity else -1 end ) as cate7019_118_18_install_trend
,max(case when cate_id='cate7019_119' and label='18以下' then cosin_similarity else -1 end ) as cate7019_119_18_install_trend
,max(case when cate_id='cate7019_119' and label='25-34岁' then cosin_similarity else -1 end ) as cate7019_119_25_34_install_trend
,max(case when cate_id='cate7019_120' and label='18以下' then cosin_similarity else -1 end ) as cate7019_120_18_install_trend
,max(case when cate_id='cate7019_121' and label='55岁以上' then cosin_similarity else -1 end ) as cate7019_121_55_install_trend
,max(case when cate_id='cate7019_121' and label='0' then cosin_similarity else -1 end ) as cate7019_121_install_trend
,max(case when cate_id='cate7019_124' and label='18-24岁' then cosin_similarity else -1 end ) as cate7019_124_18_24_install_trend
,max(case when cate_id='cate7019_126' and label='18以下' then cosin_similarity else -1 end ) as cate7019_126_18_install_trend
,max(case when cate_id='cate7019_127' and label='55岁以上' then cosin_similarity else -1 end ) as cate7019_127_55_install_trend
,max(case when cate_id='cate7019_128' and label='18-24岁' then cosin_similarity else -1 end ) as cate7019_128_18_24_install_trend
,max(case when cate_id='cate7019_130' and label='45-54岁' then cosin_similarity else -1 end ) as cate7019_130_45_54_install_trend
,max(case when cate_id='cate7019_132' and label='18-24岁' then cosin_similarity else -1 end ) as cate7019_132_18_24_install_trend
,max(case when cate_id='cate7019_133' and label='18-24岁' then cosin_similarity else -1 end ) as cate7019_133_18_24_install_trend
,max(case when cate_id='cate7019_135' and label='45-54岁' then cosin_similarity else -1 end ) as cate7019_135_45_54_install_trend
,max(case when cate_id='cate7019_137' and label='18-24岁' then cosin_similarity else -1 end ) as cate7019_137_18_24_install_trend
,max(case when cate_id='cate7019_139' and label='45-54岁' then cosin_similarity else -1 end ) as cate7019_139_45_54_install_trend
,max(case when cate_id='cate8' and label='35-44岁' then cosin_similarity else -1 end ) as cate8_35_44_install_trend
,max(case when cate_id='fin_25' and label='55岁以上' then cosin_similarity else -1 end ) as fin_25_55_install_trend
,max(case when cate_id='fin_31' and label='35-44岁' then cosin_similarity else -1 end ) as fin_31_35_44_install_trend
,max(case when cate_id='fin_31' and label='55岁以上' then cosin_similarity else -1 end ) as fin_31_55_install_trend
,max(case when cate_id='fin_31' and label='0' then cosin_similarity else -1 end ) as fin_31_install_trend
,max(case when cate_id='fin_34' and label='55岁以上' then cosin_similarity else -1 end ) as fin_34_55_install_trend
,max(case when cate_id='fin_43' and label='55岁以上' then cosin_similarity else -1 end ) as fin_43_55_install_trend
,max(case when cate_id='fin_50' and label='18以下' then cosin_similarity else -1 end ) as fin_50_18_install_trend
,max(case when cate_id='fin_50' and label='0' then cosin_similarity else -1 end ) as fin_50_install_trend
,max(case when cate_id='tgi1_18_4' and label='18-24岁' then cosin_similarity else -1 end ) as tgi1_18_4_18_24_install_trend
,max(case when cate_id='tgi1_25_34_1' and label='18以下' then cosin_similarity else -1 end ) as tgi1_25_34_1_18_install_trend
,max(case when cate_id='tgi1_25_34_2' and label='0' then cosin_similarity else -1 end ) as tgi1_25_34_2_install_trend
,max(case when cate_id='tgi2_18_4' and label='18以下' then cosin_similarity else -1 end ) as tgi2_18_4_18_install_trend
,max(case when cate_id='tgi2_35_44_2' and label='55岁以上' then cosin_similarity else -1 end ) as tgi2_35_44_2_55_install_trend
,max(case when cate_id='tgi2_35_44_6' and label='55岁以上' then cosin_similarity else -1 end ) as tgi2_35_44_6_55_install_trend
,max(case when cate_id='tgi2_45_54_2' and label='18-24岁' then cosin_similarity else -1 end ) as tgi2_45_54_2_18_24_install_trend
,max(case when cate_id='tgi2_55_1' and label='18以下' then cosin_similarity else -1 end ) as tgi2_55_1_18_install_trend
from $age_new_uninstall_avg_embedding_cosin_temp
where day='${insertday}'
group by device
"
