#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi

insert_day=$1

# 获取当前日期所在月的第一天
start_month=$(date -d "${insert_day}  " +%Y%m01)
# 获取当前日期所在月的最后一天
end_month=$insert_day

source /home/dba/mobdi_center/conf/hive-env.sh

# input
#apppkg_app2vec_par_wi=rp_mobdi_app.apppkg_app2vec_par_wi
#label_device_pkg_install_uninstall_year_info_mf=dm_mobdi_report.label_device_pkg_install_uninstall_year_info_mf
#income_cate_avg_embedding=tp_mobdi_model.income_cate_avg_embedding
#income_category_mapping=tp_mobdi_model.income_category_mapping

tmpdb=dm_mobdi_tmp
income_new_pre_uninstall_avg_embedding="${tmpdb}.income_new_pre_uninstall_avg_embedding"
income_new_uninstall_avg_embedding="${tmpdb}.income_new_uninstall_avg_embedding"
income_new_uninstall_avg_embedding_cosin_tmp="${tmpdb}.income_new_uninstall_avg_embedding_cosin_tmp"
income_new_embedding_cosin_bycate="${tmpdb}.income_new_embedding_cosin_bycate"


HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance2;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=12288;
set mapreduce.map.java.opts='-Xmx10240m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx10240m';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx6144m' -XX:+UseG1GC;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;
set hive.auto.convert.join=true;
set hive.mapjoin.smalltable.filesize=5000000000;
use dm_mobdi_tmp;

insert overwrite table $income_new_pre_uninstall_avg_embedding partition (day='$end_month')
select
    device,
    t1.pkg as pkg,
    update_day
from
(
    select device,pkg,update_day,day
    from $label_device_pkg_install_uninstall_year_info_mf
    where day='$end_month' and update_day>='$start_month' and update_day<='$end_month'
    and refine_final_flag in (0,1)
    group by device,pkg,update_day,day
)t1
inner join
(
    select pkg, cate_id
    from $income_category_mapping
    group by pkg, cate_id
)t2
on t1.pkg=t2.pkg;
"


skew_pkgs_num=500
skew_pkgs_sql="
select collect_list(pkg) pkgs from (
select pkg,count(*) cnt from $income_new_pre_uninstall_avg_embedding
where day = '$end_month' and pkg is not null and trim(pkg) <>'' group by pkg
order by cnt DESC limit $skew_pkgs_num
) t
"
skew_pkgs_arr=$(hive -e "$skew_pkgs_sql")
skew_pkgs=${skew_pkgs_arr//[\[\]]}


HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance2;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=12288;
set mapreduce.map.java.opts='-Xmx10240m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx10240m';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx6144m' -XX:+UseG1GC;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;
set hive.auto.convert.join=true;
set hive.mapjoin.smalltable.filesize=5000000000;
use dm_mobdi_tmp;

create temporary table apppkg_app2vec_par_wi_big_age_part10_$end_month as
select * from $apppkg_app2vec_par_wi where day='20210418' and apppkg in ($skew_pkgs);

create temporary table raw_table_small_$end_month as
select device,pkg,update_day
from $income_new_pre_uninstall_avg_embedding
where day='${end_month}' and pkg not in ($skew_pkgs)
group by device,pkg,update_day;


create temporary table raw_table_skew_$end_month as
select device,pkg,update_day
from $income_new_pre_uninstall_avg_embedding
where day='${end_month}' and pkg in ($skew_pkgs)
group by device,pkg,update_day;


create temporary table cate_embedding_$end_month as
select
    cate_id,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24,d25,d26,d27,
    d28,d29,d30,d31,d32,d33,d34,d35,d36,d37,d38,d39,d40,d41,d42,d43,d44,d45,d46,d47,d48,d49,d50,d51,d52,d53,d54,
    d55,d56,d57,d58,d59,d60,d61,d62,d63,d64,d65,d66,d67,d68,d69,d70,d71,d72,d73,d74,d75,d76,d77,d78,d79,d80,d81,
    d82,d83,d84,d85,d86,d87,d88,d89,d90,d91,d92,d93,d94,d95,d96,d97,d98,d99,d100,'0' as label,
    sqrt(d1*d1+d2*d2+d3*d3+d4*d4+d5*d5+d6*d6+d7*d7+d8*d8+d9*d9+d10*d10+d11*d11+d12*d12+d13*d13+d14*d14+d15*d15+d16*d16+d17*d17+d18*d18+d19*d19+d20*d20+d21*d21+d22*d22+d23*d23+d24*d24+d25*d25+d26*d26+d27*d27+d28*d28+d29*d29+d30*d30+d31*d31+d32*d32+d33*d33+d34*d34+d35*d35+d36*d36+d37*d37+d38*d38+d39*d39+d40*d40+d41*d41+d42*d42+d43*d43+d44*d44+d45*d45+d46*d46+d47*d47+d48*d48+d49*d49+d50*d50+d51*d51+d52*d52+d53*d53+d54*d54+d55*d55+d56*d56+d57*d57+d58*d58+d59*d59+d60*d60+d61*d61+d62*d62+d63*d63+d64*d64+d65*d65+d66*d66+d67*d67+d68*d68+d69*d69+d70*d70+d71*d71+d72*d72+d73*d73+d74*d74+d75*d75+d76*d76+d77*d77+d78*d78+d79*d79+d80*d80+d81*d81+d82*d82+d83*d83+d84*d84+d85*d85+d86*d86+d87*d87+d88*d88+d89*d89+d90*d90+d91*d91+d92*d92+d93*d93+d94*d94+d95*d95+d96*d96+d97*d97+d98*d98+d99*d99+d100*d100) as cate_vector_length
from $income_cate_avg_embedding
where cate_id in (
'a10_1','a10_3','a11','a12','a13_2','a13_4','a14_3','a15','a16' ,'a17' ,'a2_5','a2_6','a2_8','a2',
'a20_12','a20_5','a20_6','a20_7','a20','a4_1','a4_3','a5_3','a5','a6_3','a7_2','a7_3','a8_3','a8','a9_2'
,'b1','b3','b4','b6_4','b6','b7_1','b7_2','b8_1','b8','fin_10','fin_13','fin_14','fin_17','fin_18'
,'fin_2','fin_21','fin_22','fin_26','fin_28','fin_31','fin_33','fin_41','fin_42','fin_48','fin_50'
,'fin_51','fin_52','fin_6','tgi1_1_2','tgi1_1_3','tgi1_2_0','tgi1_2_3','tgi1_3_0','tgi1_3_1','tgi1_3_2','tgi1_4_2');



insert overwrite table $income_new_uninstall_avg_embedding partition (day='$end_month')
select
  device,avg(d1) as d1,avg(d2) as d2,avg(d3) as d3,avg(d4) as d4,avg(d5) as d5,avg(d6) as d6,avg(d7) as d7,avg(d8) as d8,
  avg(d9) as d9,avg(d10) as d10,avg(d11) as d11,avg(d12) as d12,avg(d13) as d13,avg(d14) as d14,avg(d15) as d15,avg(d16) as d16,
  avg(d17) as d17,avg(d18) as d18,avg(d19) as d19,avg(d20) as d20,avg(d21) as d21,avg(d22) as d22,avg(d23) as d23,avg(d24) as d24,
  avg(d25) as d25,avg(d26) as d26,avg(d27) as d27,avg(d28) as d28,avg(d29) as d29,avg(d30) as d30,avg(d31) as d31,avg(d32) as d32,
  avg(d33) as d33,avg(d34) as d34,avg(d35) as d35,avg(d36) as d36,avg(d37) as d37,avg(d38) as d38,avg(d39) as d39,avg(d40) as d40,
  avg(d41) as d41,avg(d42) as d42,avg(d43) as d43,avg(d44) as d44,avg(d45) as d45,avg(d46) as d46,avg(d47) as d47,avg(d48) as d48,
  avg(d49) as d49,avg(d50) as d50,avg(d51) as d51,avg(d52) as d52,avg(d53) as d53,avg(d54) as d54,avg(d55) as d55,avg(d56) as d56,
  avg(d57) as d57,avg(d58) as d58,avg(d59) as d59,avg(d60) as d60,avg(d61) as d61,avg(d62) as d62,avg(d63) as d63,avg(d64) as d64,
  avg(d65) as d65,avg(d66) as d66,avg(d67) as d67,avg(d68) as d68,avg(d69) as d69,avg(d70) as d70,avg(d71) as d71,avg(d72) as d72,
  avg(d73) as d73,avg(d74) as d74,avg(d75) as d75,avg(d76) as d76,avg(d77) as d77,avg(d78) as d78,avg(d79) as d79,avg(d80) as d80,
  avg(d81) as d81,avg(d82) as d82,avg(d83) as d83,avg(d84) as d84,avg(d85) as d85,avg(d86) as d86,avg(d87) as d87,avg(d88) as d88,
  avg(d89) as d89,avg(d90) as d90,avg(d91) as d91,avg(d92) as d92,avg(d93) as d93,avg(d94) as d94,avg(d95) as d95,avg(d96) as d96,
  avg(d97) as d97,avg(d98) as d98,avg(d99) as d99,avg(d100) as d100,update_day
from(
  select * from raw_table_small_$end_month a
  inner join( select * from $apppkg_app2vec_par_wi where day='20210418' )b
  on trim(a.pkg)=trim(b.apppkg)

  union all

  select /*+ MAPJOIN(b) */ * from raw_table_skew_$end_month a
  inner join apppkg_app2vec_par_wi_big_age_part10_$end_month b
  on trim(a.pkg)=trim(b.apppkg)
) tt
group by device,update_day;



insert overwrite table $income_new_uninstall_avg_embedding_cosin_tmp partition (day='$end_month')
select device,a.cate_id,(a.d1* b.d1+a.d2* b.d2+a.d3* b.d3+a.d4* b.d4+a.d5* b.d5+a.d6* b.d6+a.d7* b.d7+a.d8* b.d8+a.d9* b.d9+a.d10* b.d10+a.d11* b.d11+a.d12* b.d12+a.d13* b.d13+a.d14* b.d14+a.d15* b.d15+a.d16* b.d16+a.d17* b.d17+a.d18* b.d18+a.d19* b.d19+a.d20* b.d20+a.d21* b.d21+a.d22* b.d22+a.d23* b.d23+a.d24* b.d24+a.d25* b.d25+a.d26* b.d26+a.d27* b.d27+a.d28* b.d28+a.d29* b.d29+a.d30* b.d30+a.d31* b.d31+a.d32* b.d32+a.d33* b.d33+a.d34* b.d34+a.d35* b.d35+a.d36* b.d36+a.d37* b.d37+a.d38* b.d38+a.d39* b.d39+a.d40* b.d40+a.d41* b.d41+a.d42* b.d42+a.d43* b.d43+a.d44* b.d44+a.d45* b.d45+a.d46* b.d46+a.d47* b.d47+a.d48* b.d48+a.d49* b.d49+a.d50* b.d50+a.d51* b.d51+a.d52* b.d52+a.d53* b.d53+a.d54* b.d54+a.d55* b.d55+a.d56* b.d56+a.d57* b.d57+a.d58* b.d58+a.d59* b.d59+a.d60* b.d60+a.d61* b.d61+a.d62* b.d62+a.d63* b.d63+a.d64* b.d64+a.d65* b.d65+a.d66* b.d66+a.d67* b.d67+a.d68* b.d68+a.d69* b.d69+a.d70* b.d70+a.d71* b.d71+a.d72* b.d72+a.d73* b.d73+a.d74* b.d74+a.d75* b.d75+a.d76* b.d76+a.d77* b.d77+a.d78* b.d78+a.d79* b.d79+a.d80* b.d80+a.d81* b.d81+a.d82* b.d82+a.d83* b.d83+a.d84* b.d84+a.d85* b.d85+a.d86* b.d86+a.d87* b.d87+a.d88* b.d88+a.d89* b.d89+a.d90* b.d90+a.d91* b.d91+a.d92* b.d92+a.d93* b.d93+a.d94* b.d94+a.d95* b.d95+a.d96* b.d96+a.d97* b.d97+a.d98* b.d98+a.d99* b.d99+a.d100* b.d100
)/(cate_vector_length*device_vector_length) as cosin_similarity,update_day
from
(
    select * from  cate_embedding_$end_month
)a
cross join
(
    select *, sqrt(d1*d1+d2*d2+d3*d3+d4*d4+d5*d5+d6*d6+d7*d7+d8*d8+d9*d9+d10*d10+d11*d11+d12*d12+d13*d13+d14*d14+d15*d15+d16*d16+d17*d17+d18*d18+d19*d19+d20*d20+d21*d21+d22*d22+d23*d23+d24*d24+d25*d25+d26*d26+d27*d27+d28*d28+d29*d29+d30*d30+d31*d31+d32*d32+d33*d33+d34*d34+d35*d35+d36*d36+d37*d37+d38*d38+d39*d39+d40*d40+d41*d41+d42*d42+d43*d43+d44*d44+d45*d45+d46*d46+d47*d47+d48*d48+d49*d49+d50*d50+d51*d51+d52*d52+d53*d53+d54*d54+d55*d55+d56*d56+d57*d57+d58*d58+d59*d59+d60*d60+d61*d61+d62*d62+d63*d63+d64*d64+d65*d65+d66*d66+d67*d67+d68*d68+d69*d69+d70*d70+d71*d71+d72*d72+d73*d73+d74*d74+d75*d75+d76*d76+d77*d77+d78*d78+d79*d79+d80*d80+d81*d81+d82*d82+d83*d83+d84*d84+d85*d85+d86*d86+d87*d87+d88*d88+d89*d89+d90*d90+d91*d91+d92*d92+d93*d93+d94*d94+d95*d95+d96*d96+d97*d97+d98*d98+d99*d99+d100*d100) as device_vector_length
    from $income_new_uninstall_avg_embedding
    where day='$end_month'
)b;



insert overwrite table $income_new_embedding_cosin_bycate partition (day='$end_month')
    select device
    ,max(case when cate_id='a10_1' then cosin_similarity else -1 end) as a10_1_income1_install_trend
    ,max(case when cate_id='a10_3' then cosin_similarity else -1 end) as a10_3_income1_install_trend
    ,max(case when cate_id='a11' then cosin_similarity else -1 end) as a11_income1_install_trend
    ,max(case when cate_id='a12' then cosin_similarity else -1 end) as a12_income1_install_trend
    ,max(case when cate_id='a13_2' then cosin_similarity else -1 end) as a13_2_income1_install_trend
    ,max(case when cate_id='a13_4' then cosin_similarity else -1 end) as a13_4_income1_install_trend
    ,max(case when cate_id='a14_3' then cosin_similarity else -1 end) as a14_3_income1_install_trend
    ,max(case when cate_id='a15' then cosin_similarity else -1 end) as a15_income1_install_trend
    ,max(case when cate_id='a16' then cosin_similarity else -1 end) as a16_income1_install_trend
    ,max(case when cate_id='a17' then cosin_similarity else -1 end) as a17_income1_install_trend
    ,max(case when cate_id='a2_5' then cosin_similarity else -1 end) as a2_5_income1_install_trend
    ,max(case when cate_id='a2_6' then cosin_similarity else -1 end) as a2_6_income1_install_trend
    ,max(case when cate_id='a2_8' then cosin_similarity else -1 end) as a2_8_income1_install_trend
    ,max(case when cate_id='a2' then cosin_similarity else -1 end) as a2_income1_install_trend
    ,max(case when cate_id='a20_12' then cosin_similarity else -1 end) as a20_12_income1_install_trend
    ,max(case when cate_id='a20_5' then cosin_similarity else -1 end) as a20_5_income1_install_trend
    ,max(case when cate_id='a20_6' then cosin_similarity else -1 end) as a20_6_income1_install_trend
    ,max(case when cate_id='a20_7' then cosin_similarity else -1 end) as a20_7_income1_install_trend
    ,max(case when cate_id='a20' then cosin_similarity else -1 end) as a20_income1_install_trend
    ,max(case when cate_id='a4_1' then cosin_similarity else -1 end) as a4_1_income1_install_trend
    ,max(case when cate_id='a4_3' then cosin_similarity else -1 end) as a4_3_income1_install_trend
    ,max(case when cate_id='a5_3' then cosin_similarity else -1 end) as a5_3_income1_install_trend
    ,max(case when cate_id='a5' then cosin_similarity else -1 end) as a5_income1_install_trend
    ,max(case when cate_id='a6_3' then cosin_similarity else -1 end) as a6_3_income1_install_trend
    ,max(case when cate_id='a7_2' then cosin_similarity else -1 end) as a7_2_income1_install_trend
    ,max(case when cate_id='a7_3' then cosin_similarity else -1 end) as a7_3_income1_install_trend
    ,max(case when cate_id='a8_3' then cosin_similarity else -1 end) as a8_3_income1_install_trend
    ,max(case when cate_id='a8' then cosin_similarity else -1 end) as a8_income1_install_trend
    ,max(case when cate_id='a9_2' then cosin_similarity else -1 end) as a9_2_income1_install_trend
    ,max(case when cate_id='b1' then cosin_similarity else -1 end) as b1_income1_install_trend
    ,max(case when cate_id='b3' then cosin_similarity else -1 end) as b3_income1_install_trend
    ,max(case when cate_id='b4' then cosin_similarity else -1 end) as b4_income1_install_trend
    ,max(case when cate_id='b6_4' then cosin_similarity else -1 end) as b6_4_income1_install_trend
    ,max(case when cate_id='b6' then cosin_similarity else -1 end) as b6_income1_install_trend
    ,max(case when cate_id='b7_1' then cosin_similarity else -1 end) as b7_1_income1_install_trend
    ,max(case when cate_id='b7_2' then cosin_similarity else -1 end) as b7_2_income1_install_trend
    ,max(case when cate_id='b8_1' then cosin_similarity else -1 end) as b8_1_income1_install_trend
    ,max(case when cate_id='b8' then cosin_similarity else -1 end) as b8_income1_install_trend
    ,max(case when cate_id='fin_10' then cosin_similarity else -1 end) as fin_10_income1_install_trend
    ,max(case when cate_id='fin_13' then cosin_similarity else -1 end) as fin_13_income1_install_trend
    ,max(case when cate_id='fin_14' then cosin_similarity else -1 end) as fin_14_income1_install_trend
    ,max(case when cate_id='fin_17' then cosin_similarity else -1 end) as fin_17_income1_install_trend
    ,max(case when cate_id='fin_18' then cosin_similarity else -1 end) as fin_18_income1_install_trend
    ,max(case when cate_id='fin_2' then cosin_similarity else -1 end) as fin_2_income1_install_trend
    ,max(case when cate_id='fin_21' then cosin_similarity else -1 end) as fin_21_income1_install_trend
    ,max(case when cate_id='fin_22' then cosin_similarity else -1 end) as fin_22_income1_install_trend
    ,max(case when cate_id='fin_26' then cosin_similarity else -1 end) as fin_26_income1_install_trend
    ,max(case when cate_id='fin_28' then cosin_similarity else -1 end) as fin_28_income1_install_trend
    ,max(case when cate_id='fin_31' then cosin_similarity else -1 end) as fin_31_income1_install_trend
    ,max(case when cate_id='fin_33' then cosin_similarity else -1 end) as fin_33_income1_install_trend
    ,max(case when cate_id='fin_41' then cosin_similarity else -1 end) as fin_41_income1_install_trend
    ,max(case when cate_id='fin_42' then cosin_similarity else -1 end) as fin_42_income1_install_trend
    ,max(case when cate_id='fin_48' then cosin_similarity else -1 end) as fin_48_income1_install_trend
    ,max(case when cate_id='fin_50' then cosin_similarity else -1 end) as fin_50_income1_install_trend
    ,max(case when cate_id='fin_51' then cosin_similarity else -1 end) as fin_51_income1_install_trend
    ,max(case when cate_id='fin_52' then cosin_similarity else -1 end) as fin_52_income1_install_trend
    ,max(case when cate_id='fin_6' then cosin_similarity else -1 end) as fin_6_income1_install_trend
    ,max(case when cate_id='tgi1_1_2' then cosin_similarity else -1 end) as tgi1_1_2_income1_install_trend
    ,max(case when cate_id='tgi1_1_3' then cosin_similarity else -1 end) as tgi1_1_3_income1_install_trend
    ,max(case when cate_id='tgi1_2_0' then cosin_similarity else -1 end) as tgi1_2_0_income1_install_trend
    ,max(case when cate_id='tgi1_2_3' then cosin_similarity else -1 end) as tgi1_2_3_income1_install_trend
    ,max(case when cate_id='tgi1_3_0' then cosin_similarity else -1 end) as tgi1_3_0_income1_install_trend
    ,max(case when cate_id='tgi1_3_1' then cosin_similarity else -1 end) as tgi1_3_1_income1_install_trend
    ,max(case when cate_id='tgi1_3_2' then cosin_similarity else -1 end) as tgi1_3_2_income1_install_trend
    ,max(case when cate_id='tgi1_4_2' then cosin_similarity else -1 end) as tgi1_4_2_income1_install_trend
from $income_new_uninstall_avg_embedding_cosin_tmp
where day='$end_month'
group by device;
"