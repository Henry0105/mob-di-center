#!/bin/bash
set -e -x
: '
@owner:liuyanqiang
@describe:income_1001特征较多，本脚本做一次初步计算汇总
@projectName:mobdi
@BusinessName:profile_model
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi
day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh
md_db=$dw_mobdi_md
phone_contacts_index=$md_db.phone_contacts_index
phone_contacts_word2vec_index=$md_db.phone_contacts_word2vec_index

tmpdb=$dm_mobdi_tmp
calculate_model_device=$tmpdb.calculate_model_device
income_1001_phone_contacts_index=$tmpdb.income_1001_phone_contacts_index
income_1001_phone_contacts_word2vec_index=$tmpdb.income_1001_phone_contacts_word2vec_index
income_1001_phone_contacts_index=$tmpdb.income_1001_phone_contacts_index

phone_contacts_index_db=${phone_contacts_index%.*}
phone_contacts_index_tb=${phone_contacts_index#*.}

phone_contacts_word2vec_index_db=${phone_contacts_word2vec_index%.*}
phone_contacts_word2vec_index_tb=${phone_contacts_word2vec_index#*.}

phoneContactsIndexLastPar=(`hive -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('$phone_contacts_index_db', '$phone_contacts_index_tb', 'day');
"`)
phoneContactsWord2vecIndexLastPar=(`hive -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('$phone_contacts_word2vec_index_db', '$phone_contacts_word2vec_index_tb', 'day');
"`)

#hive -v -e "
#SET hive.merge.mapfiles=true;
#SET hive.merge.mapredfiles=true;
#set mapred.max.split.size=250000000;
#set mapred.min.split.size.per.node=128000000;
#set mapred.min.split.size.per.rack=128000000;
#set hive.merge.smallfiles.avgsize=250000000;
#set hive.merge.size.per.task = 250000000;
#insert overwrite table dw_mobdi_md.calculate_model_device partition(day='$day')
#select device
#from dim_mobdi_mapping.device_applist_new
#where day = '$day'
#group by device;
#"

:<<!
--先做一层dws聚合表，跑一个月数据
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table dm_mobdi_master.dws_location_bssid_info_di partition(day)
select device,bssid,plat,
       from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd HH:mm:ss') as real_date, day
from dw_mobdi_etl.log_wifi_info
where day >= '20191002'
and trim(bssid) not in ('00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
and trim(bssid) is not null
and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
group by device,bssid,plat,
         from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd HH:mm:ss'), day;
!

#每日跑一天的dws聚合表数据
#hive -v -e "
#SET hive.merge.mapfiles=true;
#SET hive.merge.mapredfiles=true;
#set mapred.max.split.size=250000000;
#set mapred.min.split.size.per.node=128000000;
#set mapred.min.split.size.per.rack=128000000;
#set hive.merge.smallfiles.avgsize=250000000;
#set hive.merge.size.per.task = 250000000;
#insert overwrite table dm_mobdi_master.dws_location_bssid_info_di partition(day='$day')
#select device,bssid,plat,
#       from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd HH:mm:ss') as real_date
#from dw_mobdi_etl.log_wifi_info
#where day = '$day'
#and trim(bssid) not in ('00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
#and trim(bssid) is not null
#and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
#group by device,bssid,plat,
#         from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd HH:mm:ss');
#"

p1month=`date -d "$day -1 months" +%Y%m%d`
#聚合一个月活跃的bssid
#与需要计算的设备数据inner join
#hive -v -e "
#SET hive.merge.mapfiles=true;
#SET hive.merge.mapredfiles=true;
#set mapred.max.split.size=250000000;
#set mapred.min.split.size.per.node=128000000;
#set mapred.min.split.size.per.rack=128000000;
#set hive.merge.smallfiles.avgsize=250000000;
#set hive.merge.size.per.task = 250000000;
#insert overwrite table dw_mobdi_md.income_1001_bssid_index_calculate_base_info partition(day='$day')
#select a.device,b.bssid,b.day as active_day
#from dw_mobdi_md.calculate_model_device a
#inner join
#(
#  select device,bssid,day
#  from dm_mobdi_master.dws_location_bssid_info_di
#  where day >= '$p1month'
#  and day <= '$day'
#  and plat = 1
#  and real_date >= '$p1month'
#  and real_date <= '${day} 23:59:59'
#  group by device,bssid,day
#) b on a.device=b.device
#where a.day='$day';
#"

#计算学校bssid特征
#通过还未上线的test.hejy_temp_poi_school_15学校bssid对应表，先计算device在各学校bssid的连接日期
#然后计算device在各学校bssid的连接天数
#对每个device保留过去一个月连接最频繁的学校对应的连接天数
#根据连接天数计算index
#hive -v -e "
#set hive.auto.convert.join=false;
#SET hive.merge.mapfiles=true;
#SET hive.merge.mapredfiles=true;
#set mapred.max.split.size=250000000;
#set mapred.min.split.size.per.node=128000000;
#set mapred.min.split.size.per.rack=128000000;
#set hive.merge.smallfiles.avgsize=250000000;
#set hive.merge.size.per.task = 250000000;
#insert overwrite table dw_mobdi_md.income_1001_university_bssid_index partition(day='$day')
#select device,
#       case
#         when university_connect_day_cnt > 0 and university_connect_day_cnt <= 10 then 1
#         when university_connect_day_cnt > 10 and university_connect_day_cnt <= 20 then 2
#         when university_connect_day_cnt > 20 then 3
#         else 4
#       end as index
#from
#(
#  select device,university_connect_day_cnt
#  from
#  (
#    select device,university_name,university_connect_day_cnt,
#           row_number() over(partition by device order by university_connect_day_cnt desc) as rank
#    from
#    (
#      select device,university_name,count(1) as university_connect_day_cnt
#      from
#      (
#        select b.device,a.university_name,b.active_day
#        from dw_mobdi_md.poi_school a
#        inner join
#        dw_mobdi_md.income_1001_bssid_index_calculate_base_info b on b.day='$day' and a.bssid=regexp_replace(b.bssid,':','')
#        group by b.device,a.university_name,b.active_day
#      ) t
#      group by device,university_name
#    ) a
#  ) b
#  where rank=1
#) t;
#"

#shoppingMallBssidMappingLastPar=(`hive -e "
#add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
#create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
#SELECT GET_LAST_PARTITION('dim_mobdi_mapping', 'dim_shopping_mall_ssid_bssid_match_info_mf', 'day');
#"`)
#计算商场bssid特征
#通过dim_mobdi_mapping.dim_shopping_mall_ssid_bssid_match_info_mf表，先计算device连接商场的日期
#然后计算device在商场的连接天数
#根据连接天数计算index
#hive -v -e "
#SET hive.merge.mapfiles=true;
#SET hive.merge.mapredfiles=true;
#set mapred.max.split.size=250000000;
#set mapred.min.split.size.per.node=128000000;
#set mapred.min.split.size.per.rack=128000000;
#set hive.merge.smallfiles.avgsize=250000000;
#set hive.merge.size.per.task = 250000000;
#insert overwrite table dw_mobdi_md.income_1001_shopping_mall_bssid_index partition(day='$day')
#select t1.device,
#       case
#         when day_cnt > 0 and day_cnt <= 5 then 5
#         when day_cnt > 5 and day_cnt <= 10 then 6
#         when day_cnt > 10 and day_cnt <= 20 then 7
#         when day_cnt > 20 then 8
#         else 9
#       end as index
#from dw_mobdi_md.calculate_model_device t1
#left join
#(
#  select device, count(1) as day_cnt
#  from
#  (
#    select b.device,b.active_day
#    from
#    (
#      select bssid
#      from dim_mobdi_mapping.dim_shopping_mall_ssid_bssid_match_info_mf
#      lateral view explode(bssid_array) n as bssid
#      where day='$shoppingMallBssidMappingLastPar'
#      group by bssid
#    ) a
#    inner join
#    dw_mobdi_md.income_1001_bssid_index_calculate_base_info b on b.day='$day' and a.bssid=b.bssid
#    group by b.device,b.active_day
#  ) a
#  group by device
#) t2 on t1.device=t2.device
#where t1.day='$day';
#"

#计算交通bssid特征
#通过还未上线的test.qingy_traffic_bssid_dpp交通bssid表，先计算device连接交通的类型和连接日期
#然后计算device是否连接过长途汽车站、地铁站、机场、火车站
#根据连接结果计算index
#hive -v -e "
#SET hive.merge.mapfiles=true;
#SET hive.merge.mapredfiles=true;
#set mapred.max.split.size=250000000;
#set mapred.min.split.size.per.node=128000000;
#set mapred.min.split.size.per.rack=128000000;
#set hive.merge.smallfiles.avgsize=250000000;
#set hive.merge.size.per.task = 250000000;
#insert overwrite table dw_mobdi_md.income_1001_traffic_bssid_index partition(day='$day')
#select device,
#       case
#         when traffic_bus=0 then 10
#         when traffic_bus>0 then 11
#       end as traffic_bus_index,
#       case
#         when traffic_subway=0 then 12
#         when traffic_subway>0 then 13
#       end as traffic_subway_index,
#       case
#         when traffic_airport=0 then 14
#         when traffic_airport>0 then 15
#       end as traffic_airport_index,
#       case
#         when traffic_train=0 then 16
#         when traffic_train>0 then 17
#       end as traffic_train_index
#from
#(
#  select device,
#         sum(case when type_name='长途汽车站' then 1 else 0 end) as traffic_bus,
#         sum(case when type_name='地铁站' then 1 else 0 end) as traffic_subway,
#         sum(case when type_name='机场' then 1 else 0 end) as traffic_airport,
#         sum(case when type_name='火车站' then 1 else 0 end) as traffic_train
#  from
#  (
#    select a.device,b.active_day,b.type_name
#    from dw_mobdi_md.calculate_model_device a
#    left join
#    (
#      select b.device,b.active_day,a.type_name
#      from dw_mobdi_md.traffic_bssid a
#      inner join
#      dw_mobdi_md.income_1001_bssid_index_calculate_base_info b on b.day='$day' and a.bssid=b.bssid
#    ) b on a.device=b.device
#    where a.day='$day'
#    group by a.device,b.active_day,b.type_name
#  ) a
#  group by device
#) t1;
#"

#计算酒店bssid特征
#通过还未上线的test.qingy_hotel_bssid_ddp酒店bssid表，先计算device在每个连接日期中最高的价格等级、最高的星级、最高的评分等级
#计算设备是否去过各个价格等级、星级、评分等级的酒店
#根据上一步结果计算index
#hive -v -e "
#SET hive.merge.mapfiles=true;
#SET hive.merge.mapredfiles=true;
#set mapred.max.split.size=250000000;
#set mapred.min.split.size.per.node=128000000;
#set mapred.min.split.size.per.rack=128000000;
#set hive.merge.smallfiles.avgsize=250000000;
#set hive.merge.size.per.task = 250000000;
#insert overwrite table dw_mobdi_md.income_1001_hotel_bssid_index partition(day='$day')
#select device,
#       case
#         when price_level1 =0 then 18
#         when price_level1 >0 then 19
#       end as price_level1_index,
#       case
#         when price_level2 =0 then 20
#         when price_level2 >0 then 21
#       end as price_level2_index,
#       case
#         when price_level3 =0 then 22
#         when price_level3 >0 then 23
#       end as price_level3_index,
#       case
#         when price_level4 =0 then 24
#         when price_level4 >0 then 25
#       end as price_level4_index,
#       case
#         when price_level5 =0 then 26
#         when price_level5 >0 then 27
#       end as price_level5_index,
#       case
#         when price_level6 =0 then 28
#         when price_level6 >0 then 29
#       end as price_level6_index,
#       case
#         when rank_star1 =0 then 30
#         when rank_star1 >0 then 31
#       end as rank_star1_index,
#       case
#         when rank_star2 =0 then 32
#         when rank_star2 >0 then 33
#       end as rank_star2_index,
#       case
#         when rank_star3 =0 then 34
#         when rank_star3 >0 then 35
#       end as rank_star3_index,
#       case
#         when rank_star4 =0 then 36
#         when rank_star4 >0 then 37
#       end as rank_star4_index,
#       case
#         when rank_star5 =0 then 38
#         when rank_star5 >0 then 39
#       end as rank_star5_index,
#       case
#         when score_type1 =0 then 40
#         when score_type1 >0 then 41
#       end as score_type1_index,
#       case
#         when score_type2 =0 then 42
#         when score_type2 >0 then 43
#       end as score_type2_index,
#       case
#         when score_type3 =0 then 44
#         when score_type3 >0 then 45
#       end as score_type3_index
#from
#(
#  select device,
#         sum(case when price_level=1 then 1 else 0 end) as price_level1,
#         sum(case when price_level=2 then 1 else 0 end) as price_level2,
#         sum(case when price_level=3 then 1 else 0 end) as price_level3,
#         sum(case when price_level=4 then 1 else 0 end) as price_level4,
#         sum(case when price_level=5 then 1 else 0 end) as price_level5,
#         sum(case when price_level=6 then 1 else 0 end) as price_level6,
#         sum(case when rank_star=1 then 1 else 0 end) as rank_star1,
#         sum(case when rank_star=2 then 1 else 0 end) as rank_star2,
#         sum(case when rank_star=3 then 1 else 0 end) as rank_star3,
#         sum(case when rank_star=4 then 1 else 0 end) as rank_star4,
#         sum(case when rank_star=5 then 1 else 0 end) as rank_star5,
#         sum(case when score_type=1 then 1 else 0 end) as score_type1,
#         sum(case when score_type=2 then 1 else 0 end) as score_type2,
#         sum(case when score_type=3 then 1 else 0 end) as score_type3
#  from
#  (
#    select device,active_day,
#           max(price_level) as price_level,
#           max(rank_star) as rank_star,
#           max(score_type) as score_type
#    from
#    (
#      select a.device,b.active_day,price_level,
#             case
#               when rank_star='无星级' then 1
#               when rank_star='二星/经济' then 2
#               when rank_star='三星/舒适' then 3
#               when rank_star='四星/高档' then 4
#               when rank_star='五星/豪华' then 5
#               else 0
#             end as rank_star,
#             case
#               when score_type='评分较低' then 1
#               when score_type='评价中等' then 2
#               when score_type='评分较高' then 3
#             else 0 end as score_type
#      from dw_mobdi_md.calculate_model_device a
#      left join
#      (
#        select b.device,b.active_day,a.price_level,a.rank_star,a.score_type
#        from dw_mobdi_md.hotel_bssid a
#        inner join
#        dw_mobdi_md.income_1001_bssid_index_calculate_base_info b on b.day='$day' and a.bssid=b.bssid
#      ) b on a.device=b.device
#      where a.day='$day'
#    ) a
#    group by device,active_day
#  ) t1
#  group by device
#) t1;
#"

#通讯录特征计算
#先通过android_id_mapping_full表找出设备的最新手机号
#然后和通讯录特征结果dw_mobdi_md.phone_contacts_index表join，得到设备的微商水军标志位、通讯录号码得分分段、是否有公司名、公司手机数量分段、职级排行分段、分词index
#最后处理50个词向量四分位数特征dw_mobdi_md.income_1001_phone_contacts_index
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function explode_tags as 'com.youzu.mob.java.udtf.ExplodeTags';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $income_1001_phone_contacts_index partition(day='$day')
select c.device,c.phone,
       case
         when micro_business_flag = 0 then 19479
         when micro_business_flag = 1 then 19480
       end as micro_business_flag_index,
       case
         when score_level = 1 then 19481
         when score_level = 2 then 19482
         when score_level = 3 then 19483
         when score_level = 4 then 19484
         when score_level = 5 then 19485
         when score_level = 6 then 19486
       end as score_level_index,
       case
         when if_company = 0 then 19487
         when if_company = 1 then 19488
       end as if_company_index,
       case
         when company_size = 1 then 19489
         when company_size = 2 then 19490
         when company_size = 3 then 19491
         when company_size = 4 then 19492
       end as company_size_index,
       case
         when company_rk = 1 then 19493
         when company_rk = 2 then 19494
         when company_rk = 3 then 19495
       end as company_rk_index,
       word_index
from
(
  select a.device,b.phone
  from $calculate_model_device a
  inner join
  (
    select device,phone
    from
    (
      select device,phone,
             row_number() over(partition by device order by pn_tm desc) num
      from
      (
        select device,concat(phone,'=',phone_ltm) phone_list
        from $dim_id_mapping_android_df_view
        where length(phone)>0
        and length(phone)<10000
      ) phone_info
      lateral view explode_tags(phone_list) phone_tmp as phone,pn_tm
    ) un
    where num=1
  ) b on a.device=b.device
  where a.day='$day'
) c
inner join $phone_contacts_index d on d.day='$phoneContactsIndexLastPar' and c.phone=d.phone;

insert overwrite table $income_1001_phone_contacts_word2vec_index partition(day='$day')
select device,index
from
(
  select a.device,
         vec1_quantile,vec2_quantile,vec3_quantile,vec4_quantile,vec5_quantile,vec6_quantile,vec7_quantile,vec8_quantile,
         vec9_quantile,vec10_quantile,vec11_quantile,vec12_quantile,vec13_quantile,vec14_quantile,vec15_quantile,vec16_quantile,
         vec17_quantile,vec18_quantile,vec19_quantile,vec20_quantile,vec21_quantile,vec22_quantile,vec23_quantile,vec24_quantile,
         vec25_quantile,vec26_quantile,vec27_quantile,vec28_quantile,vec29_quantile,vec30_quantile,vec31_quantile,vec32_quantile,
         vec33_quantile,vec34_quantile,vec35_quantile,vec36_quantile,vec37_quantile,vec38_quantile,vec39_quantile,vec40_quantile,
         vec41_quantile,vec42_quantile,vec43_quantile,vec44_quantile,vec45_quantile,vec46_quantile,vec47_quantile,vec48_quantile,
         vec49_quantile,vec50_quantile
  from $income_1001_phone_contacts_index a
  inner join
  $phone_contacts_word2vec_index b on b.day='$phoneContactsWord2vecIndexLastPar' and a.phone=b.phone
  where a.day='$day'
) t
LATERAL VIEW explode(Array(vec1_quantile+21496,
                           vec2_quantile+21500,
                           vec3_quantile+21504,
                           vec4_quantile+21508,
                           vec5_quantile+21512,
                           vec6_quantile+21516,
                           vec7_quantile+21520,
                           vec8_quantile+21524,
                           vec9_quantile+21528,
                           vec10_quantile+21532,
                           vec11_quantile+21536,
                           vec12_quantile+21540,
                           vec13_quantile+21544,
                           vec14_quantile+21548,
                           vec15_quantile+21552,
                           vec16_quantile+21556,
                           vec17_quantile+21560,
                           vec18_quantile+21564,
                           vec19_quantile+21568,
                           vec20_quantile+21572,
                           vec21_quantile+21576,
                           vec22_quantile+21580,
                           vec23_quantile+21584,
                           vec24_quantile+21588,
                           vec25_quantile+21592,
                           vec26_quantile+21596,
                           vec27_quantile+21600,
                           vec28_quantile+21604,
                           vec29_quantile+21608,
                           vec30_quantile+21612,
                           vec31_quantile+21616,
                           vec32_quantile+21620,
                           vec33_quantile+21624,
                           vec34_quantile+21628,
                           vec35_quantile+21632,
                           vec36_quantile+21636,
                           vec37_quantile+21640,
                           vec38_quantile+21644,
                           vec39_quantile+21648,
                           vec40_quantile+21652,
                           vec41_quantile+21656,
                           vec42_quantile+21660,
                           vec43_quantile+21664,
                           vec44_quantile+21668,
                           vec45_quantile+21672,
                           vec46_quantile+21676,
                           vec47_quantile+21680,
                           vec48_quantile+21684,
                           vec49_quantile+21688,
                           vec50_quantile+21692)) a as index;
"