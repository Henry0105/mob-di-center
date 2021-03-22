#!/bin/bash
: '
@owner:liuyanqiang
@describe:交通枢纽poi ssid匹配
@projectName:mobdi
'

set -e -x

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#源表
#poi_config_mapping_par=dm_sdk_mapping.poi_config_mapping_par

#中间库
traffic_poi_info=dw_mobdi_tmp.traffic_poi_info
traffic_poi_and_bssid_connect_info=dw_mobdi_tmp.traffic_poi_and_bssid_connect_info
ssid_match_data_prepare=dw_mobdi_tmp.ssid_match_data_prepare
traffic_poi_and_ssid_connect_count_info=dw_mobdi_tmp.traffic_poi_and_ssid_connect_count_info
traffic_ssid_calculate_base_info=dw_mobdi_tmp.traffic_ssid_calculate_base_info
traffic_name_match_ssid_info=dw_mobdi_tmp.traffic_name_match_ssid_info
traffic_ssid_cn_match=dw_mobdi_tmp.traffic_ssid_cn_match
city_name_combine_area_name=dw_mobdi_tmp.city_name_combine_area_name
traffic_ssid_cn_keyword_match=dw_mobdi_tmp.traffic_ssid_cn_keyword_match
traffic_ssid_famous_en_match=dw_mobdi_tmp.traffic_ssid_famous_en_match
traffic_ssid_rude_match=dw_mobdi_tmp.traffic_ssid_rude_match
traffic_ssid_match_merge_all_conditions=dw_mobdi_tmp.traffic_ssid_match_merge_all_conditions
traffic_split_ssid_pinyin_match=dw_mobdi_tmp.traffic_split_ssid_pinyin_match
traffic_split_ssid_pinyin_short_match=dw_mobdi_tmp.traffic_split_ssid_pinyin_short_match
traffic_ssid_pinyin_match=dw_mobdi_tmp.traffic_ssid_pinyin_match
traffic_ssid_pinyin_short_match=dw_mobdi_tmp.traffic_ssid_pinyin_short_match
traffic_bssid_remain_1=dw_mobdi_tmp.traffic_bssid_remain_1
traffic_ssid_match_second_confidence=dw_mobdi_tmp.traffic_ssid_match_second_confidence
traffic_bssid_remain_2=dw_mobdi_tmp.traffic_bssid_remain_2
traffic_ssid_match_third_confidence=dw_mobdi_tmp.traffic_ssid_match_third_confidence
traffic_poi_and_bssid_info=dw_mobdi_tmp.traffic_poi_and_bssid_info
one_bssid_ssid_with_multiple_traffic_info=dw_mobdi_tmp.one_bssid_ssid_with_multiple_traffic_info

#目标表
#dim_traffic_ssid_bssid_match_info_mf=dm_mobdi_mapping.dim_traffic_ssid_bssid_match_info_mf

#mapping表
#dim_mapping_bssid_location_mf=dm_mobdi_mapping.dim_mapping_bssid_location_mf

#解析交通枢纽poi表
hive -v -e "
insert overwrite table $traffic_poi_info partition(day='$day')
select name,poi_lat,poi_lon,geohash7,city_code,type_name
from
(
  select trim(name) as name,
         lat as poi_lat,
         lon as poi_lon,
         geohash7,
         city as city_code,
         get_json_object(attribute,'$.type_name') as type_name
  from $poi_config_mapping_par
  where type=17
  and version='1000'
) t1
where type_name != '公交站'
group by name,poi_lat,poi_lon,geohash7,city_code,type_name;
"

#获取geohash点及周围的点的位置并匹配上近三个月的bssid信息
hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_geohash_adjacent as 'com.youzu.mob.java.udf.GeohashAdjacent';
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $traffic_poi_and_bssid_connect_info partition(day='$day')
select name,poi_lat,poi_lon,city_code,type_name,b.bssid,b.ssid,b.device,b.appear_day,b.connect_num
from
(
  select name,poi_lat,poi_lon,city_code,type_name,geohash7_adjacent
  from
  (
  	select name,poi_lat,poi_lon,city_code,type_name,
  	       get_geohash_adjacent(geohash7) as geohash7_n
    from $traffic_poi_info
    where day='$day'
    and cast(poi_lon as double) > 73
    and cast(poi_lon as double) < 136
    and cast(poi_lat as double) > 3
    and cast(poi_lat as double) < 54
  ) a1
  lateral view explode(split(geohash7_n,',')) tt as geohash7_adjacent
) a
inner join
(
  select bssid,ssid,device,appear_day,connect_num,geohash7
  from $ssid_match_data_prepare
  where day='$day'
) b on a.geohash7_adjacent=b.geohash7;
"

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.map.aggr=false;
--计算交通枢纽poi的ssid连接次数、连接设备数、活跃天数、bssid数
insert overwrite table $traffic_poi_and_ssid_connect_count_info partition(day='$day')
select t1.name,t1.poi_lat,t1.poi_lon,city_code,type_name,t1.ssid,device_num,connect_num,active_days,bssid_num
from
(
  select name,poi_lat,poi_lon,city_code,type_name,ssid,
         count(1) as device_num,sum(connect_num) connect_num
  from
  (
    select name,poi_lat,poi_lon,city_code,type_name,ssid,device,sum(connect_num) as connect_num
    from $traffic_poi_and_bssid_connect_info
    where day='$day'
    group by name,poi_lat,poi_lon,city_code,type_name,ssid,device
  ) a
  group by name,poi_lat,poi_lon,city_code,type_name,ssid
) t1
left join
(
  select name,poi_lat,poi_lon,ssid,count(1) as active_days
  from
  (
    select name,poi_lat,poi_lon,ssid,appear_day
    from $traffic_poi_and_bssid_connect_info
    where day='$day'
    group by name,poi_lat,poi_lon,ssid,appear_day
  ) a
  group by name,poi_lat,poi_lon,ssid
) t2 on t1.name=t2.name and t1.poi_lat=t2.poi_lat and t1.poi_lon=t2.poi_lon and t1.ssid=t2.ssid
left join
(
  select name,poi_lat,poi_lon,ssid,count(1) as bssid_num
  from
  (
    select name,poi_lat,poi_lon,ssid,bssid
    from $traffic_poi_and_bssid_connect_info
    where day='$day'
    group by name,poi_lat,poi_lon,ssid,bssid
  ) a
  group by name,poi_lat,poi_lon,ssid
) t3 on t1.name=t3.name and t1.poi_lat=t3.poi_lat and t1.poi_lon=t3.poi_lon and t1.ssid=t3.ssid;

--交通枢纽取出前5个主要的ssid
insert overwrite table $traffic_ssid_calculate_base_info partition(day='$day')
select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
from
(
  select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag,
         row_number() over(partition by name order by device_num desc,bssid_num desc,connect_num desc) rn
  from
  (
    select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,
           case
             when (regexp_extract(lower(ssid),'(haidilao|sanfu|kfc|macdonald|mcd|starbucks|coffee|pizza|huoguo|malatang|hutaoli|肯德基|麦当劳|饭店|餐厅|自助|火锅|烤鱼|咖啡|美食|披萨|烧烤|甜品|海鲜|麻辣烫|沙县|小吃|拉面)',1)!=''
                  or (regexp_extract(lower(name),'(菜)',1)='' and regexp_extract(lower(ssid),'(菜)',1)!=''))
             then 1
             when (regexp_extract(lower(ssid),'(spa|ikea|ktv|haoledi|disco|lianjia|好乐迪|量贩版|健身|会所|便利|网吧|网咖|电影|影城|影院|影视|地产|不动产|房产)',1)!=''                 or (regexp_extract(lower(name),'(店)',1)='' and regexp_extract(lower(ssid),'(店)',1)!=''))
             then 2
             when (regexp_extract(lower(ssid),'(bank|pingan|yinhang|银行|支行|分行|保险|太平洋|lifeagent|cpic)',1)!=''
                  or split(split(ssid,'-')[0],'_')[0] in ('ABC','BC','BOC','CCB','ICBC','CMBC','CITIC','CEB','COMM','SPDB','HSBC','chinalife','PingAn','招商银行'))
             then 3
             when regexp_extract(lower(ssid),'(atour|huazhu|homeinn|holidayinn|lavande|super8|jinjiang|greentree|7days|sevendays|podinns|wyn88|quanji|ramada|hyatt|shangri-la|sheraton|capitaland|hilton|motel|inn$|inns|hyatt|hotel|lvguan|jiudian|binguan|青旅|旅馆|宾馆|交通枢纽)',1)!=''
             then 4
             when (regexp_extract(lower(ssid),'(decathlon|carrefour|walmart|lotus|auchan|yonghui|mart|market|tesco|vanguard|hualian|lianhua|hema|超市|永辉生活|shop)',1)!=''
                  or (regexp_extract(lower(name),'(商场)',1)='' and regexp_extract(lower(ssid),'(mall)',1)!=''))
             then 5
             when regexp_extract(lower(ssid),'(edu|giwifi|young|school|大学|学院|学校|高中|中学|初中|小学|幼儿园)',1)!=''
             then 6
             when (regexp_extract(lower(ssid),'(hospital|yiyuan|医院|诊所)',1)!=''
                  or (regexp_extract(lower(name),'(院)',1)='' and regexp_extract(lower(ssid),'(院)',1)!=''))
             then 7
             when (regexp_extract(lower(ssid),'(chinanet|chinamobile|chinaunicom|aiwifi|^智慧|智慧城市|魅力北海|无线东莞|移动|联通|电信)',1)!=''
                  or split(split(ssid,'-')[0],'_')[0] in ('ChinaNet','ChinaNGB','ChinaMobile','ChinaUnicom','CMCC','CCINN','aWiFi','i','I','@i','and','114 Free','iNingbo','@iWuhan','SZ','YANGZHOU','ifuzhou'))
             then 8
             when regexp_extract(lower(ssid),'(metro|subway|train|airport|chengcheyi|heikuai|地铁|高铁|火车|车站|公交|巴士|客车|客运|航站楼|机场|售票|候机|候车)',1)!=''
             then 9
             when regexp_extract(lower(ssid),'(wanda|ffan|vanke|powerlong|longfor|wuyue|intime|joycity|9square|mixc|plaza|mall|万达|广场|商场)',1)!=''
             then 10
             else 0
           end as flag
    from $traffic_poi_and_ssid_connect_count_info
    where day='$day'
    and not (regexp_extract(lower(ssid),'(xiaomi[^-_]|huawei[^-_]|honor|vivo|oppo|iphone|samsung|redmi|mi[\\\\s]|小米|手机|华为|三星)',1)!=''
              and regexp_extract(lower(ssid),'(专卖|维修|店)',1)='')
    and split(split(ssid,'-')[0],'_')[0] not in ('','0x','  小米共享WiFi',' 免费安全共享WiFi','360WiFi','360行车记录仪','360免费WiFi')
  ) a
  where flag not in (6,7)
) b
where rn<=5;
"

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
--汉字推断
--先筛选掉ssid名字没有汉字的数据
--然后去掉非汉字部分,针对既有汉字又有字母的，后面会对字母单独处理
--计算ssid与交通枢纽name匹配上的字数跟词的名字，按照两个字来匹配
insert overwrite table $traffic_name_match_ssid_info partition(day='$day')
select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag,
       word1,word2,word3,word4,word5,word6,word7,word8,flag1,flag2,flag3,flag4,flag5,flag6,flag7,flag8,match1,match2,match3,match4,match5,match6,match7,match8,
           match_num,regexp_extract(concat_ws('',array(match1,match2,match3,match4,match5,match6,match7,match8)),'([^_]+(.*[^_]+)*)',1) as match_word
from
(
  select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag,
         word1,word2,word3,word4,word5,word6,word7,word8,flag1,flag2,flag3,flag4,flag5,flag6,flag7,flag8,
         case when flag1=1 then ssid_array[1] else '_' end as match1,
         case when (flag1=1 or flag2=1) then ssid_array[2] else '_' end as match2,
         case when (flag2=1 or flag3=1) then ssid_array[3] else '_' end as match3,
         case when (flag3=1 or flag4=1) then ssid_array[4] else '_' end as match4,
         case when (flag4=1 or flag5=1) then ssid_array[5] else '_' end as match5,
         case when (flag5=1 or flag6=1) then ssid_array[6] else '_' end as match6,
         case when (flag6=1 or flag7=1) then ssid_array[7] else '_' end as match7,
         case when (flag7=1 or flag8=1) then ssid_array[8] else '_' end as match8,
         2*(flag1+flag2+flag3+flag4+flag5+flag6+flag7+flag8)-(flag1*flag2+flag2*flag3+flag3*flag4+flag4*flag5+flag5*flag6+flag6*flag7+flag7*flag8) as match_num
  from
  (
    select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag,ssid_array,
           word1,word2,word3,word4,word5,word6,word7,word8,
           case when length(word1)=2 and instr(name,word1)>0 then 1 else 0 end as flag1,
           case when length(word2)=2 and instr(name,word2)>0 then 1 else 0 end as flag2,
           case when length(word3)=2 and instr(name,word3)>0 then 1 else 0 end as flag3,
           case when length(word4)=2 and instr(name,word4)>0 then 1 else 0 end as flag4,
           case when length(word5)=2 and instr(name,word5)>0 then 1 else 0 end as flag5,
           case when length(word6)=2 and instr(name,word6)>0 then 1 else 0 end as flag6,
           case when length(word7)=2 and instr(name,word7)>0 then 1 else 0 end as flag7,
           case when length(word8)=2 and instr(name,word8)>0 then 1 else 0 end as flag8
    from
    (
      select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag,ssid_array,
             concat(ssid_array[1],ssid_array[2]) as word1,
             concat(ssid_array[2],ssid_array[3]) as word2,
             concat(ssid_array[3],ssid_array[4]) as word3,
             concat(ssid_array[4],ssid_array[5]) as word4,
             concat(ssid_array[5],ssid_array[6]) as word5,
             concat(ssid_array[6],ssid_array[7]) as word6,
             concat(ssid_array[7],ssid_array[8]) as word7,
             concat(ssid_array[8],ssid_array[9]) as word8
      from
      (
        select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag,
               split(regexp_replace(lower(ssid),'([^\\\\u4e00-\\\\u9fa5])',''), '') as ssid_array
        from $traffic_ssid_calculate_base_info
        where day='$day'
        and regexp_extract(lower(ssid),'([\\\\u4e00-\\\\u9fa5])',1)!=''
      )a
    )b
  )c
)d;

--汉字匹配规则
insert overwrite table $traffic_ssid_cn_match partition(day='$day')
select name,poi_lat,poi_lon,a.city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
from
(
  select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag,match_num,match_word
  from $traffic_name_match_ssid_info
  where day='$day'
) a
left join
$city_name_combine_area_name b on b.day='$day' and a.city_code=b.city_code
where instr(b.area_list,match_word)=0
and (match_num>=4
  or (length(match_word) in (2,3) and bssid_num>=2 and device_num>=30 and connect_num>=130)
);

--汉字关键字匹配
insert overwrite table $traffic_ssid_cn_keyword_match partition(day='$day')
select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
from $traffic_ssid_calculate_base_info
where day='$day'
and regexp_extract(lower(ssid),'([\\\\u4e00-\\\\u9fa5])',1)!=''
and (
  (type_name='机场' and regexp_extract(ssid,'(航站楼|机场|售票|候机)',1)!='')
  or (type_name='火车站' and regexp_extract(ssid,'(高铁|火车|车站|售票|候车|[东南西北总]站)',1)!='')
  or (type_name='地铁站' and regexp_extract(ssid,'(地铁|售票|候车)',1)!='')
  or (type_name='长途汽车站' and regexp_extract(ssid,'(巴士|客运|客车|车站|售票|公交|候车|[东南西北总]站)',1)!='')
);
"

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
--交通枢纽匹配英文名
insert overwrite table $traffic_ssid_famous_en_match partition(day='$day')
select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
from $traffic_ssid_calculate_base_info
where day='$day'
and (
  (type_name='机场' and regexp_extract(lower(ssid),'(terminal|airport|ticket|departure|lounge)',1)!='')
  or (type_name='火车站' and regexp_extract(lower(ssid),'(rail|train|station|ticket|waiting)',1)!='')
  or (type_name='地铁站' and regexp_extract(lower(ssid),'(metro|subway|ticket|waiting)',1)!='')
  or (type_name='长途汽车站' and regexp_extract(lower(ssid),'(bus[^i]|passenger|transport|traffic|station|ticket|car)',1)!='')
);
"

#拼音推断
spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.bssidmapping.TrafficPinyinMatch \
--driver-memory 8G \
--executor-memory 15G \
--executor-cores 5 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=50 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.speculation=true \
--conf spark.driver.maxResultSize=4g \
--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=1024m -XX:PermSize=256m" \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT.jar "$day" "$dw_mobdi_tmp"

#考虑所有的组合情况，看是否匹配
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $traffic_ssid_rude_match partition(day='$day')
select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
from $traffic_ssid_calculate_base_info
where day='$day'
and instr(lower(name),lower(ssid))>0
and regexp_extract(lower(ssid),'([a-z0-9\\\\u4e00-\\\\u9fa5])',1)!=''
and length(ssid)>=2;
"

#合并上述所有情况
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $traffic_ssid_match_merge_all_conditions partition(day='$day')
select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
from
(
    select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
    from $traffic_ssid_cn_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
    from $traffic_ssid_cn_keyword_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
    from $traffic_ssid_famous_en_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
    from $traffic_split_ssid_pinyin_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
    from $traffic_split_ssid_pinyin_short_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
    from $traffic_ssid_pinyin_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
    from $traffic_ssid_pinyin_short_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
    from $traffic_ssid_rude_match
    where day='$day'
)a
group by name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag;
"

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
--计算剩下的交通枢纽（由于相邻交通枢纽的存在,之前匹配上的ssid也要剔除）
insert overwrite table $traffic_bssid_remain_1 partition(day='$day')
select name,poi_lat,poi_lon,city_code,type_name,d.ssid,device_num,connect_num,active_days,bssid_num,flag
from
(
  select a.name,a.poi_lat,a.poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
  from $traffic_ssid_calculate_base_info a
  left join
  (
    select name
  	from $traffic_ssid_match_merge_all_conditions
  	where day='$day'
  	group by name
  ) b on a.name=b.name
  where a.day='$day'
  and b.name is null
)d
left join
(
  select ssid
  from $traffic_ssid_match_merge_all_conditions
  where day='$day'
  group by ssid
) c on d.ssid=c.ssid
where c.ssid is null;

--剩下的交通枢纽
--取出设备数>=100，连接数>=400，活跃天数>=45天的数据
--取出rank前三的ssid，排序标准：设备数，活跃天数
--得到置信度第二高的ssid数据
insert overwrite table $traffic_ssid_match_second_confidence partition(day='$day')
select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag,rank
from
(
  select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag,
         row_number() over (partition by name order by device_num desc,active_days desc) as rank
  from $traffic_bssid_remain_1
  where day='$day'
  and device_num>=100
  and connect_num>=400
  and active_days>=45
) a
where rank<=3;
"

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
--继续计算剩下的交通枢纽（由于相邻交通枢纽的存在,之前匹配上的ssid也要剔除）
insert overwrite table $traffic_bssid_remain_2 partition(day='$day')
select name,poi_lat,poi_lon,city_code,type_name,d.ssid,device_num,connect_num,active_days,bssid_num,flag
from
(
  select a.name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag
  from $traffic_bssid_remain_1 a
  left join
  (
  	select name
  	from $traffic_ssid_match_second_confidence
  	where day='$day'
  	group by name
  ) b on a.name=b.name
  where a.day='$day'
  and b.name is null
)d
left join
(
  select ssid
  from $traffic_ssid_match_second_confidence
  where day='$day'
  group by ssid
) c on d.ssid=c.ssid
where c.ssid is null;

--继续计算剩下的交通枢纽
--取出rank前三的ssid，排序标准：是否满足bssid数>=2 设备数>=10 活跃天数>=30，设备数，活跃天数
--得到置信度第三的ssid数据
insert overwrite table $traffic_ssid_match_third_confidence partition(day='$day')
select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag,flag_active,rank
from
(
  select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag,flag_active,
         row_number() over (partition by name order by flag_active desc,active_days desc,device_num desc) as rank
  from
  (
  	select name,poi_lat,poi_lon,city_code,type_name,ssid,device_num,connect_num,active_days,bssid_num,flag,
           case
             when bssid_num>=2 and device_num>=10 and active_days>=30 then 1
             else 0
           end as flag_active
    from $traffic_bssid_remain_2
    where day='$day'
  ) a
) b
where rank<=3;
"

bssidMappingLastParStr=`hive -e "show partitions $dim_mapping_bssid_location_mf" | sort| tail -n 1`
#先匹配所有交通枢纽的bssid
#在已算出的ssid的交通枢纽中，匹配上bssid
#然后计算哪些bssid、ssid出现在多个不同的交通枢纽中
hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_geohash_adjacent as 'com.youzu.mob.java.udf.GeohashAdjacent';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $traffic_poi_and_bssid_info partition(day='$day')
select name,poi_lat,poi_lon,a.geohash7_adjacent,b.bssid,b.ssid
from
(
  select name,poi_lat,poi_lon,geohash7_adjacent
  from
  (
  	select name,poi_lat,poi_lon,
  	       get_geohash_adjacent(geohash7) as geohash7_n
    from $traffic_poi_info
    where day='$day'
    and cast(poi_lon as double) > 73
    and cast(poi_lon as double) < 136
    and cast(poi_lat as double) > 3
    and cast(poi_lat as double) < 54
  ) a1
  lateral view explode(split(geohash7_n,',')) tt as geohash7_adjacent
) a
inner join
(
  select trim(bssid) as bssid,ssid,lon,lat,substr(geohash8,1,7) as geohash7
  from $dim_mapping_bssid_location_mf
  where $bssidMappingLastParStr
  and cast(lon as double) > 73
  and cast(lon as double) < 136
  and cast(lat as double) > 3
  and cast(lat as double) < 54
  and trim(bssid) not in ('','00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
  group by trim(bssid),ssid,lon,lat,substr(geohash8,1,7)
) b on a.geohash7_adjacent=b.geohash7;

with calculate_duplicate_pre as (
  select result.name,result.poi_lat,result.poi_lon,result.ssid,b.bssid
  from
  (
    select name,ssid,poi_lat,poi_lon
    from $traffic_ssid_match_merge_all_conditions
    where day='$day'
    union all
    select name,ssid,poi_lat,poi_lon
    from $traffic_ssid_match_second_confidence
    where day='$day'
    union all
    select name,ssid,poi_lat,poi_lon
    from $traffic_ssid_match_third_confidence
    where day='$day'
  ) result
  inner join
  $traffic_poi_and_bssid_info b
  on b.day='$day' and result.name=b.name and result.poi_lat=b.poi_lat and result.poi_lon=b.poi_lon and result.ssid=b.ssid
)
insert overwrite table $one_bssid_ssid_with_multiple_traffic_info partition(day='$day')
select name,poi_lat,poi_lon,calculate_duplicate_pre.ssid
from calculate_duplicate_pre
inner join
(
  select bssid,ssid
  from
  (
    select bssid,ssid,min(poi_lat) as lat_min,min(poi_lon) as lon_min,max(poi_lat) as lat_max,max(poi_lon) as lon_max
    from
    (
      select bssid,ssid,name,max(poi_lat) as poi_lat,max(poi_lon) as poi_lon
      from calculate_duplicate_pre
      group by bssid,ssid,name
    ) t
    group by bssid,ssid
  ) t1
  where lat_min != lat_max
  or lon_min != lon_max
) b on calculate_duplicate_pre.bssid=b.bssid and calculate_duplicate_pre.ssid=b.ssid
group by name,poi_lat,poi_lon,calculate_duplicate_pre.ssid;
"

#最终结果
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $dim_traffic_ssid_bssid_match_info_mf partition(day='$day')
select t1.name,t1.ssid,t1.poi_lat,t1.poi_lon,city_code,type_name,device_num,connect_num,active_days,bssid_num,flag,
       round(if(t3.name is not null,confidence-0.1,confidence),1) as confidence,
       t2.bssid_array
from
(
  select name,ssid,poi_lat,poi_lon,city_code,type_name,device_num,connect_num,active_days,bssid_num,flag,1.0 as confidence
  from $traffic_ssid_match_merge_all_conditions
  where day='$day'
  union all
  select name,ssid,poi_lat,poi_lon,city_code,type_name,device_num,connect_num,active_days,bssid_num,flag,0.6-(rank-1)/10 as confidence
  from $traffic_ssid_match_second_confidence
  where day='$day'
  union all
  select name,ssid,poi_lat,poi_lon,city_code,type_name,device_num,connect_num,active_days,bssid_num,flag,0.3-(rank-1)/10 as confidence
  from $traffic_ssid_match_third_confidence
  where day='$day'
) t1
left join
(
  select name,poi_lat,poi_lon,ssid,collect_list(bssid) as bssid_array
  from $traffic_poi_and_bssid_info
  where day='$day'
  group by name,poi_lat,poi_lon,ssid
) t2 on t1.name=t2.name and t1.poi_lat=t2.poi_lat and t1.poi_lon=t2.poi_lon and t1.ssid=t2.ssid
left join
$one_bssid_ssid_with_multiple_traffic_info t3
on t3.day='$day' and t1.name=t3.name and t1.poi_lat=t3.poi_lat and t1.poi_lon=t3.poi_lon and t1.ssid=t3.ssid;
"
