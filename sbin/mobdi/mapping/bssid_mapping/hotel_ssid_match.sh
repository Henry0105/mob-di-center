#!/bin/bash
: '
@owner:liuyanqiang
@describe:酒店poi ssid匹配
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
hotel_poi_info=dw_mobdi_tmp.hotel_poi_info
hotel_poi_and_bssid_connect_info=dw_mobdi_tmp.hotel_poi_and_bssid_connect_info
ssid_match_data_prepare=dw_mobdi_tmp.ssid_match_data_prepare
hotel_poi_and_ssid_connect_count_info=dw_mobdi_tmp.hotel_poi_and_ssid_connect_count_info
hotel_ssid_calculate_base_info=dw_mobdi_tmp.hotel_ssid_calculate_base_info
hotel_name_match_ssid_info=dw_mobdi_tmp.hotel_name_match_ssid_info
hotel_ssid_cn_match=dw_mobdi_tmp.hotel_ssid_cn_match
city_name_combine_area_name=dw_mobdi_tmp.city_name_combine_area_name
hotel_ssid_3number_match=dw_mobdi_tmp.hotel_ssid_3number_match
hotel_ssid_name_en_match=dw_mobdi_tmp.hotel_ssid_name_en_match
hotel_ssid_famous_en_match=dw_mobdi_tmp.hotel_ssid_famous_en_match
hotel_ssid_rude_match=dw_mobdi_tmp.hotel_ssid_rude_match
hotel_ssid_match_merge_all_conditions=dw_mobdi_tmp.hotel_ssid_match_merge_all_conditions
hotel_split_ssid_pinyin_match=dw_mobdi_tmp.hotel_split_ssid_pinyin_match
hotel_split_ssid_pinyin_short_match=dw_mobdi_tmp.hotel_split_ssid_pinyin_short_match
hotel_ssid_pinyin_match=dw_mobdi_tmp.hotel_ssid_pinyin_match
hotel_ssid_pinyin_short_match=dw_mobdi_tmp.hotel_ssid_pinyin_short_match
hotel_ssid_floor_room_split=dw_mobdi_tmp.hotel_ssid_floor_room_split
hotel_ssid_floor_room_info=dw_mobdi_tmp.hotel_ssid_floor_room_info
hotel_ssid_floor_room_info_filter_by_room_block=dw_mobdi_tmp.hotel_ssid_floor_room_info_filter_by_room_block
hotel_ssid_room_block=dw_mobdi_tmp.hotel_ssid_room_block
hotel_ssid_room_block_room_scope=dw_mobdi_tmp.hotel_ssid_room_block_room_scope
hotel_ssid_floor_block=dw_mobdi_tmp.hotel_ssid_floor_block
hotel_ssid_floor_block_count_info=dw_mobdi_tmp.hotel_ssid_floor_block_count_info
hotel_ssid_floor_block_count_rate_info=dw_mobdi_tmp.hotel_ssid_floor_block_count_rate_info
hotel_ssid_floor_block_rate_rank_filter=dw_mobdi_tmp.hotel_ssid_floor_block_rate_rank_filter
hotel_ssid_floor_block_filter=dw_mobdi_tmp.hotel_ssid_floor_block_filter
hotel_ssid_floor_room_match=dw_mobdi_tmp.hotel_ssid_floor_room_match
hotel_bssid_remain_1=dw_mobdi_tmp.hotel_bssid_remain_1
hotel_ssid_match_second_confidence=dw_mobdi_tmp.hotel_ssid_match_second_confidence
hotel_bssid_remain_2=dw_mobdi_tmp.hotel_bssid_remain_2
hotel_ssid_match_third_confidence=dw_mobdi_tmp.hotel_ssid_match_third_confidence
hotel_poi_and_bssid_info=dw_mobdi_tmp.hotel_poi_and_bssid_info
one_bssid_ssid_with_multiple_hotel_info=dw_mobdi_tmp.one_bssid_ssid_with_multiple_hotel_info


#目标表
#dim_hotel_ssid_bssid_match_info_mf=dm_mobdi_mapping.dim_hotel_ssid_bssid_match_info_mf

#mapping表
#dim_mapping_bssid_location_mf=dm_mobdi_mapping.dim_mapping_bssid_location_mf

#解析酒店poi表
hive -v -e "
insert overwrite table $hotel_poi_info partition(day='$day')
select name,poi_lat,poi_lon,geohash7,city_code
from
(
  select trim(name) as name,
         lat as poi_lat,
         lon as poi_lon,
         geohash7,
         city as city_code
  from $poi_config_mapping_par
  where type=6
  and version='1000'
) t1
group by name,poi_lat,poi_lon,geohash7,city_code;
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
insert overwrite table $hotel_poi_and_bssid_connect_info partition(day='$day')
select name,poi_lat,poi_lon,city_code,b.bssid,b.ssid,b.device,b.appear_day,b.connect_num
from
(
  select name,poi_lat,poi_lon,city_code,geohash7_adjacent
  from
  (
  	select name,poi_lat,poi_lon,city_code,
  	       get_geohash_adjacent(geohash7) as geohash7_n
    from $hotel_poi_info
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
--计算酒店poi的ssid连接次数、连接设备数、活跃天数、bssid数
insert overwrite table $hotel_poi_and_ssid_connect_count_info partition(day='$day')
select t1.name,t1.poi_lat,t1.poi_lon,city_code,t1.ssid,device_num,connect_num,active_days,bssid_num
from
(
  select name,poi_lat,poi_lon,city_code,ssid,
         count(1) as device_num,sum(connect_num) connect_num
  from
  (
    select name,poi_lat,poi_lon,city_code,ssid,device,sum(connect_num) as connect_num
    from $hotel_poi_and_bssid_connect_info
    where day='$day'
    group by name,poi_lat,poi_lon,city_code,ssid,device
  ) a
  group by name,poi_lat,poi_lon,city_code,ssid
) t1
left join
(
  select name,poi_lat,poi_lon,ssid,count(1) as active_days
  from
  (
    select name,poi_lat,poi_lon,ssid,appear_day
    from $hotel_poi_and_bssid_connect_info
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
    from $hotel_poi_and_bssid_connect_info
    where day='$day'
    group by name,poi_lat,poi_lon,ssid,bssid
  ) a
  group by name,poi_lat,poi_lon,ssid
) t3 on t1.name=t3.name and t1.poi_lat=t3.poi_lat and t1.poi_lon=t3.poi_lon and t1.ssid=t3.ssid;

--取出所有酒店的ssid
insert overwrite table $hotel_ssid_calculate_base_info partition(day='$day')
select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,flag
from
(
  select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
         case
           when (regexp_extract(lower(ssid),'(haidilao|sanfu|kfc|macdonald|mcd|starbucks|coffee|pizza|huoguo|malatang|hutaoli|肯德基|麦当劳|饭店|餐厅|自助|火锅|烤鱼|咖啡|美食|披萨|烧烤|甜品|海鲜|麻辣烫|沙县|小吃|拉面)',1)!=''
                or (regexp_extract(lower(name),'(菜)',1)='' and regexp_extract(lower(ssid),'(菜)',1)!=''))
           then 1
           when (regexp_extract(lower(ssid),'(spa|ikea|ktv|haoledi|disco|lianjia|好乐迪|量贩版|健身|会所|便利|网吧|网咖|电影|影城|影院|影视|地产|不动产|房产)',1)!=''                 or (regexp_extract(lower(name),'(店)',1)='' and regexp_extract(lower(ssid),'(店)',1)!=''))
           then 2
           when (regexp_extract(lower(ssid),'(bank|pingan|yinhang|银行|支行|分行|保险|太平洋|lifeagent|cpic)',1)!=''
                or split(split(ssid,'-')[0],'_')[0] in ('ABC','BC','BOC','CCB','ICBC','CMBC','CITIC','CEB','COMM','SPDB','HSBC','chinalife','PingAn','招商银行'))
           then 3
           when regexp_extract(lower(ssid),'(atour|huazhu|homeinn|holidayinn|lavande|super8|jinjiang|greentree|7days|sevendays|podinns|wyn88|quanji|ramada|hyatt|shangri-la|sheraton|capitaland|hilton|motel|inn$|inns|hyatt|hotel|lvguan|jiudian|binguan|青旅|旅馆|宾馆|酒店)',1)!=''
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
  from $hotel_poi_and_ssid_connect_count_info
  where day='$day'
  and not (regexp_extract(lower(ssid),'(xiaomi[^-_]|huawei[^-_]|honor|vivo|oppo|iphone|samsung|redmi|mi[\\\\s]|小米|手机|华为|三星)',1)!=''
            and regexp_extract(lower(ssid),'(专卖|维修|店)',1)='')
  and split(split(ssid,'-')[0],'_')[0] not in ('','0x','  小米共享WiFi',' 免费安全共享WiFi','360WiFi','360行车记录仪','360免费WiFi')
) a
where flag in (4,0);
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
--计算ssid与酒店name匹配上的字数跟词的名字，按照两个字来匹配
insert overwrite table $hotel_name_match_ssid_info partition(day='$day')
select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
       word1,word2,word3,word4,word5,word6,word7,word8,flag1,flag2,flag3,flag4,flag5,flag6,flag7,flag8,match1,match2,match3,match4,match5,match6,match7,match8,
           match_num,regexp_extract(concat_ws('',array(match1,match2,match3,match4,match5,match6,match7,match8)),'([^_]+(.*[^_]+)*)',1) as match_word
from
(
  select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
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
    select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,ssid_array,
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
      select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,ssid_array,
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
        select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
               split(regexp_replace(lower(ssid),'([^\\\\u4e00-\\\\u9fa5])',''), '') as ssid_array
        from $hotel_ssid_calculate_base_info
        where day='$day'
        and regexp_extract(lower(ssid),'([\\\\u4e00-\\\\u9fa5])',1)!=''
      )a
    )b
  )c
)d;

--汉字匹配规则
--需要去掉酒店|宾馆|旅馆|青旅等关键字，并且剩余的汉字不能是城市地区名
insert overwrite table $hotel_ssid_cn_match partition(day='$day')
select name,poi_lat,poi_lon,a.city_code,ssid,device_num,connect_num,active_days,bssid_num
from
(
  select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,match_num,
         regexp_replace(match_word,'[大]?酒店|[大]?宾馆|旅馆|青旅|酒店|客栈|民宿|招待所|旅社|公寓|住宿|客房|_','') as match_word_new
  from $hotel_name_match_ssid_info
  where day='$day'
) a
left join
$city_name_combine_area_name b on b.day='$day' and a.city_code=b.city_code
where instr(b.area_list,match_word_new)=0
and (match_num>=4
  or (length(match_word_new)=3 and bssid_num>=2 and device_num>=30 and connect_num>=130)
  or match_word_new in ('锦江之星','格林豪泰','尚客优','丽枫','麗枫','如家','汉庭','速8','明珠','168','118','99','全季',
       '万豪','7天','七天','布丁','锐思特','宜必思','莫泰','桔子','希尔顿','喜来登','香格里拉','洲际','假日','凯宾斯基','凯悦','威斯汀')
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
--数字推断
--先筛选ssid和name有数字的数据
--ssid数字要连续三位以上，并且要与酒店name中的数字匹配
insert overwrite table $hotel_ssid_3number_match partition(day='$day')
select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
       ssid_num,ssid_split
from
(
  select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
         regexp_replace(ssid,'[^0-9]','-') as ssid_num
  from $hotel_ssid_calculate_base_info
  where day='$day'
  and regexp_extract(lower(ssid),'([0-9]+)',1)!=''
  and regexp_extract(lower(name),'([0-9]+)',1)!=''
) a
lateral view explode(split(ssid_num,'-'))b as ssid_split
where length(ssid_split)>=3
and regexp_extract(lower(name),'([0-9]{1,})',1)=ssid_split;
"

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
--英文字母推断
--筛选ssid名字有字母的数据
--酒店名称本身就为英文的情况(cc mall -> cc mall,ccmall)
insert overwrite table $hotel_ssid_name_en_match partition(day='$day')
select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
       name_en1,name_en2,ssid_en1,ssid_en2
from
(
  select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
         regexp_extract(lower(name),'([a-z].*[a-z])',1) as name_en1,
         regexp_replace(lower(name),'[^a-z]','') as name_en2,
         lower(ssid) as ssid_en1,
         regexp_extract(lower(ssid),'([a-z]+)',1) as ssid_en2
  from $hotel_ssid_calculate_base_info
  where day='$day'
  and regexp_extract(lower(ssid),'([a-z])',1)!=''
)a
where (length(name_en1)>=3 and instr(ssid_en1,name_en1)!=0)
or (length(ssid_en2)>=3 and instr(name_en1,ssid_en2)!=0)
or (length(name_en2)>=3 and instr(ssid_en1,name_en2)!=0)
or (length(ssid_en2)>=3 and instr(name_en2,ssid_en2)!=0);

--大的连锁品牌酒店去匹配英文名
insert overwrite table $hotel_ssid_famous_en_match partition(day='$day')
select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
from $hotel_ssid_calculate_base_info
where day='$day'
and ((instr(name,'锦江之星')>0 and instr(lower(ssid),'jinjiang')>0)
  or (instr(name,'格林豪泰')>0 and regexp_extract(lower(ssid),'(green[\\\\s]*tree)',1)!='')
  or (instr(name,'尚客优')>0 and regexp_extract(lower(ssid),'(thank|shangkeyou)',1)!='')
  or (regexp_extract(name,'([7七]天)',1)!='' and instr(lower(ssid),'7day')>0)
  or (instr(name,'如家')>0 and instr(lower(ssid),'homeinn')>0)
  or (instr(name,'汉庭')>0 and regexp_extract(lower(ssid),'(hanting|huazhu)',1)!='')
  or (instr(name,'速8')>0 and regexp_extract(lower(ssid),'(su8|super[.*]8)',1)!='')
  or (instr(name,'明珠')>0 and regexp_extract(lower(ssid),'(mingzhu)',1)!='')
  or (instr(name,'168')>0 and regexp_extract(lower(ssid),'(168)',1)!='')
  or (instr(name,'118')>0 and regexp_extract(lower(ssid),'(118)',1)!='')
  or (instr(name,'99')>0 and regexp_extract(lower(ssid),'(99[^0-9]|jiujiu)',1)!='')
  or (instr(name,'全季')>0 and regexp_extract(lower(ssid),'(jihotel)',1)!='')
  or (instr(name,'万豪')>0 and regexp_extract(lower(ssid),'(marriott)',1)!='')
  or (regexp_extract(name,'([丽麗]枫)',1)!='' and regexp_extract(lower(ssid),'(lavande)',1)!='')
  or (instr(name,'布丁')>0 and regexp_extract(lower(ssid),'(podinn)',1)!='')
  or (instr(name,'锐思特')>0 and regexp_extract(lower(ssid),'(r[e]?st)',1)!='')
  or (instr(name,'宜必思')>0 and regexp_extract(lower(ssid),'(ibis)',1)!='')
  or (instr(name,'莫泰')>0 and regexp_extract(lower(ssid),'(motel)',1)!='')
  or (instr(name,'桔子')>0 and regexp_extract(lower(ssid),'(orange)',1)!='')
  or (instr(name,'希尔顿')>0 and regexp_extract(lower(ssid),'(hilton)',1)!='')
  or (instr(name,'喜来登')>0 and regexp_extract(lower(ssid),'(sheraton)',1)!='')
  or (instr(name,'香格里拉')>0 and regexp_extract(lower(ssid),'(shangri[-_ ]?la)',1)!='')
  or (instr(name,'洲际')>0 and regexp_extract(lower(ssid),'(intercontinental)',1)!='')
  or (instr(name,'假日')>0 and regexp_extract(lower(ssid),'(holidayinn)',1)!='')
  or (instr(name,'凯宾斯基')>0 and regexp_extract(lower(ssid),'(kempinsi)',1)!='')
  or (instr(name,'凯悦')>0 and regexp_extract(lower(ssid),'(hyatt)',1)!='')
  or (instr(name,'威斯汀')>0 and regexp_extract(lower(ssid),'(westin)',1)!='')
);
"

#拼音推断
spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.bssidmapping.HotelPinyinMatch \
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
insert overwrite table $hotel_ssid_rude_match partition(day='$day')
select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
from $hotel_ssid_calculate_base_info
where day='$day'
and instr(lower(name),lower(ssid))>0
and regexp_extract(lower(ssid),'([a-z0-9])',1)!=''
and length(ssid)>=2
and ssid not in ('酒店','宾馆','旅馆','青旅','酒店','酒楼','客栈','民宿','招待所','旅社','公寓','住宿','客房','旅社','饭店','会馆');
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
insert overwrite table $hotel_ssid_match_merge_all_conditions partition(day='$day')
select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
from
(
    select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
    from $hotel_ssid_cn_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
    from $hotel_ssid_3number_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
    from $hotel_ssid_name_en_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
    from $hotel_ssid_famous_en_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
    from $hotel_split_ssid_pinyin_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
    from $hotel_split_ssid_pinyin_short_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
    from $hotel_ssid_pinyin_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
    from $hotel_ssid_pinyin_short_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
    from $hotel_ssid_rude_match
    where day='$day'
)a
group by name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num;
"

#房间号码推断
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
--房间号预处理(tp-link_303_1_5ghz与tp-link_303_1考虑为同一个)
--按照规则将ssid切割成三份（头部、中间的房间号、尾部）
insert overwrite table $hotel_ssid_floor_room_split partition(day='$day')
select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
       regexp_extract(ssid_new,'(.*?|^)(((_2.4g|_5g)(hz)?_)?([0-9]{3,})(_[0-9]{1,2})?((_2.4g|_5g)(hz)?)?)($|[^a-z0-9]+.*)',1) as ssid_head,
       regexp_extract(ssid_new,'(.*?|^)(((_2.4g|_5g)(hz)?_)?([0-9]{3,})(_[0-9]{1,2})?((_2.4g|_5g)(hz)?)?)($|[^a-z0-9]+.*)',2) as ssid_info,
       regexp_extract(ssid_new,'(.*?|^)(((_2.4g|_5g)(hz)?_)?([0-9]{3,})(_[0-9]{1,2})?((_2.4g|_5g)(hz)?)?)($|[^a-z0-9]+.*)',11) as ssid_tail
from
(
  select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
         regexp_replace(lower(ssid),'[^_\\\\.a-z0-9\\\\u4e00-\\\\u9fa5]','_') as ssid_new
  from $hotel_ssid_calculate_base_info
  where day='$day'
  and regexp_extract(lower(ssid),'([0-9]+)',1)!=''
) a;

--计算楼层与房间信息(考虑到楼层可能0开头 0804与804)
--房间号为3-4位连续数字，楼层为1-2位，剔除楼层号码剩下的是该楼层的房号
insert overwrite table $hotel_ssid_floor_room_info partition(day='$day')
select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,ssid_info,ssid_tag,room_info,
       cast(substr(room_info,1,length(room_info)-2) as int) as floor,
       cast(substr(room_info,length(room_info)-1,2) as int) as room
from
(
  select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,ssid_info,
         concat(ssid_head,'|',ssid_tail) as ssid_tag,
         regexp_extract(ssid_info,'([0-9]{3,})',1) as room_info
  from $hotel_ssid_floor_room_split
  where day='$day'
  and length(regexp_extract(ssid_info,'([0-9]{3,})',1)) in (3,4)
)a
where substr(room_info,1,length(room_info)-2)!=0
and not(
  (regexp_extract(name,'([7七]天)',1)='' and instr(ssid_tag,'7days')>0)
  or (regexp_extract(name,'(如家)',1)='' and instr(ssid_tag,'homeinns')>0)
  or (regexp_extract(name,'(汉庭)',1)='' and instr(ssid_tag,'huazhu_hanting')>0)
  or (regexp_extract(name,'(锦江)',1)='' and instr(ssid_tag,'jinjianghotels')>0)
  or (regexp_extract(ssid_tag,'(ziroom|greentree)',1)!='')
);

--对同一楼层的房间号进行分block（房间号相差7以内认为是一个block）
insert overwrite table $hotel_ssid_room_block partition(day='$day')
select name,poi_lat,poi_lon,ssid_tag,floor,room,
       sum(flag_room_diff) over (partition by name,poi_lat,poi_lon,ssid_tag,floor order by room) as block_room
from
(
  select name,poi_lat,poi_lon,ssid_tag,floor,room,
         case when room-room_previous>7 then 1 else 0 end as flag_room_diff
  from
  (
    select name,poi_lat,poi_lon,ssid_tag,floor,room,
           lag(room,1) over(partition by name,poi_lat,poi_lon,ssid_tag,floor order by room) as room_previous
    from
    (
      select name,poi_lat,poi_lon,ssid_tag,floor,room
      from $hotel_ssid_floor_room_info
      where day='$day'
      group by name,poi_lat,poi_lon,ssid_tag,floor,room
    ) t1
  )a
)b;

--首先确定同一楼层同一block的房间号边界及房间数
--一个bolck内起码有三个房间号才认为是真的酒店房间号，计算整个酒店的最大房间号和最小房间号
insert overwrite table $hotel_ssid_room_block_room_scope partition(day='$day')
select name,poi_lat,poi_lon,ssid_tag,
       max(block_room_max) as room_max,
       min(block_room_min) as room_min
from
(
  select name,poi_lat,poi_lon,ssid_tag,floor,block_room,
         count(1) as block_room_num,
         max(room) as block_room_max,
         min(room) as block_room_min
  from $hotel_ssid_room_block
  where day='$day'
  group by name,poi_lat,poi_lon,ssid_tag,floor,block_room
) t1
where block_room_num>=3
group by name,poi_lat,poi_lon,ssid_tag;

--从房间号block的角度，过滤掉不符合房间号范围的数据
insert overwrite table $hotel_ssid_floor_room_info_filter_by_room_block partition(day='$day')
select a.name,a.poi_lat,a.poi_lon,a.ssid_tag,floor,a.room,b.room_max
from
(
  select name,poi_lat,poi_lon,ssid_tag,floor,room
  from $hotel_ssid_floor_room_info
  where day='$day'
  group by name,poi_lat,poi_lon,ssid_tag,floor,room
)a
left join
$hotel_ssid_room_block_room_scope b
on b.day='$day' and a.name=b.name and a.poi_lat=b.poi_lat and a.poi_lon=b.poi_lon and a.ssid_tag=b.ssid_tag
where a.room<=b.room_max;

--下面再查看楼层连续性，确定真实楼层
--对楼层进行分block（楼层号相差5以内认为是一个block）
insert overwrite table $hotel_ssid_floor_block partition(day='$day')
select name,poi_lat,poi_lon,ssid_tag,floor,room_num,
       sum(flag_floor_diff) over (partition by name,poi_lat,poi_lon,ssid_tag order by floor) as block_floor
from
(
  select name,poi_lat,poi_lon,ssid_tag,floor,room_num,
         case when floor-floor_previous>5 then 1 else 0 end as flag_floor_diff
  from
  (
    select name,poi_lat,poi_lon,ssid_tag,floor,room_num,
           lag(floor,1) over(partition by name,poi_lat,poi_lon,ssid_tag order by floor) as floor_previous
    from
    (
      select name,poi_lat,poi_lon,ssid_tag,floor,count(1) as room_num
      from $hotel_ssid_floor_room_info_filter_by_room_block
      where day='$day'
      group by name,poi_lat,poi_lon,ssid_tag,floor
    )a
  )b
)c;

--每个楼层block的楼层数与房间数、平均楼层房间数
insert overwrite table $hotel_ssid_floor_block_count_info partition(day='$day')
select name,poi_lat,poi_lon,ssid_tag,block_floor,
       count(1) as block_floor_num,
       sum(room_num) as block_floor_room_num,
       round(sum(room_num)/count(1),1) as block_floor_room_num_avg
from $hotel_ssid_floor_block
where day='$day'
group by name,poi_lat,poi_lon,ssid_tag,block_floor;

--计算一个酒店每个楼层block的房间数/该酒店楼层block最大的房间数=每个楼层block房间数比例
--计算一个酒店每个楼层block的平均楼层房间数/该酒店楼层block最大平均楼层房间数=每个楼层block平均楼层房间数比例
insert overwrite table $hotel_ssid_floor_block_count_rate_info partition(day='$day')
select a.name,a.poi_lat,a.poi_lon,ssid_tag,block_floor,block_floor_num,a.block_floor_room_num,a.block_floor_room_num_avg,
       b.block_floor_room_num_max,b.block_floor_room_num_avg_max,
       round(a.block_floor_room_num/b.block_floor_room_num_max,3) as room_num_rate,
       round(a.block_floor_room_num_avg/b.block_floor_room_num_avg_max,3) as room_num_avg_rate
from $hotel_ssid_floor_block_count_info a
inner join
(
  select name,poi_lat,poi_lon,
         max(block_floor_room_num) as block_floor_room_num_max,
         max(block_floor_room_num_avg) as block_floor_room_num_avg_max
  from $hotel_ssid_floor_block_count_info
  where day='$day'
  group by name,poi_lat,poi_lon
)b on a.name=b.name and a.poi_lat=b.poi_lat and a.poi_lon=b.poi_lon
where a.day='$day';

--对每个酒店的楼层block的房间数、平均楼层房间数进行排序（如果数据相同，排名相同）
--取楼层block的房间数排名前2并且楼层block房间数比例>=0.7，
--或者楼层block的平均楼层房间数排名前2并且楼层block平均楼层房间数比例>=0.7的数据作为该酒店的真实楼层block
insert overwrite table $hotel_ssid_floor_block_rate_rank_filter partition(day='$day')
select name,poi_lat,poi_lon,ssid_tag,block_floor,block_floor_num,block_floor_room_num,block_floor_room_num_avg,
       block_floor_room_num_max,block_floor_room_num_avg_max,room_num_rate,room_num_avg_rate,rn1,rn2
from
(
  select name,poi_lat,poi_lon,ssid_tag,block_floor,block_floor_num,block_floor_room_num,block_floor_room_num_avg,
         block_floor_room_num_max,block_floor_room_num_avg_max,room_num_rate,room_num_avg_rate,
         rank() over(partition by name,poi_lat,poi_lon order by block_floor_room_num desc) rn1,
         rank() over(partition by name,poi_lat,poi_lon order by block_floor_room_num_avg desc) rn2
  from $hotel_ssid_floor_block_count_rate_info
  where day='$day'
) t1
where (rn1<=2 and room_num_avg_rate>=0.7)
or (rn2<=2 and room_num_rate>=0.7);

--对hotel_ssid_floor_block，使用join筛选保留真实的楼层block，并匹配上筛选后的最大房间号
insert overwrite table $hotel_ssid_floor_block_filter partition(day='$day')
select a.name,a.poi_lat,a.poi_lon,a.ssid_tag,a.floor,a.block_floor,c.room_max
from $hotel_ssid_floor_block a
inner join
$hotel_ssid_floor_block_rate_rank_filter b
on b.day='$day' and a.name=b.name and a.poi_lat=b.poi_lat and a.poi_lon=b.poi_lon and a.ssid_tag=b.ssid_tag and a.block_floor=b.block_floor
inner join
$hotel_ssid_floor_room_info_filter_by_room_block c
on c.day='$day' and a.name=c.name and a.poi_lat=c.poi_lat and a.poi_lon=c.poi_lon and a.ssid_tag=c.ssid_tag and a.floor=c.floor
where a.day='$day'
group by a.name,a.poi_lat,a.poi_lon,a.ssid_tag,a.floor,a.block_floor,c.room_max;

--最后再做一次房间号的范围筛选，得到房间号匹配的最终结果
insert overwrite table $hotel_ssid_floor_room_match partition(day='$day')
select a.name,a.poi_lat,a.poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
       ssid_info,a.ssid_tag,room_info,a.floor,a.room,
       b.block_floor,b.room_max
from $hotel_ssid_floor_room_info a
inner join
$hotel_ssid_floor_block_filter b
on b.day='$day' and a.name=b.name and a.poi_lat=b.poi_lat and a.poi_lon=b.poi_lon and a.ssid_tag=b.ssid_tag and a.floor=b.floor
where a.day='$day'
and a.room<=b.room_max;
"

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
--计算剩下的酒店（由于相邻酒店的存在,之前匹配上的ssid也要剔除）
insert overwrite table $hotel_bssid_remain_1 partition(day='$day')
select name,poi_lat,poi_lon,city_code,d.ssid,device_num,connect_num,active_days,bssid_num
from
(
  select a.name,a.poi_lat,a.poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
  from $hotel_ssid_calculate_base_info a
  left join
  (
    select name,poi_lat,poi_lon
    from
    (
  	  select name,poi_lat,poi_lon
  	  from $hotel_ssid_match_merge_all_conditions
  	  where day='$day'

  	  union all

  	  select name,poi_lat,poi_lon
  	  from $hotel_ssid_floor_room_match
  	  where day='$day'
  	) t1
  	group by name,poi_lat,poi_lon
  ) b on a.name=b.name and a.poi_lat=b.poi_lat and a.poi_lon=b.poi_lon
  where a.day='$day'
  and b.name is null
)d
left join
(
  select ssid
  from
  (
    select ssid
    from $hotel_ssid_match_merge_all_conditions
    where day='$day'

    union all

  	select ssid
  	from $hotel_ssid_floor_room_match
  	where day='$day'
  ) t2
  group by ssid
) c on d.ssid=c.ssid
where c.ssid is null;

--剩下的酒店
--取出设备数>=50，连接数>=100，活跃天数>=45天的数据
--取出rank前三的ssid，排序标准：设备数，活跃天数
--得到置信度第二高的ssid数据
insert overwrite table $hotel_ssid_match_second_confidence partition(day='$day')
select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,rank
from
(
  select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
         row_number() over (partition by name order by device_num desc,active_days desc) as rank
  from $hotel_bssid_remain_1
  where day='$day'
  and device_num>=50
  and connect_num>=100
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
--继续计算剩下的酒店（由于相邻酒店的存在,之前匹配上的ssid也要剔除）
insert overwrite table $hotel_bssid_remain_2 partition(day='$day')
select name,poi_lat,poi_lon,city_code,d.ssid,device_num,connect_num,active_days,bssid_num
from
(
  select a.name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
  from $hotel_bssid_remain_1 a
  left join
  (
  	select name
  	from $hotel_ssid_match_second_confidence
  	where day='$day'
  	group by name
  ) b on a.name=b.name
  where a.day='$day'
  and b.name is null
)d
left join
(
  select ssid
  from $hotel_ssid_match_second_confidence
  where day='$day'
  group by ssid
) c on d.ssid=c.ssid
where c.ssid is null;

--继续计算剩下的酒店
--取出rank前三的ssid，排序标准：是否满足bssid数>=2 设备数>=10 活跃天数>=30，设备数，活跃天数
--得到置信度第三的ssid数据
insert overwrite table $hotel_ssid_match_third_confidence partition(day='$day')
select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,flag_active,rank
from
(
  select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,flag_active,
         row_number() over (partition by name order by flag_active desc,active_days desc,device_num desc) as rank
  from
  (
  	select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
           case
             when bssid_num>=2 and device_num>=10 and active_days>=30 then 1
             else 0
           end as flag_active
    from $hotel_bssid_remain_2
    where day='$day'
  ) a
) b
where rank<=3;
"

bssidMappingLastParStr=`hive -e "show partitions $dim_mapping_bssid_location_mf" | sort| tail -n 1`
#先匹配所有酒店的bssid
#在已算出的ssid的酒店中，匹配上bssid
#然后计算哪些bssid、ssid出现在多个不同的酒店中
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
insert overwrite table $hotel_poi_and_bssid_info partition(day='$day')
select name,poi_lat,poi_lon,a.geohash7_adjacent,b.bssid,b.ssid
from
(
  select name,poi_lat,poi_lon,geohash7_adjacent
  from
  (
  	select name,poi_lat,poi_lon,
  	       get_geohash_adjacent(geohash7) as geohash7_n
    from $hotel_poi_info
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
    from $hotel_ssid_match_merge_all_conditions
    where day='$day'
    union all
    select name,ssid,poi_lat,poi_lon
  	from $hotel_ssid_floor_room_match
  	where day='$day'
    union all
    select name,ssid,poi_lat,poi_lon
    from $hotel_ssid_match_second_confidence
    where day='$day'
    union all
    select name,ssid,poi_lat,poi_lon
    from $hotel_ssid_match_third_confidence
    where day='$day'
  ) result
  inner join
  $hotel_poi_and_bssid_info b
  on b.day='$day' and result.name=b.name and result.poi_lat=b.poi_lat and result.poi_lon=b.poi_lon and result.ssid=b.ssid
)
insert overwrite table $one_bssid_ssid_with_multiple_hotel_info partition(day='$day')
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
insert overwrite table $dim_hotel_ssid_bssid_match_info_mf partition(day='$day')
select t1.name,t1.ssid,t1.poi_lat,t1.poi_lon,city_code,device_num,connect_num,active_days,bssid_num,
       round(if(t3.name is not null,confidence-0.1,confidence),1) as confidence,
       t2.bssid_array,
       t4.rank_star,t4.score_type,t4.price_level
from
(
  select name,ssid,poi_lat,poi_lon,city_code,device_num,connect_num,active_days,bssid_num,max(confidence) as confidence
  from
  (
    select name,ssid,poi_lat,poi_lon,city_code,device_num,connect_num,active_days,bssid_num,1.0 as confidence
    from $hotel_ssid_match_merge_all_conditions
    where day='$day'
    union all
    select name,ssid,poi_lat,poi_lon,city_code,device_num,connect_num,active_days,bssid_num,0.8 as confidence
    from $hotel_ssid_floor_room_match
    where day='$day'
  ) a
  group by name,ssid,poi_lat,poi_lon,city_code,device_num,connect_num,active_days,bssid_num
  union all
  select name,ssid,poi_lat,poi_lon,city_code,device_num,connect_num,active_days,bssid_num,0.6-(rank-1)/10 as confidence
  from $hotel_ssid_match_second_confidence
  where day='$day'
  union all
  select name,ssid,poi_lat,poi_lon,city_code,device_num,connect_num,active_days,bssid_num,0.3-(rank-1)/10 as confidence
  from $hotel_ssid_match_third_confidence
  where day='$day'
) t1
left join
(
  select name,poi_lat,poi_lon,ssid,collect_list(bssid) as bssid_array
  from $hotel_poi_and_bssid_info
  where day='$day'
  group by name,poi_lat,poi_lon,ssid
) t2 on t1.name=t2.name and t1.poi_lat=t2.poi_lat and t1.poi_lon=t2.poi_lon and t1.ssid=t2.ssid
left join
$one_bssid_ssid_with_multiple_hotel_info t3
on t3.day='$day' and t1.name=t3.name and t1.poi_lat=t3.poi_lat and t1.poi_lon=t3.poi_lon and t1.ssid=t3.ssid
left join
(
  select name,poi_lat,poi_lon,rank_star,score_type,price_level
  from
  (
    select trim(name) as name,
           lat as poi_lat,
           lon as poi_lon,
           get_json_object(attribute,'$.rank_star') as rank_star,
           get_json_object(attribute,'$.score_type') as score_type,
           get_json_object(attribute,'$.price_level') as price_level
    from $poi_config_mapping_par
    where type=6
    and version='1000'
  ) t
  group by name,poi_lat,poi_lon,rank_star,score_type,price_level
) t4 on t1.name=t4.name and t1.poi_lat=t4.poi_lat and t1.poi_lon=t4.poi_lon;
"
