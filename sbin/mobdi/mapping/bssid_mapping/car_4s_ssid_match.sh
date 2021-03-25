#!/bin/bash
: '
@owner:liuyanqiang
@describe:4s店poi ssid匹配
@projectName:mobdi
'

set -e -x

day=$1
#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#源表
#poi_config_mapping_par=dim_sdk_mapping.poi_config_mapping_par

#tmp
car_4s_poi_info=${dw_mobdi_tmp}.car_4s_poi_info
car_4s_poi_and_bssid_connect_info=${dw_mobdi_tmp}.car_4s_poi_and_bssid_connect_info
ssid_match_data_prepare=${dw_mobdi_tmp}.ssid_match_data_prepare
car_4s_poi_and_ssid_connect_count_info=${dw_mobdi_tmp}.car_4s_poi_and_ssid_connect_count_info
car_4s_ssid_calculate_base_info=${dw_mobdi_tmp}.car_4s_ssid_calculate_base_info
car_4s_name_match_ssid_info=${dw_mobdi_tmp}.car_4s_name_match_ssid_info
car_4s_ssid_cn_match=${dw_mobdi_tmp}.car_4s_ssid_cn_match
city_name_combine_area_name=${dw_mobdi_tmp}.city_name_combine_area_name
car_4s_ssid_name_en_match=${dw_mobdi_tmp}.car_4s_ssid_name_en_match
car_4s_ssid_famous_en_match=${dw_mobdi_tmp}.car_4s_ssid_famous_en_match
car_4s_ssid_rude_match=${dw_mobdi_tmp}.car_4s_ssid_rude_match
car_4s_ssid_match_merge_all_conditions=${dw_mobdi_tmp}.car_4s_ssid_match_merge_all_conditions
car_4s_split_ssid_pinyin_match=${dw_mobdi_tmp}.car_4s_split_ssid_pinyin_match
car_4s_split_ssid_pinyin_short_match=${dw_mobdi_tmp}.car_4s_split_ssid_pinyin_short_match
car_4s_ssid_pinyin_match=${dw_mobdi_tmp}.car_4s_ssid_pinyin_match
car_4s_ssid_pinyin_short_match=${dw_mobdi_tmp}.car_4s_ssid_pinyin_short_match
car_4s_bssid_remain_1=${dw_mobdi_tmp}.car_4s_bssid_remain_1
car_4s_ssid_match_second_confidence=${dw_mobdi_tmp}.car_4s_ssid_match_second_confidence
car_4s_bssid_remain_2=${dw_mobdi_tmp}.car_4s_bssid_remain_2
car_4s_ssid_match_third_confidence=${dw_mobdi_tmp}.car_4s_ssid_match_third_confidence
car_4s_poi_and_bssid_info=${dw_mobdi_tmp}.car_4s_poi_and_bssid_info
one_bssid_ssid_with_multiple_car_4s_info=${dw_mobdi_tmp}.one_bssid_ssid_with_multiple_car_4s_info

#mapping表
#dim_mapping_bssid_location_mf=dim_mobdi_mapping.dim_mapping_bssid_location_mf

#目标表
#dim_car_4s_ssid_bssid_match_info_mf=dim_mobdi_mapping.dim_car_4s_ssid_bssid_match_info_mf


#解析4s店poi表
hive -v -e "
insert overwrite table $car_4s_poi_info partition(day='$day')
select if(instr(name,brand)>0,name,concat(name,'|',brand)) as name,
       poi_lat,poi_lon,geohash7,city_code,brand
from
(
  select trim(name) as name,
         lat as poi_lat,
         lon as poi_lon,
         geohash7,
         city as city_code,
         get_json_object(attribute,'$.brand') as brand
  from $poi_config_mapping_par
  where type=3
  and version='1000'
) t1
group by if(instr(name,brand)>0,name,concat(name,'|',brand)),poi_lat,poi_lon,geohash7,city_code,brand;
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
insert overwrite table $car_4s_poi_and_bssid_connect_info partition(day='$day')
select name,poi_lat,poi_lon,city_code,brand,b.bssid,b.ssid,b.device,b.appear_day,b.connect_num
from
(
  select name,poi_lat,poi_lon,city_code,brand,geohash7_adjacent
  from
  (
  	select name,poi_lat,poi_lon,city_code,brand,
  	       get_geohash_adjacent(geohash7) as geohash7_n
    from $car_4s_poi_info
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
--计算4s店poi的ssid连接次数、连接设备数、活跃天数、bssid数
insert overwrite table $car_4s_poi_and_ssid_connect_count_info partition(day='$day')
select t1.name,t1.poi_lat,t1.poi_lon,city_code,brand,t1.ssid,device_num,connect_num,active_days,bssid_num
from
(
  select name,poi_lat,poi_lon,city_code,brand,ssid,
         count(1) as device_num,sum(connect_num) connect_num
  from
  (
    select name,poi_lat,poi_lon,city_code,brand,ssid,device,sum(connect_num) as connect_num
    from $car_4s_poi_and_bssid_connect_info
    where day='$day'
    group by name,poi_lat,poi_lon,city_code,brand,ssid,device
  ) a
  group by name,poi_lat,poi_lon,city_code,brand,ssid
) t1
left join
(
  select name,poi_lat,poi_lon,ssid,count(1) as active_days
  from
  (
    select name,poi_lat,poi_lon,ssid,appear_day
    from $car_4s_poi_and_bssid_connect_info
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
    from $car_4s_poi_and_bssid_connect_info
    where day='$day'
    group by name,poi_lat,poi_lon,ssid,bssid
  ) a
  group by name,poi_lat,poi_lon,ssid
) t3 on t1.name=t3.name and t1.poi_lat=t3.poi_lat and t1.poi_lon=t3.poi_lon and t1.ssid=t3.ssid;

--取出所有4s店的ssid
insert overwrite table $car_4s_ssid_calculate_base_info partition(day='$day')
select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,flag
from
(
  select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,
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
  from $car_4s_poi_and_ssid_connect_count_info
  where day='$day'
  and not (regexp_extract(lower(ssid),'(xiaomi[^-_]|huawei[^-_]|honor|vivo|oppo|iphone|samsung|redmi|mi[\\\\s]|小米|手机|华为|三星)',1)!=''
            and regexp_extract(lower(ssid),'(专卖|维修|店)',1)='')
  and split(split(ssid,'-')[0],'_')[0] not in ('','0x','  小米共享WiFi',' 免费安全共享WiFi','360WiFi','360行车记录仪','360免费WiFi')
) a
where flag=0;
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
--计算ssid与4s店name匹配上的字数跟词的名字，按照两个字来匹配
insert overwrite table $car_4s_name_match_ssid_info partition(day='$day')
select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,
       word1,word2,word3,word4,word5,word6,word7,word8,flag1,flag2,flag3,flag4,flag5,flag6,flag7,flag8,match1,match2,match3,match4,match5,match6,match7,match8,
       match_num,regexp_extract(concat_ws('',array(match1,match2,match3,match4,match5,match6,match7,match8)),'([^_]+(.*[^_]+)*)',1) as match_word
from
(
  select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,
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
    select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,ssid_array,
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
      select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,ssid_array,
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
        select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,
               split(regexp_replace(lower(ssid),'([^\\\\u4e00-\\\\u9fa5])',''), '') as ssid_array
        from $car_4s_ssid_calculate_base_info
        where day='$day'
        and regexp_extract(lower(ssid),'([\\\\u4e00-\\\\u9fa5])',1)!=''
      )a
    )b
  )c
)d;

--汉字匹配规则
--需要去掉汽车|4s|维修等关键字，并且剩余的汉字不能是城市地区名
insert overwrite table $car_4s_ssid_cn_match partition(day='$day')
select name,poi_lat,poi_lon,a.city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
from
(
  select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,match_num,match_word,
         regexp_replace(match_word,'汽车|4s|维修|_','') as match_word_new
  from $car_4s_name_match_ssid_info
  where day='$day'
) a
left join
$city_name_combine_area_name b on b.day='$day' and a.city_code=b.city_code
where instr(b.area_list,match_word_new)=0
and (match_num>=4
  or (length(match_word_new)=3 and bssid_num>=2 and device_num>=30 and connect_num>=130)
  or match_word_new in (
    '大众','丰田','本田','众泰','吉利','日产','别克','长安','现代','福特','五菱','传祺','哈弗','荣威','奥迪','雪佛兰','宝马',
    '比亚迪','奔驰','马自达','起亚','三菱','奇瑞','斯柯达','风行','铃木','海马','标致','江淮','猎豹','欧尚','北汽','幻速',
    '名爵','一汽','雪铁龙','路虎','凯迪拉克','启辰','jeep','奔腾','陆风','驭胜','沃尔沃','小康','汉腾','风神','雷诺','中华',
    '力帆','wey','雷克萨斯','金杯','北京','威旺','福田','swm','斯威','上汽','大通','宝沃','风光','克莱斯勒','斯巴鲁','比速',
    '保时捷','林肯','英菲尼迪','观致','野马','领克','开瑞','绅宝','smart','玛莎拉蒂','凯翼','mini','五十铃','纳智捷','东南',
    '君马','讴歌','江铃','东风','潍柴','英致','依维柯','新能源','宾利','昌河','福迪','红旗','长城','华泰','制造','宝骏','捷豹',
    '法拉利','劳斯莱斯','迈凯伦','特斯拉','兰博基尼')
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
--英文字母推断
--筛选ssid名字有字母的数据
--酒店名称本身就为英文的情况(cc mall -> cc mall,ccmall)
insert overwrite table $car_4s_ssid_name_en_match partition(day='$day')
select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,
       name_en1,name_en2,ssid_en1,ssid_en2
from
(
  select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,
         regexp_extract(lower(name),'([a-z].*[a-z])',1) as name_en1,
         regexp_replace(lower(name),'[^a-z]','') as name_en2,
         lower(ssid) as ssid_en1,
         regexp_extract(lower(ssid),'([a-z]+)',1) as ssid_en2
  from $car_4s_ssid_calculate_base_info
  where day='$day'
  and regexp_extract(lower(ssid),'([a-z])',1)!=''
)a
where (length(name_en1)>=3 and instr(ssid_en1,name_en1)!=0)
or (length(ssid_en2)>=3 and instr(name_en1,ssid_en2)!=0)
or (length(name_en2)>=3 and instr(ssid_en1,name_en2)!=0)
or (length(ssid_en2)>=3 and instr(name_en2,ssid_en2)!=0);

--大的连锁品牌4s店查看英文名
insert overwrite table $car_4s_ssid_famous_en_match partition(day='$day')
select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
from $car_4s_ssid_calculate_base_info
where day='$day'
and (
  (regexp_extract(lower(name),'(大众)',1)!='' and regexp_extract(lower(ssid),'(volks|wagen)',1)!='')
  or (regexp_extract(lower(name),'(丰田)',1)!='' and instr(lower(ssid),'toyota')>0)
  or (regexp_extract(lower(name),'(本田)',1)!='' and instr(lower(ssid),'honda')>0)
  or (regexp_extract(lower(name),'(众泰)',1)!='' and instr(lower(ssid),'zotye')>0)
  or (regexp_extract(lower(name),'(吉利)',1)!='' and instr(lower(ssid),'geely')>0)
  or (regexp_extract(lower(name),'(日产)',1)!='' and instr(lower(ssid),'nissan')>0)
  or (regexp_extract(lower(name),'(别克)',1)!='' and instr(lower(ssid),'buick')>0)
  or (regexp_extract(lower(name),'(长安)',1)!='' and instr(lower(ssid),'changan')>0)
  or (regexp_extract(lower(name),'(现代)',1)!='' and instr(lower(ssid),'hyundai')>0)
  or (regexp_extract(lower(name),'(福特)',1)!='' and instr(lower(ssid),'ford')>0)
  or (regexp_extract(lower(name),'(五菱)',1)!='' and instr(lower(ssid),'sgmw')>0)
  or (regexp_extract(lower(name),'(传祺)',1)!='' and regexp_extract(lower(ssid),'(gacmotor|trumpchi)',1)!='')
  or (regexp_extract(lower(name),'(哈弗)',1)!='' and instr(lower(ssid),'haval')>0)
  or (regexp_extract(lower(name),'(荣威)',1)!='' and instr(lower(ssid),'roewe')>0)
  or (regexp_extract(lower(name),'(奥迪)',1)!='' and instr(lower(ssid),'audi')>0)
  or (regexp_extract(lower(name),'(雪佛兰)',1)!='' and instr(lower(ssid),'chevrolet')>0)
  or (regexp_extract(lower(name),'(宝马)',1)!='' and regexp_extract(lower(ssid),'([^a-z]|^)(bmw)([^a-z]|$)',2)!='')
  or (regexp_extract(lower(name),'(比亚迪)',1)!='' and regexp_extract(lower(ssid),'([^a-z]|^)(byd)([^a-z]|$)',2)!='')
  or (regexp_extract(lower(name),'(奔驰)',1)!='' and regexp_extract(lower(ssid),'(benz|mercedes)',1)!='')
  or (regexp_extract(lower(name),'(马自达)',1)!='' and instr(lower(ssid),'mazda')>0)
  or (regexp_extract(lower(name),'(起亚)',1)!='' and regexp_extract(lower(ssid),'([^a-z]|^)(kia)([^a-z]|$)',2)!='')
  or (regexp_extract(lower(name),'(三菱)',1)!='' and instr(lower(ssid),'mitsubishi')>0)
  or (regexp_extract(lower(name),'(奇瑞)',1)!='' and instr(lower(ssid),'chery')>0)
  or (regexp_extract(lower(name),'(斯柯达)',1)!='' and instr(lower(ssid),'skoda')>0)
  or (regexp_extract(lower(name),'(风行)',1)!='' and instr(lower(ssid),'fxauto')>0)
  or (regexp_extract(lower(name),'(铃木)',1)!='' and instr(lower(ssid),'suzuki')>0)
  or (regexp_extract(lower(name),'(海马)',1)!='' and instr(lower(ssid),'haima')>0)
  or (regexp_extract(lower(name),'(标致)',1)!='' and instr(lower(ssid),'peugeot')>0)
  or (regexp_extract(lower(name),'(江淮)',1)!='' and regexp_extract(lower(ssid),'([^a-z]|^)(jac)([^a-z]|$)',2)!='')
  or (regexp_extract(lower(name),'(猎豹)',1)!='' and instr(lower(ssid),'leopaard')>0)
  or (regexp_extract(lower(name),'(欧尚)',1)!='' and instr(lower(ssid),'oshan')>0)
  or (regexp_extract(lower(name),'(北汽|绅宝)',1)!='' and regexp_extract(lower(ssid),'([^a-z]|^)(baic)([^a-z]|$)',2)!='')
  or (regexp_extract(lower(name),'(名爵)',1)!='' and regexp_extract(lower(ssid),'(morris|garages)',1)!='')
  or (regexp_extract(lower(name),'(一汽)',1)!='' and regexp_extract(lower(ssid),'([^a-z]|^)(faw)([^a-z]|$)',2)!='')
  or (regexp_extract(lower(name),'(雪铁龙)',1)!='' and instr(lower(ssid),'citroen')>0)
  or (regexp_extract(lower(name),'(路虎)',1)!='' and instr(lower(ssid),'landrover')>0)
  or (regexp_extract(lower(name),'(凯迪拉克)',1)!='' and instr(lower(ssid),'cadillac')>0)
  or (regexp_extract(lower(name),'(启辰)',1)!='' and instr(lower(ssid),'venucia')>0)
  or (regexp_extract(lower(name),'(jeep)',1)!='' and instr(lower(ssid),'jeep')>0)
  or (regexp_extract(lower(name),'(奔腾)',1)!='' and instr(lower(ssid),'bestune')>0)
  or (regexp_extract(lower(name),'(陆风)',1)!='' and instr(lower(ssid),'landwind')>0)
  or (regexp_extract(lower(name),'(江铃|驭胜)',1)!='' and regexp_extract(lower(ssid),'([^a-z]|^)(jmc)([^a-z]|$)',2)!='')
  or (regexp_extract(lower(name),'(沃尔沃)',1)!='' and instr(lower(ssid),'volvo')>0)
  or (regexp_extract(lower(name),'(风神)',1)!='' and regexp_extract(lower(ssid),'(dfpv|aeolus)',1)!='')
  or (regexp_extract(lower(name),'(雷诺)',1)!='' and instr(lower(ssid),'renault')>0)
  or (regexp_extract(lower(name),'(力帆)',1)!='' and instr(lower(ssid),'lifan')>0)
  or (regexp_extract(lower(name),'(魏派|wey)',1)!='' and regexp_extract(lower(ssid),'([^a-z]|^)(wey)([^a-z]|$)',2)!='')
  or (regexp_extract(lower(name),'(雷克萨斯)',1)!='' and instr(lower(ssid),'lexus')>0)
  or (regexp_extract(lower(name),'(金杯)',1)!='' and instr(lower(ssid),'jinbei')>0)
  or (regexp_extract(lower(name),'(福田)',1)!='' and instr(lower(ssid),'foton')>0)
  or (regexp_extract(lower(name),'(swm|斯威)',1)!='' and regexp_extract(lower(ssid),'([^a-z]|^)(swm)([^a-z]|$)',2)!='')
  or (regexp_extract(lower(name),'(上汽)',1)!='' and regexp_extract(lower(ssid),'([^a-z]|^)(saic)([^a-z]|$)',2)!='')
  or (regexp_extract(lower(name),'(大通)',1)!='' and instr(lower(ssid),'maxus')>0)
  or (regexp_extract(lower(name),'(宝沃)',1)!='' and instr(lower(ssid),'borgward')>0)
  or (regexp_extract(lower(name),'(克莱斯勒)',1)!='' and instr(lower(ssid),'chrysler')>0)
  or (regexp_extract(lower(name),'(斯巴鲁)',1)!='' and instr(lower(ssid),'subaru')>0)
  or (regexp_extract(lower(name),'(比速)',1)!='' and instr(lower(ssid),'bisu')>0)
  or (regexp_extract(lower(name),'(保时捷)',1)!='' and instr(lower(ssid),'porsche')>0)
  or (regexp_extract(lower(name),'(林肯)',1)!='' and instr(lower(ssid),'lincoln')>0)
  or (regexp_extract(lower(name),'(英菲尼迪)',1)!='' and instr(lower(ssid),'infiniti')>0)
  or (regexp_extract(lower(name),'(观致)',1)!='' and instr(lower(ssid),'qoros')>0)
  or (regexp_extract(lower(name),'(野马)',1)!='' and instr(lower(ssid),'yema')>0)
  or (regexp_extract(lower(name),'(领克)',1)!='' and regexp_extract(lower(ssid),'(lynk.?co)',1)!='')
  or (regexp_extract(lower(name),'(开瑞)',1)!='' and instr(lower(ssid),'karry')>0)
  or (regexp_extract(lower(name),'(smart)',1)!='' and instr(lower(ssid),'smart')>0)
  or (regexp_extract(lower(name),'(玛莎拉蒂)',1)!='' and instr(lower(ssid),'maserati')>0)
  or (regexp_extract(lower(name),'(凯翼)',1)!='' and instr(lower(ssid),'cowin')>0)
  or (regexp_extract(lower(name),'(mini)',1)!='' and instr(lower(ssid),'mini')>0)
  or (regexp_extract(lower(name),'(五十铃)',1)!='' and instr(lower(ssid),'isuzu')>0)
  or (regexp_extract(lower(name),'(纳智捷)',1)!='' and instr(lower(ssid),'luxgen')>0)
  or (regexp_extract(lower(name),'(东南)',1)!='' and instr(lower(ssid),'soueast')>0)
  or (regexp_extract(lower(name),'(ds)',1)!='' and regexp_extract(lower(ssid),'([^a-z]|^)(ds)([^a-z]|$)',2)!='')
  or (regexp_extract(lower(name),'(君马)',1)!='' and instr(lower(ssid),'traum')>0)
  or (regexp_extract(lower(name),'(讴歌)',1)!='' and instr(lower(ssid),'acura')>0)
  or (regexp_extract(lower(name),'(东风)',1)!='' and instr(lower(ssid),'dfmc')>0)
  or (regexp_extract(lower(name),'(潍柴)',1)!='' and instr(lower(ssid),'weichai')>0)
  or (regexp_extract(lower(name),'(依维柯)',1)!='' and regexp_extract(lower(ssid),'(iveco|naveco)',1)!='')
  or (regexp_extract(lower(name),'(宾利)',1)!='' and instr(lower(ssid),'bentley')>0)
  or (regexp_extract(lower(name),'(昌河)',1)!='' and instr(lower(ssid),'changhe')>0)
  or (regexp_extract(lower(name),'(红旗)',1)!='' and instr(lower(ssid),'hongqi')>0)
  or (regexp_extract(lower(name),'(长城)',1)!='' and regexp_extract(lower(ssid),'([^a-z]|^)(gwm)([^a-z]|$)',2)!='')
  or (regexp_extract(lower(name),'(宝骏)',1)!='' and instr(lower(ssid),'baojun')>0)
  or (regexp_extract(lower(name),'(捷豹)',1)!='' and instr(lower(ssid),'jaguar')>0)
  or (regexp_extract(lower(name),'(法拉利)',1)!='' and instr(lower(ssid),'ferrari')>0)
  or (regexp_extract(lower(name),'(劳斯莱斯)',1)!='' and regexp_extract(lower(ssid),'(rolls|royce)',1)!='')
  or (regexp_extract(lower(name),'(迈凯伦)',1)!='' and instr(lower(ssid),'mclaren')>0)
  or (regexp_extract(lower(name),'(特斯拉)',1)!='' and instr(lower(ssid),'tesla')>0)
  or (regexp_extract(lower(name),'(兰博基尼)',1)!='' and instr(lower(ssid),'lamborghini')>0)
);
"

#拼音推断
spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.bssidmapping.Car4sPinyinMatch \
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
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT.jar "$day" "${dw_mobdi_tmp}"

#考虑所有的组合情况，看是否匹配
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $car_4s_ssid_rude_match partition(day='$day')
select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
from $car_4s_ssid_calculate_base_info
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
insert overwrite table $car_4s_ssid_match_merge_all_conditions partition(day='$day')
select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
from
(
    select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
    from $car_4s_ssid_cn_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
    from $car_4s_ssid_name_en_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
    from $car_4s_ssid_famous_en_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
    from $car_4s_split_ssid_pinyin_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
    from $car_4s_split_ssid_pinyin_short_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
    from $car_4s_ssid_pinyin_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
    from $car_4s_ssid_pinyin_short_match
    where day='$day'
    union all
    select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
    from $car_4s_ssid_rude_match
    where day='$day'
)a
group by name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num;
"

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
--计算剩下的4s店（由于相邻4s店的存在,之前匹配上的ssid也要剔除）
insert overwrite table $car_4s_bssid_remain_1 partition(day='$day')
select name,poi_lat,poi_lon,city_code,brand,d.ssid,device_num,connect_num,active_days,bssid_num
from
(
  select a.name,a.poi_lat,a.poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
  from $car_4s_ssid_calculate_base_info a
  left join
  (
    select name
  	from $car_4s_ssid_match_merge_all_conditions
  	where day='$day'
  	group by name
  ) b on a.name=b.name
  where a.day='$day'
  and b.name is null
)d
left join
(
  select ssid
  from $car_4s_ssid_match_merge_all_conditions
  where day='$day'
  group by ssid
) c on d.ssid=c.ssid
where c.ssid is null;

--剩下的4s店
--取出设备数>=10，连接数>=30，活跃天数>=45天的数据
--取出rank前三的ssid，排序标准：设备数，活跃天数
--得到置信度第二高的ssid数据
insert overwrite table $car_4s_ssid_match_second_confidence partition(day='$day')
select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,rank
from
(
  select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,
         row_number() over (partition by name order by device_num desc,active_days desc) as rank
  from $car_4s_bssid_remain_1
  where day='$day'
  and device_num>=10
  and connect_num>=30
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
--继续计算剩下的4s店（由于相邻4s店的存在,之前匹配上的ssid也要剔除）
insert overwrite table $car_4s_bssid_remain_2 partition(day='$day')
select name,poi_lat,poi_lon,city_code,brand,d.ssid,device_num,connect_num,active_days,bssid_num
from
(
  select a.name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num
  from $car_4s_bssid_remain_1 a
  left join
  (
  	select name
  	from $car_4s_ssid_match_second_confidence
  	where day='$day'
  	group by name
  ) b on a.name=b.name
  where a.day='$day'
  and b.name is null
)d
left join
(
  select ssid
  from $car_4s_ssid_match_second_confidence
  where day='$day'
  group by ssid
) c on d.ssid=c.ssid
where c.ssid is null;

--继续计算剩下的4s店
--取出rank前三的ssid，排序标准：是否满足bssid数>=2 设备数>=10 活跃天数>=30，设备数，活跃天数
--得到置信度第三的ssid数据
insert overwrite table $car_4s_ssid_match_third_confidence partition(day='$day')
select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,flag_active,rank
from
(
  select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,flag_active,
         row_number() over (partition by name order by flag_active desc,active_days desc,device_num desc) as rank
  from
  (
  	select name,poi_lat,poi_lon,city_code,brand,ssid,device_num,connect_num,active_days,bssid_num,
           case
             when bssid_num>=2 and device_num>=10 and active_days>=30 then 1
             else 0
           end as flag_active
    from $car_4s_bssid_remain_2
    where day='$day'
  ) a
) b
where rank<=3;
"

bssidMappingLastParStr=`hive -e "show partitions $dim_mapping_bssid_location_mf" | sort| tail -n 1`
#先匹配所有4s店的bssid
#在已算出的ssid的4s店中，匹配上bssid
#然后计算哪些bssid、ssid出现在多个不同的4s店中
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
insert overwrite table $car_4s_poi_and_bssid_info partition(day='$day')
select name,poi_lat,poi_lon,a.geohash7_adjacent,b.bssid,b.ssid
from
(
  select name,poi_lat,poi_lon,geohash7_adjacent
  from
  (
  	select name,poi_lat,poi_lon,
  	       get_geohash_adjacent(geohash7) as geohash7_n
    from $car_4s_poi_info
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
    from $car_4s_ssid_match_merge_all_conditions
    where day='$day'
    union all
    select name,ssid,poi_lat,poi_lon
    from $car_4s_ssid_match_second_confidence
    where day='$day'
    union all
    select name,ssid,poi_lat,poi_lon
    from $car_4s_ssid_match_third_confidence
    where day='$day'
  ) result
  inner join
  $car_4s_poi_and_bssid_info b
  on b.day='$day' and result.name=b.name and result.poi_lat=b.poi_lat and result.poi_lon=b.poi_lon and result.ssid=b.ssid
)
insert overwrite table $one_bssid_ssid_with_multiple_car_4s_info partition(day='$day')
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
insert overwrite table $dim_car_4s_ssid_bssid_match_info_mf partition(day='$day')
select t1.name,t1.ssid,t1.poi_lat,t1.poi_lon,city_code,brand,device_num,connect_num,active_days,bssid_num,
       round(if(t3.name is not null,confidence-0.1,confidence),1) as confidence,
       t2.bssid_array
from
(
  select name,ssid,poi_lat,poi_lon,city_code,brand,device_num,connect_num,active_days,bssid_num,1.0 as confidence
  from $car_4s_ssid_match_merge_all_conditions
  where day='$day'
  union all
  select name,ssid,poi_lat,poi_lon,city_code,brand,device_num,connect_num,active_days,bssid_num,0.6-(rank-1)/10 as confidence
  from $car_4s_ssid_match_second_confidence
  where day='$day'
  union all
  select name,ssid,poi_lat,poi_lon,city_code,brand,device_num,connect_num,active_days,bssid_num,0.3-(rank-1)/10 as confidence
  from $car_4s_ssid_match_third_confidence
  where day='$day'
) t1
left join
(
  select name,poi_lat,poi_lon,ssid,collect_list(bssid) as bssid_array
  from $car_4s_poi_and_bssid_info
  where day='$day'
  group by name,poi_lat,poi_lon,ssid
) t2 on t1.name=t2.name and t1.poi_lat=t2.poi_lat and t1.poi_lon=t2.poi_lon and t1.ssid=t2.ssid
left join
$one_bssid_ssid_with_multiple_car_4s_info t3
on t3.day='$day' and t1.name=t3.name and t1.poi_lat=t3.poi_lat and t1.poi_lon=t3.poi_lon and t1.ssid=t3.ssid;
"
