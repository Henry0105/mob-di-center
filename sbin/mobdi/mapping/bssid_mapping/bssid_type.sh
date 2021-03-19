#!/bin/bash
: '
@owner:liuyanqiang
@describe:bssid属性识别,http://c.mob.com/pages/viewpage.action?pageId=19892097
@projectName:mobdi
@BusinessName:BssidMapping
'

set -e -x

day=$1
pday=`date -d "$day -31 days" "+%Y%m%d"`
#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
#input
#dwd_location_info_sec_di=dm_mobdi_master.dwd_location_info_sec_di
#tmp
calculate_bssid_type_base_info_except_abnormal_data_all=dw_mobdi_tmp.calculate_bssid_type_base_info_except_abnormal_data_all
calculate_bssid_type_base_info_except_abnormal_data=dw_mobdi_tmp.calculate_bssid_type_base_info_except_abnormal_data
calculate_bssid_type_base_info_except_abnormal_data_final_info=dw_mobdi_tmp.calculate_bssid_type_base_info_except_abnormal_data_final_info
calculate_bssid_type_base_info=dw_mobdi_tmp.calculate_bssid_type_base_info
speed_abnormal_device_info=dw_mobdi_tmp.speed_abnormal_device_info
speed_abnormal_device_info_2=dw_mobdi_tmp.speed_abnormal_device_info_2
bssid_abnormal_type=dw_mobdi_tmp.bssid_abnormal_type
bssid_mobile_type=dw_mobdi_tmp.bssid_mobile_type
bssid_stable_type=dw_mobdi_tmp.bssid_stable_type
#output
#dim_bssid_type_all_mf=dm_mobdi_mapping.dim_bssid_type_all_mf

hive -v -e "
insert overwrite table $calculate_bssid_type_base_info partition(day='$day')
select muid as deviceid, cur_bssid as bssid, cur_ssid as ssid, latitude, longitude, clienttime
from $dwd_location_info_sec_di
where day > '$pday' and day <='$day'
and length(trim(cur_bssid))=17
and cur_bssid is not null and latitude is not null and longitude is not null
and cur_bssid <> '' and cur_bssid <> '00:00:00:00:00:00' and cur_bssid <> 'null' and cur_bssid <> '02:00:00:00:00:00' and cur_bssid <> '01:80:c2:00:00:03' and cur_bssid <> 'ff:ff:ff:ff:ff:ff' and cur_bssid <> '00:02:00:00:00:00'
and (latitude - round(latitude, 1))*10 <> 0.0 and (longitude - round(longitude, 1))*10 <> 0.0
--以下条件在计算bssid_mapping表时没有
and regexp_replace(cur_bssid, '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
and longitude > 73
and longitude < 136
and latitude > 3
and latitude < 54
group by muid, cur_bssid, cur_ssid, latitude, longitude, clienttime;
"

#12小时内的移动距离>=100m且平均速度>=30m/s，认为是异常数据
hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $speed_abnormal_device_info partition(day='$day')
select deviceid, longitude, latitude, longitude_new, latitude_new, distance, time
from
(
  select deviceid, longitude, latitude, longitude_new, latitude_new,
         cast(get_distance(latitude,longitude,latitude_new,longitude_new) as int) as distance,
         if(clienttime-clienttime_new=0,0.0001,clienttime-clienttime_new) as time
  from
  (
    select deviceid, longitude, latitude, clienttime,
           lag(longitude,1) over(partition by deviceid order by clienttime) as longitude_new,
           lag(latitude,1) over(partition by deviceid order by clienttime) as latitude_new,
           lag(clienttime,1,0) over(partition by deviceid order by clienttime) as clienttime_new
    from $calculate_bssid_type_base_info
    where day='$day'
  ) t1
  where latitude_new is not null
  and longitude_new is not null
  and clienttime-clienttime_new<=43200000
) t2
where cast(nvl(distance/time*1000,0) as bigint)>=30
and distance>=100;

--异常数据出现超过两次以上的认为是缓存数据，需要剔除
insert overwrite table $calculate_bssid_type_base_info_except_abnormal_data partition(day='$day')
select t1.deviceid, bssid, ssid, t1.latitude, t1.longitude, clienttime
from $calculate_bssid_type_base_info t1
left join
(
  select deviceid, longitude, latitude
  from
  (
    select deviceid, longitude, latitude
    from $speed_abnormal_device_info
    where day='$day'

    union all

    select deviceid, longitude_new as longitude, latitude_new as latitude
    from $speed_abnormal_device_info
    where day='$day'
  ) t1
  group by deviceid, longitude, latitude
  having count(1)>=2
) t2 on t1.deviceid=t2.deviceid and t1.latitude=t2.latitude and t1.longitude=t2.longitude
where t1.day='$day'
and t2.deviceid is null;
"

#计算出现超过5次的bssid的活跃天数、设备数、连接数、ssid、ssid数、各个距离的覆盖率
hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $calculate_bssid_type_base_info_except_abnormal_data_final_info partition(day='$day')
select t1.bssid, active_days, device_num, connect_num,
       nvl(t4.ssid,'?') as ssid, nvl(t4.ssid_num,1) as ssid_num,
       round(nvl(sum(if(distance<=50, 1, 0))/count(1),0),4) as rate_50m,
       round(nvl(sum(if(distance<=100, 1, 0))/count(1),0),4) as rate_100m,
       round(nvl(sum(if(distance<=200, 1, 0))/count(1),0),4) as rate_200m,
       round(nvl(sum(if(distance<=500, 1, 0))/count(1),0),4) as rate_500m,
       round(nvl(sum(if(distance<=1000, 1, 0))/count(1),0),4) as rate_1km,
       round(nvl(sum(if(distance<=2000, 1, 0))/count(1),0),4) as rate_2km,
       round(nvl(sum(if(distance<=5000, 1, 0))/count(1),0),4) as rate_5km,
       round(nvl(sum(if(distance<=10000, 1, 0))/count(1),0),4) as rate_10km,
       round(nvl(sum(if(distance<=100000, 1, 0))/count(1),0),4) as rate_100km,
       round(nvl(sum(if(distance<=200000, 1, 0))/count(1),0),4) as rate_200km
from
(
  select bssid, cast(get_distance(latitude,longitude,latitude_avg,longitude_avg) as int) as distance
  from
  (
    select t1.bssid, latitude, longitude, clienttime, longitude_avg, latitude_avg
    from $calculate_bssid_type_base_info_except_abnormal_data t1
    inner join
    (
      select bssid,
             percentile(cast(longitude*100000 as bigint),0.5)/100000 as longitude_avg,
             percentile(cast(latitude*100000 as bigint),0.5)/100000 as latitude_avg
      from $calculate_bssid_type_base_info_except_abnormal_data
      where day='$day'
      group by bssid
      having count(1)>=5
    ) t2 on t1.bssid=t2.bssid
    where t1.day='$day'
  ) t1
) t1
left join
(
  select bssid,count(1) as active_days
  from
  (
    select bssid,from_unixtime(floor(clienttime/1000),'yyyy-MM-dd') as clientdate
    from $calculate_bssid_type_base_info
    where day='$day'
    group by bssid,from_unixtime(floor(clienttime/1000),'yyyy-MM-dd')
  ) t1
  group by bssid
) t2 on t1.bssid=t2.bssid
left join
(
  select bssid,count(1) as device_num,sum(connect_num) connect_num
  from
  (
    select bssid,deviceid,count(1) as connect_num
    from $calculate_bssid_type_base_info
    where day='$day'
    group by bssid,deviceid
  ) t1
  group by bssid
) t3 on t1.bssid=t3.bssid
left join
(
  select bssid,min(ssid) as ssid,count(1) as ssid_num
  from
  (
    select bssid,ssid
    from $calculate_bssid_type_base_info
    where day='$day'
    and instr(ssid,'?')=0
    and instr(ssid,'unknown')=0
    group by bssid,ssid
  ) t1
  group by bssid
) t4 on t1.bssid=t4.bssid
group by t1.bssid, active_days, device_num, connect_num, nvl(t4.ssid,'?'), nvl(t4.ssid_num,1);
"

: '
移动型bssid
条件1：500米覆盖率小于50%、1公里覆盖率小于80%、200公里覆盖率大于等于90%、ssid数量为1、连接设备数为1、活跃天数小于7、连接次数小于30、剔除部分特殊ssid
条件2：ssid内包含有手机、汽车、火车或者移动路由器等信息，剔除ssid内包含华为路由器，小米路由器，维修店，4s店等关键字的bssid
'
hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $bssid_mobile_type partition(day='$day')
select bssid
from
(
  select bssid
  from $calculate_bssid_type_base_info_except_abnormal_data_final_info
  where day='$day'
  and rate_500m<0.5
  and rate_1km<0.8
  and rate_200km>=0.9
  and ssid_num=1
  and device_num=1
  and active_days<7
  and connect_num<30
  and split(split(ssid,'-')[0],'_')[0] not in ('CMCC','ChinaNet','ChinaMobile','i','MERCURY','Xiaomi','FAST','TP')

  union all

  select bssid
  from $calculate_bssid_type_base_info_except_abnormal_data_final_info
  where day='$day'
  and regexp_extract(lower(ssid),'(维修|店)',1)=''
  and (regexp_extract(lower(ssid),'(android|oppo|vivo|redmi|honor|mi |iphone|meizu|samsung|nubia|oneplus|realme|iqoo|nokia|mate|note|nova|phone|手机|小米|华为|苹果|三星|畅享)',1) != ''
  or (regexp_extract(lower(ssid),'(huawei|xiaomi)',1)<>'' and regexp_extract(lower(ssid),'(huawei_|xiaomi_|huawei-|xiaomi-)',1 )=''))

  union all

  select bssid
  from $calculate_bssid_type_base_info_except_abnormal_data_final_info
  where day='$day'
  and regexp_extract(lower(ssid),'(维修|店|行$)',1)=''
  and regexp_extract(lower(ssid),'(bus[^a-z0-9]|^bmw|benz|volks|toyota|^honda|^audi|nissan|buick|cadillac|geely|lexus|haval|volvo|高铁|地铁|导航|行车|车载|公交|巴士|huawei.*b311|huawei.*e8372|huawei.*e557|glocalme|^ikuai|heikuai|es06w|chengcheyi)',1) != ''
) t1
group by bssid;
"

: '
异常型bssid数据准备
条件1可以用上面的数据，下面准备条件2需要用到的数据
区别于calculate_bssid_type_base_info_except_abnormal_data，将异常数据全部排除
'
hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $calculate_bssid_type_base_info_except_abnormal_data_all partition(day='$day')
select t1.deviceid, bssid, ssid, t1.latitude, t1.longitude, clienttime
from $calculate_bssid_type_base_info t1
left join
(
  select deviceid, longitude, latitude
  from
  (
    select deviceid, longitude, latitude
    from $speed_abnormal_device_info
    where day='$day'

    union all

    select deviceid, longitude_new as longitude, latitude_new as latitude
    from $speed_abnormal_device_info
    where day='$day'
  ) t1
  group by deviceid, longitude, latitude
) t2 on t1.deviceid=t2.deviceid and t1.latitude=t2.latitude and t1.longitude=t2.longitude
where t1.day='$day'
and t2.deviceid is null;

--72小时内的平均速度>=100m/s并且移动距离大于10km，认为是异常数据
insert overwrite table $speed_abnormal_device_info_2 partition(day='$day')
select deviceid, bssid, ssid
from
(
  select deviceid, longitude, latitude, clienttime, bssid, ssid,
         cast(get_distance(latitude,longitude,latitude_new,longitude_new) as int) as distance_ahead,
         if(clienttime-clienttime_new=0,0.0001,clienttime-clienttime_new) as time
  from
  (
    select deviceid, longitude, latitude, clienttime, bssid, ssid,
           lag(longitude,1) over(partition by bssid order by clienttime) as longitude_new,
           lag(latitude,1) over(partition by bssid order by clienttime) as latitude_new,
           lag(clienttime,1,0) over(partition by bssid order by clienttime) as clienttime_new
    from $calculate_bssid_type_base_info_except_abnormal_data_all
    where day='$day'
  ) t1
  where latitude_new is not null
  and longitude_new is not null
  and clienttime-clienttime_new<=259200000
) t2
where cast(nvl(distance_ahead/time*1000,0) as bigint)>=100
and distance_ahead>=10000;
"

: '
异常型bssid
条件1：设备数大于等于2、100公里覆盖率小于80%、排除掉移动型bssid、ssid数量为1且500米覆盖率小于50% 或者 ssid数量为2且连接数大于10 或者 ssid数量为3
条件2：计算三天内的移动距离以及移动速度，移动距离超过10公里，移动速度超过100m/s为异常数据，异常数据大于等于4条或者异常数据中的ssid数量大于等于2，排除掉移动型bssid
'
hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $bssid_abnormal_type partition(day='$day')
select t1.bssid
from
(
  select bssid
  from
  (
    select bssid
    from $calculate_bssid_type_base_info_except_abnormal_data_final_info
    where day='$day'
    and device_num>=2
    and rate_100km<0.8
    and ((ssid_num=1 and rate_500m<0.5)
    or (ssid_num=2 and connect_num>10)
    or ssid_num>=3)

    union all

    select bssid
    from
    (
      select bssid,count(1) as ssid_num,sum(num) bssid_num
      from
      (
        select bssid,ssid,count(1) as num
        from $speed_abnormal_device_info_2
        where day='$day'
        group by bssid,ssid
      ) t1
      group by bssid
    ) t2
    where ssid_num>=2 or bssid_num>=4
  ) t1
  group by bssid
) t1
left join
$bssid_mobile_type t2 on t2.day='$day' and t1.bssid=t2.bssid
where t2.bssid is null;
"

: '
稳定型bssid
500米覆盖率大于等于50%、5公里覆盖率大于等于80%、ssid数量为1、排除掉移动型bssid和异常型bssid
'
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $bssid_stable_type partition(day='$day')
select t1.bssid
from $calculate_bssid_type_base_info_except_abnormal_data_final_info t1
left join
$bssid_mobile_type t2 on t2.day='$day' and t1.bssid=t2.bssid
left join
$bssid_abnormal_type t3 on t3.day='$day' and t1.bssid=t3.bssid
where t1.day='$day'
and rate_5km>=0.8
and rate_500m>=0.5
and ssid_num=1
and t2.bssid is null
and t3.bssid is null;
"

: '
增量表合并到全量表的计算规则如下
更新优先级：异常型>移动型>稳定型>无法识别型
稳定型BSSID：全量表中的bssid为稳定型，若增量表中为异常型或者移动型，更新时以增量表数据为准，若增量表中未无法识别型bssid，更新时以全量表数据为准；
移动型BSSID：全量表中的bssid为移动型，若增量表中为异常型或者连续三个月为稳定型，更新时以增量表数据为准，若增量表中没有连续三个月为稳定型或者无法识别型，更新时以全量表数据为准；
异常型BSSID：全量表中的bssid为异常型，若连续三个月的增量表中为稳定型或者连续三个月为移动型，更新时以增量表数据为准，若增量表中为无法识别型或者没有连续三个月为稳定型/移动型，则以全量表中为准；
无法识别型BSSID：全量表中的bssid为无法识别型，更新时以增量表中数据为准。
'
#计算dw_mobdi_md.bssid_type_all表小于day最近的一个分区
lastPartition=`hive -e "show partitions $dim_bssid_type_all_mf" | awk -v day=${day} -F '=' '$2<day {print $0}'| sort| tail -n 1`
#计算小于等于day最近的三个分区，并用' or '连接
newestThreePartitions=`hive -e "show partitions $bssid_stable_type" | awk -v day=${day} -F '=' '$2<=day {print $0}'| sort| tail -n 3| xargs echo| sed 's/\s/ or /g'`
echo ${newestThreePartitions}

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $dim_bssid_type_all_mf partition(day='$day')
select ful.bssid,
       case
         when ful.type=2 and stable_3month.bssid is not null then 1
         when ful.type=3 and mobile_3month.bssid is not null then 2
         when ful.type=3 and stable_3month.bssid is not null then 1
         else ful.type
       end as type

from
(
  select bssid,max(type) as type
  from
  (
    select bssid,type from $dim_bssid_type_all_mf where $lastPartition

    union all

    SELECT bssid, 1 as type from $bssid_stable_type where day='$day'

    union all

    SELECT bssid, 2 as type from $bssid_mobile_type where day='$day'

    union all

    SELECT bssid, 3 as type from $bssid_abnormal_type where day='$day'
  ) t1
  group by bssid
) ful
left join
(
  --连续三个月为稳定型
  SELECT bssid
  from $bssid_stable_type where $newestThreePartitions
  group by bssid
  having count(1)=3
) stable_3month on ful.bssid=stable_3month.bssid
left join
(
  --连续三个月为移动型
  SELECT bssid
  from $bssid_mobile_type where $newestThreePartitions
  group by bssid
  having count(1)=3
) mobile_3month on ful.bssid=mobile_3month.bssid;
"