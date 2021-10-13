#!/bin/sh

: '
@owner: wangyc
@describe: 社交平台信息总表
@update: 20190215 1. 主键改为device+plat+snsplat+snsuid 2. 添加rawuserdata（不用解析）
@projectname: MOBDI
@businessname: sns
@sourcetable: dw_mobdi_etl.dwd_log_share_new_di,dw_mobdi_etl.dwd_log_oauth_new_di,dw_mobdi_md.sns_tmp_oauth,dw_mobdi_md.sns_tmp_share,rp_mobdi_app.rp_device_sns_full
@targettable: dw_mobdi_md.sns_tmp_share,dw_mobdi_md.sns_tmp_oauth,rp_mobdi_app.rp_device_sns_full
@TableRelation:dw_mobdi_etl.dwd_log_share_new_di->dw_mobdi_md.sns_tmp_share|dw_mobdi_etl.dwd_log_oauth_new_di->dw_mobdi_md.sns_tmp_oauth|dw_mobdi_md.sns_tmp_share,dw_mobdi_md.sns_tmp_oauth->rp_mobdi_app.rp_device_sns_full
'

set -x -e
export LANG=en_US.UTF-8

if [ $# -ne 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: <date>"
     exit 1
fi

indate=$1

: '
实现功能：share增量临时表

实现逻辑：从$dwd_log_share_new_di取当天日志
          只取snsuserpartdata有值的记录
          对于每一个device+plat+snsplat上的share信息只保留最新
          share表只提供以下信息：
          gender 用户性别
          birthday 用户生日
          secret 用户认证信息
          educationjsonarraystr 教育信息
          workjsonarraystr 工作信息
          从 $dwd_log_share_new_di 取数时要加上 '' as rawuserdata(20190215 update)

输出结果：表名(dw_mobdi_md.sns_tmp_share)

'
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
#input
#dwd_log_share_new_di=dm_mobdi_master.dwd_log_share_new_di
#dwd_log_oauth_new_di=dm_mobdi_master.dwd_log_oauth_new_di
tmpdb=$dw_mobdi_tmp
#md
sns_tmp_share=$tmpdb.sns_tmp_share
sns_tmp_oauth=$tmpdb.sns_tmp_oauth

#out
#rp_device_sns_full=dm_mobdi_report.rp_device_sns_full
lastpartition=`hive -e "show partitions $rp_device_sns_full"|sort|tail -n1`

hive -e "
set hive.exec.parallel=true;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
create table if not exists $sns_tmp_share(
device string,
plat int,
snsplat int,
snsuid string,
birthday string,
icon string,
secrettype string,
nickname string,
snsuserurl string,
sharecount string,
resume string,
educationjson string,
snsuserlevel string,
snsregat string,
favouritecount string,
gender string,
secret string,
followercount string,
workjson string,
processtime string,
rawuserdata string comment '社交平台响应体加密信息'
)
partitioned by (day string)
stored as orc;

insert overwrite table $sns_tmp_share partition(day=$indate)
select device,plat,snsplat,snsuid,birthday,icon,secrettype,nickname,snsuserurl,sharecount,resume,
       educationjson,snsuserlevel,snsregat,favouritecount,gender,secret,followercount,workjson,
       processtime,'' as rawuserdata
from(
    select device,plat,snsplat,snsuid,birthday,icon,secrettype,nickname,snsuserurl,sharecount,resume,
           educationjson,snsuserlevel,snsregat,favouritecount,gender,secret,followercount,workjson,processtime,
    row_number() over (partition by device,plat,snsplat,snsuid order by processtime desc)  as rank
    from(
        select  muid as device,
                plat,
                snsplat,
                snsuid,
                '' as nickname,
                '' icon,
                case
                when split(snsuserpartdata,'\\\\|')[0]='0' then '0'
                when split(snsuserpartdata,'\\\\|')[0]='1' then '1'
                when split(snsuserpartdata,'\\\\|')[0]='2' then '-1'
                else ''
                end as gender,
                '' as  snsuserurl,
                '' as  resume,
                '' as secret,
                split(snsuserpartdata,'\\\\|')[2] as secrettype,
                case
                when length(from_unixtime(floor(split(snsuserpartdata,'\\\\|')[1]/1000),'yyyyMMdd')) = 8
                and DATEDIFF(CURRENT_DATE,from_unixtime(floor(split(snsuserpartdata,'\\\\|')[1]/1000),'yyyy-MM-dd')) < 100*365
                and DATEDIFF(CURRENT_DATE,from_unixtime(floor(split(snsuserpartdata,'\\\\|')[1]/1000),'yyyy-MM-dd')) > 5*365
                then from_unixtime(floor(split(snsuserpartdata,'\\\\|')[1]/1000),'yyyyMMdd')
                else ''
                end as birthday,
                '' as followercount,
                '' as favouritecount,
                '' as sharecount,
                '' as snsregat,
                '' as snsuserlevel,
                split(snsuserpartdata,'\\\\|')[3] as educationjson,
                split(snsuserpartdata,'\\\\|')[4] as workjson,
                day as processtime
        from    $dwd_log_share_new_di
        where   day = ${indate}
                and length(trim(muid)) = 40
                and length(trim(snsuserpartdata)) > 4
                and plat=1

                union all

                select  deviceid as device,
                plat,
                snsplat,
                snsuid,
                '' as nickname,
                '' icon,
                case
                when split(snsuserpartdata,'\\\\|')[0]='0' then '0'
                when split(snsuserpartdata,'\\\\|')[0]='1' then '1'
                when split(snsuserpartdata,'\\\\|')[0]='2' then '-1'
                else ''
                end as gender,
                '' as  snsuserurl,
                '' as  resume,
                '' as secret,
                split(snsuserpartdata,'\\\\|')[2] as secrettype,
                case
                when length(from_unixtime(floor(split(snsuserpartdata,'\\\\|')[1]/1000),'yyyyMMdd')) = 8
                and DATEDIFF(CURRENT_DATE,from_unixtime(floor(split(snsuserpartdata,'\\\\|')[1]/1000),'yyyy-MM-dd')) < 100*365
                and DATEDIFF(CURRENT_DATE,from_unixtime(floor(split(snsuserpartdata,'\\\\|')[1]/1000),'yyyy-MM-dd')) > 5*365
                then from_unixtime(floor(split(snsuserpartdata,'\\\\|')[1]/1000),'yyyyMMdd')
                else ''
                end as birthday,
                '' as followercount,
                '' as favouritecount,
                '' as sharecount,
                '' as snsregat,
                '' as snsuserlevel,
                split(snsuserpartdata,'\\\\|')[3] as educationjson,
                split(snsuserpartdata,'\\\\|')[4] as workjson,
                day as processtime
        from    $dwd_log_share_new_di
        where   day = ${indate}
                and length(trim(deviceid)) = 40
                and length(trim(snsuserpartdata)) > 4
                and plat=2
        )a1
    )aa
where rank = 1;
"


: '
实现功能：oauth增量临时表

实现逻辑：从$dwd_log_oauth_new_di取当天日志
          只取snsuserdata有值的记录
          对于每一个device+plat+snsplat上的oauth信息只保留最新

输出结果：表名($sns_tmp_oauth)

'

hive -e "
set hive.exec.parallel=true;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
create table if not exists $sns_tmp_oauth(
device string,
plat int,
snsplat int,
snsuid string,
birthday string,
icon string,
secrettype string,
nickname string,
snsuserurl string,
sharecount string,
resume string,
educationjson string,
snsuserlevel string,
snsregat string,
favouritecount string,
gender string,
secret string,
followercount string,
workjson string,
processtime string,
rawuserdata string comment '社交平台响应体加密信息'
)
partitioned by (day string)
stored as orc;

insert overwrite table $sns_tmp_oauth partition(day=$indate)
select device,plat,snsplat,snsuid,birthday,icon,secrettype,nickname,snsuserurl,sharecount,resume,
educationjson,snsuserlevel,snsregat,favouritecount,gender,secret,followercount,workjson,processtime,rawuserdata
from
    (
    select device,plat,snsplat,snsuid,birthday,icon,secrettype,nickname,snsuserurl,sharecount,resume,
    educationjson,snsuserlevel,snsregat,favouritecount,gender,secret,followercount,workjson,processtime,rawuserdata,
    row_number() over (partition by device,plat,snsplat,snsuid order by processtime desc) as rank
    from
        (
        select muid as device,
            plat,
            snsplat,
            snsuid,
            case
            when length(from_unixtime(floor(snsuserdata['birthday']/1000),'yyyyMMdd')) = 8
            and DATEDIFF(CURRENT_DATE,from_unixtime(floor(snsuserdata['birthday']/1000),'yyyy-MM-dd')) < 100*365
            and DATEDIFF(CURRENT_DATE,from_unixtime(floor(snsuserdata['birthday']/1000),'yyyy-MM-dd')) > 5*365
            then from_unixtime(floor(snsuserdata['birthday']/1000),'yyyyMMdd')
            else ''
            end as birthday,
            snsuserdata['icon'] as icon,
            case
            when trim(snsuserdata['secretType'])='-1' then '-1'
            when trim(snsuserdata['secretType'])='0' then '0'
            when trim(snsuserdata['secretType'])='1' then '1'
            else ''
            end as secrettype,
            snsuserdata['nickname'] as nickname,
            snsuserdata['snsUserUrl'] as snsuserurl,
            snsuserdata['shareCount'] as sharecount,
            snsuserdata['resume'] as resume,
            snsuserdata['educationJSON'] as educationjson,
            snsuserdata['snsUserLevel'] as snsuserlevel,
            case
            when length(from_unixtime(floor(snsuserdata['snsregat']/1000),'yyyyMMdd')) = 8 then from_unixtime(floor(snsuserdata['snsregat']/1000),'yyyyMMdd')
            else ''
            end as snsregat,
            snsuserdata['favouriteCount'] as favouritecount,
            case
            when trim(snsuserdata['gender'])='0' then '0'
            when trim(snsuserdata['gender'])='1' then '1'
            when trim(snsuserdata['gender'])='2' then '-1'
            else ''
            end as gender,
            snsuserdata['secret'] as secret,
            snsuserdata['followerCount'] as followercount,
            snsuserdata['workJSON'] as workjson,
            day as processtime,
            rawuserdata
        from $dwd_log_oauth_new_di
        where day = ${indate}
              and length(trim(muid)) = 40
              and size(snsuserdata)>1
              and (snsuserdata['birthday'] <> '' or snsuserdata['icon'] <> '' or snsuserdata['secretType'] <> '' or snsuserdata['nickname'] <> '' or snsuserdata['snsUserUrl'] <> '' or snsuserdata['shareCount'] <> '' or snsuserdata['resume'] <> '' or snsuserdata['educationJSON'] <> '' or snsuserdata['snsUserLevel'] <> '' or snsuserdata['snsregat'] <> '' or snsuserdata['favouriteCount'] <> '' or snsuserdata['gender'] <> '' or snsuserdata['secret'] <> '' or snsuserdata['followerCount'] <> '' or snsuserdata['workJSON'] <> '')
              and plat=1

              union all

        select deviceid as device,
            plat,
            snsplat,
            snsuid,
            case
            when length(from_unixtime(floor(snsuserdata['birthday']/1000),'yyyyMMdd')) = 8
            and DATEDIFF(CURRENT_DATE,from_unixtime(floor(snsuserdata['birthday']/1000),'yyyy-MM-dd')) < 100*365
            and DATEDIFF(CURRENT_DATE,from_unixtime(floor(snsuserdata['birthday']/1000),'yyyy-MM-dd')) > 5*365
            then from_unixtime(floor(snsuserdata['birthday']/1000),'yyyyMMdd')
            else ''
            end as birthday,
            snsuserdata['icon'] as icon,
            case
            when trim(snsuserdata['secretType'])='-1' then '-1'
            when trim(snsuserdata['secretType'])='0' then '0'
            when trim(snsuserdata['secretType'])='1' then '1'
            else ''
            end as secrettype,
            snsuserdata['nickname'] as nickname,
            snsuserdata['snsUserUrl'] as snsuserurl,
            snsuserdata['shareCount'] as sharecount,
            snsuserdata['resume'] as resume,
            snsuserdata['educationJSON'] as educationjson,
            snsuserdata['snsUserLevel'] as snsuserlevel,
            case
            when length(from_unixtime(floor(snsuserdata['snsregat']/1000),'yyyyMMdd')) = 8 then from_unixtime(floor(snsuserdata['snsregat']/1000),'yyyyMMdd')
            else ''
            end as snsregat,
            snsuserdata['favouriteCount'] as favouritecount,
            case
            when trim(snsuserdata['gender'])='0' then '0'
            when trim(snsuserdata['gender'])='1' then '1'
            when trim(snsuserdata['gender'])='2' then '-1'
            else ''
            end as gender,
            snsuserdata['secret'] as secret,
            snsuserdata['followerCount'] as followercount,
            snsuserdata['workJSON'] as workjson,
            day as processtime,
            rawuserdata
        from $dwd_log_oauth_new_di
        where day = ${indate}
              and length(trim(deviceid)) = 40
              and size(snsuserdata)>1
              and (snsuserdata['birthday'] <> '' or snsuserdata['icon'] <> '' or snsuserdata['secretType'] <> '' or snsuserdata['nickname'] <> '' or snsuserdata['snsUserUrl'] <> '' or snsuserdata['shareCount'] <> '' or snsuserdata['resume'] <> '' or snsuserdata['educationJSON'] <> '' or snsuserdata['snsUserLevel'] <> '' or snsuserdata['snsregat'] <> '' or snsuserdata['favouriteCount'] <> '' or snsuserdata['gender'] <> '' or snsuserdata['secret'] <> '' or snsuserdata['followerCount'] <> '' or snsuserdata['workJSON'] <> '')
              and plat=2
        ) a
    ) aa
where rank = 1;
"

: '
实现功能：sns_full增量更新

实现逻辑：oauth全部插入到full表
          share只补充oauth和full没有的
          对于每一个device+plat+snsplat只保留最新

输出结果：表名($rp_device_sns_full)

'

hive -e "
set hive.exec.parallel=true;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
create table if not exists $rp_device_sns_full(
device string,
plat int,
snsplat int,
snsuid string,
birthday string,
icon string,
secrettype string,
nickname string,
snsuserurl string,
sharecount string,
resume string,
educationjson string,
snsuserlevel string,
snsregat string,
favouritecount string,
gender string,
secret string,
followercount string,
workjson string,
processtime string,
rawuserdata string comment '社交平台响应体加密信息',
unionid string comment '微信UnionID'
)
partitioned by (day string)
stored as orc;

insert overwrite table $rp_device_sns_full partition(day=$indate)
select device,plat,snsplat,snsuid,birthday,icon,secrettype,nickname,snsuserurl,
sharecount,resume,educationjson,snsuserlevel,snsregat,favouritecount,gender,secret,followercount,workjson,processtime,rawuserdata,
get_json_object(rawuserdata, '$.unionid') as unionid
from
    (
    select device,plat,snsplat,snsuid,birthday,icon,secrettype,nickname,snsuserurl,sharecount,resume,educationjson,
    snsuserlevel,snsregat,favouritecount,gender,secret,followercount,workjson,processtime,rawuserdata,
    row_number() over (partition by device,plat,snsplat,snsuid order by processtime desc) as rank
    from
        (
        select device,plat,snsplat,snsuid,birthday,icon,secrettype,nickname,snsuserurl,sharecount,resume,educationjson,
        snsuserlevel,snsregat,favouritecount,gender,secret,followercount,workjson,processtime,rawuserdata
        from $sns_tmp_oauth
        where day = ${indate}

        union all

        select device,plat,snsplat,snsuid,birthday,icon,secrettype,nickname,snsuserurl,sharecount,resume,educationjson,
        snsuserlevel,snsregat,favouritecount,gender,secret,followercount,workjson,processtime,rawuserdata
        from $rp_device_sns_full
        where day = ${lastpartition}

        union all

        select
        a1.device,a1.plat,a1.snsplat,a1.snsuid,a1.birthday,a1.icon,a1.secrettype,a1.nickname,a1.snsuserurl,a1.sharecount,
        a1.resume,a1.educationjson,a1.snsuserlevel,a1.snsregat,a1.favouritecount,a1.gender,a1.secret,a1.followercount,a1.workjson,
        a1.processtime,a1.rawuserdata
        from
        (select * from $sns_tmp_share where day = ${indate}) a1
        left join
        (select device from $sns_tmp_oauth where day = ${indate} group by device) a2
        on a1.device = a2.device
        left join
        (select device from $rp_device_sns_full where day = ${lastpartition} group by device) a3
        on a1.device = a3.device
        where a2.device is null and a3.device is null
        )a
    )aa
where rank = 1;
"
