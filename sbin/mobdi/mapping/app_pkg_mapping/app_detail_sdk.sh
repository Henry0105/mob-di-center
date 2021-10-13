#!/bin/bash

: '
@owner: 
@describe: APP详情--来自豌豆荚和应用宝爬虫,每天跑
@projectName: MobDI
@BusinessName: 
@SourceTable: dw_appgo_crawl.app_wdj_info,dw_appgo_crawl.app_yyb_info,dw_mobdi_md.sdkdetail,dm_sdk_mapping.dim_app_pkg_mapping_par,dm_sdk_mapping.dim_app_category_mapping_par,dm_sdk_mapping.dim_app_info_sdk,rp_mobdi_app.app_details_sdk
@TargetTable: dw_mobdi_md.sdkdetail,dm_sdk_mapping.dim_app_info_sdk,rp_mobdi_app.app_details_sdk
@TableRelation:dw_appgo_crawl.app_wdj_info,dw_appgo_crawl.app_yyb_info,dm_sdk_mapping.dim_app_pkg_mapping_par,dm_sdk_mapping.dim_app_category_mapping_par->dw_mobdi_md.sdkdetail|dw_mobdi_md.sdkdetail->rp_mobdi_app.app_details_sdk|rp_mobdi_app.app_details_sdk->dm_sdk_mapping.dim_app_info_sdk
'

set -x -e

if [ $# -lt 1 ]; then
 echo "ERROR: wrong number of parameters"
 echo "USAGE: <date>"
 exit 1
fi
   
day=$1;
wdj_last_par=${day}
yyb_last_par=${day}

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_ods.properties

tmpdb="$dm_mobdi_tmp"

#input
#app_wdj_info=dw_appgo_crawl.app_wdj_info
#app_yyb_info=dw_appgo_crawl.app_yyb_info

#mapping
#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par
#dim_app_category_mapping_par=dim_sdk_mapping.dim_app_category_mapping_par

#output
#app_details_sdk=dm_mobdi_report.app_details_sdk
#dim_app_info_sdk=dim_sdk_mapping.dim_app_info_sdk
sdkdetail=$tmpdb.sdkdetail


# 检查爬虫表上一个日期分区的数据是否生成
CHECK_DATA()
{
  local src_path=$1
  hadoop fs -test -e $src_path
  if [[ $? -eq 0 ]] ; then
    # path存在
    src_data_du=`hadoop fs -du -s $src_path | awk '{print $1}'`
    # 文件夹大小不为0
    if [[ $src_data_du != 0 ]] ;then
      return 0
    else
      return 1
    fi
  else
      return 1
  fi
}

app_wdj_info_db=$(echo $app_wdj_info|awk -F '.' '{print $1}')
app_wdj_info_tb=$(echo $app_wdj_info|awk -F '.' '{print $2}')

app_yyb_info_db=$(echo $app_yyb_info|awk -F '.' '{print $1}')
app_yyb_info_tb=$(echo $app_yyb_info|awk -F '.' '{print $2}')



CHECK_DATA "hdfs://ShareSdkHadoop/hiveDW/$app_wdj_info_db/$app_wdj_info_tb/day=${wdj_last_par}"
CHECK_DATA "hdfs://ShareSdkHadoop/hiveDW/$app_yyb_info_db/$app_yyb_info_tb/day=${yyb_last_par}"

   #整合wdj和yyb两张源表，将app包名为空或者是乱码的去除
   #在app_id(app包名),zone(地区),lang(语言) 对时间去重，优先取wdj，因为wdj有suport_os字段
   sql="
   add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
   create temporary function array_struct_to_array as 'com.youzu.mob.java.udf.ArrayStruct2Array';

   insert overwrite table $sdkdetail
   select app_id,
   ios_id,
   zone,
   lang,
   icon,
   screenshots,
   name,
   app_category_id,
   publisher,
   publisher_id,
   channel_link,
   is_iap,
   iap,
   about,
   description,
   family,
   channel_id,
   createat
   from
       (
       select *,row_number() over (partition by app_id,zone,lang order by tag,about.updated desc) rk
       from
           (
           select packagename as app_id,
                  '' as ios_id,
                  '' as zone,
                  '' as lang,
                  icon_src as icon,
                  split(array_struct_to_array(scrolls),',') as screenshots,
                  appname as name,
                  cat_l1_id as app_category_id,
                  developer_name as publisher,
                  '' as publisher_id,
                  url as channel_link,
                  -1 as is_iap,
                  '' as iap,
                  named_struct('updated',publisheddate,'version',coalesce(version,''),'size',filesize,'installs',downloadnum,'offer_by',developer_name,'content_rating','','interactiveelement','','requiresandroid',suport_os,'report','','permissions',permissions,'developer',named_struct('developer_link','','developer_website','','developer_email','','developer_address','')) as about,
                  summary as description,
                  -1 as family,
                  3 as channel_id,
                  day as createat,
                  1 as tag
           from $app_wdj_info
           where day=${wdj_last_par}
           and packagename is not null
           and length(trim(packagename)) > 0
           and packagename = regexp_extract(packagename,'([a-zA-Z0-9\.\_]+)',0)

           union all

           select packagename as app_id,
                  '' as ios_id,
                  '' as zone,
                  '' as lang,
                  icon_src as icon,
                  split(array_struct_to_array(scrolls),',') as screenshots,
                  appname as name,
                  cat_l1_id as app_category_id,
                  developer_name as publisher,
                  '' as publisher_id,
                  url as channel_link,
                  -1 as is_iap,
                  '' as iap,
                  named_struct('updated',publisheddate,'version',coalesce(version,''),'size',filesize,'installs',downloadnum,'offer_by',developer_name,'content_rating','','interactiveelement','','requiresandroid','','report','','permissions',permissions,'developer',named_struct('developer_link','','developer_website','','developer_email','','developer_address','')) as about,
                  summary as description,
                  -1 as family,
                  3 as channel_id,
                  day as createat,
                  2 as tag
           from $app_yyb_info
           where day=${yyb_last_par}
           and packagename is not null
           and length(trim(packagename)) > 0
           and packagename = regexp_extract(packagename,'([a-zA-Z0-9\.\_]+)',0)
           )a
       )aa
   where rk = 1;
   "
   echo "$sql"; hive -e "$sql";


   #对app包名进行渠道清理，同时赋值app_category_id
   sql="
   INSERT overwrite TABLE $sdkdetail
   SELECT x.app_id
            ,ios_id
            ,zone
            ,lang
            ,icon
            ,screenshots
            ,name
            ,coalesce(y.cate_l1_id,'') AS app_category_id
            ,publisher
            ,publisher_id
            ,channel_link
            ,is_iap
            ,iap
            ,about
            ,description
            ,family
            ,channel_id
            ,createat
   FROM (
            SELECT coalesce(b.apppkg, a.app_id) app_id
                      ,ios_id
                      ,zone
                      ,lang
                      ,icon
                      ,screenshots
                      ,name
                      ,'' AS app_category_id
                      ,publisher
                      ,publisher_id
                      ,channel_link
                      ,is_iap
                      ,iap
                      ,about
                      ,description
                      ,family
                      ,channel_id
                      ,createat
            FROM $sdkdetail a
            LEFT JOIN (SELECT pkg,apppkg FROM $dim_app_pkg_mapping_par WHERE version = '1000') b ON a.app_id = b.pkg
            ) x
   LEFT OUTER JOIN (
            SELECT apppkg
                      ,max(cate_l1_id) AS cate_l1_id
                      ,max(cate_l2_id) AS cate_l2_id
            FROM $dim_app_category_mapping_par
            WHERE version = '1000'
            GROUP BY apppkg
            ) y ON x.app_id = y.apppkg;
   "
   echo "$sql"; hive -e "$sql";


   #对渠道清理后的app包名进行去重,替换掉无意义的publisher
   #对app_id,zone,family和version 在par_day上去重，取最近的记录
   sql="
   insert overwrite table $app_details_sdk
   select app_id,
          ios_id,
          zone,
          lang,
         icon,
          screenshots_iphone,
          screenshots_ipad,
          screenshots_android,
          name,
          app_category_id,
          publisher,
          publisher_id,
          channel_link,
          is_iap,
          iap,
          about,
          description,
          family,
          channel_id,
          par_day
   from
   (
   select *,row_number() over(partition by app_id,zone,family,coalesce(about.version,'') order by par_day desc) rk
   from
       (
       select app_id,
              ios_id,
              zone,
              lang,
              icon,
              array('') as screenshots_iphone,
              array('') as screenshots_ipad,
              screenshots as screenshots_android,
              name,
              app_category_id,
              case when trim(publisher) in ('Google Play', 'Google Play提供', '应用汇', '安卓市场', '百度手机助手', '360手机助手', '应用宝',
              '搜狗应用市场', '当乐网', '小米手机助手', 'OPPO软件商店', 'Mobogenie', '酷安网', '华为应用市场', '互联网', '优亿市场',
              'N多市场', '机锋市场', '网易应用中心', '豌豆荚', 'ApkPure', '小米应用商店', '来自Google?Play', '来自互联网', '来自谷歌市场',
              '魅族应用商店', 'Google?Play提供', '来自豌豆荚', '来自360助手', '来自当乐', '来自安智') then '未知'
              else publisher
              end as publisher,
              publisher_id,
              channel_link,
              is_iap,
              iap,
              about,
              description,
              family,
              channel_id,
              ${day} as par_day
       from
           (
           select *,row_number() over(partition by app_id,zone,lang order by about.updated desc) rk
           from $sdkdetail
           )a
       where rk = 1

       union all

       select app_id,
              ios_id,
              zone,
              lang,
              icon,
              screenshots_iphone,
              screenshots_ipad,
              screenshots_android,
              name,
              app_category_id,
              publisher,
              publisher_id,
              channel_link,
              is_iap,
              iap,
              about,
              description,
              family,
              channel_id,
              par_day
       from $app_details_sdk
       )aa
   )aaa
   where rk = 1
   "
   echo "$sql"; hive -e "$sql";


   #加入新爬到的app，并对老的app信息更新
   sql="
   insert overwrite table $dim_app_info_sdk
   select app_id,ios_id,zone,lang,icon,name,app_category_id,publisher,channel_id,family,day
   from
   (
     select *,
            row_number() over (partition by app_id order by day desc) as rk
     from
     (
       select a.app_id,ios_id,a.zone,a.lang,if((length(a.icon)=0 or a.icon is null) and b.icon is not null,b.icon,a.icon) as icon,name,app_category_id,publisher,channel_id,family,par_day as day
       from
       (
         select app_id,ios_id,zone,lang,icon,name,app_category_id,publisher,channel_id,family,par_day,
                row_number() over (partition by app_id,zone,lang order by par_day desc) rk
         from $app_details_sdk
       )a
       left join --爬虫系统有时获取不到icon，所以在这里做了补偿，如果发现app的icon是空值，将最新不为空的icon补充进来
       (
         select app_id,zone,lang,icon
         from
         (
           select app_id,zone,lang,icon,par_day,
                  row_number() over (partition by app_id,zone,lang order by par_day desc) rk
           from $app_details_sdk
           where length(icon)>0
         )a2
         where rk = 1
       )b on a.app_id=b.app_id and a.zone=b.zone and a.lang=b.lang
       where rk = 1

       union all

       select app_id,ios_id,zone,lang,icon,name,app_category_id,publisher,channel_id,family,day
       from $dim_app_info_sdk
     )x
   )y
   where rk = 1
   "
   echo "$sql"; hive -e "$sql";
   

