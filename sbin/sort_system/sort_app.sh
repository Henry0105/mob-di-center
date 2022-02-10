#!/bin/bash
: '
@owner: menff
@describe: 每个月自动导入待分拣app到mysql
@projectName: mobdi
@BusinessName: sort_system
@SourceTable: dm_sdk_mapping.apppkg_info,$rp_app_rank_category_insatll_monthly,dm_sdk_mapping.dim_app_pkg_mapping_par,$app_detail_wdj,$app_detail_yyb
@TargetTable: $sorted_pkg
@TableRelation:$rp_app_rank_category_insatll_monthly,$app_detail_wdj,$app_detail_yyb,dm_sdk_mapping.dim_app_pkg_mapping_par,dm_sdk_mapping.apppkg_info->$sorted_pkg
'

: '
1. 从app360项目中获取到top3000的apppkg
2. $app_detail_yyb中找到appname
3. $app_detail_wdj中找到url
'
source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp
#rp_app_rank_category_insatll_monthly=rp_mobeye_app360.rp_app_rank_category_insatll_monthly
#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par
sorted_pkg=$tmpdb.sorted_pkg
#app_detail_wdj=dw_ext_crawl.app_detail_wdj
#app_detail_yyb=dw_ext_crawl.app_detail_yyb

cd `dirname $0`
libpath=/home/dba/mobdi_center/lib

lastpar=`hive -S -e "show partitions $rp_app_rank_category_insatll_monthly" |tail -n 1`

lastPkgPar=`hive -S -e "show partitions $dim_app_pkg_mapping_par"| sort | tail -n 1`
hive -v -e "
insert overwrite table $sorted_pkg
select coalesce(g.pkg, a.apppkg) as pkg, a.apppkg, rank_install, install_cnt, h.name as appname, g.url from (
    select apppkg, rank_install, install_cnt, rank_install_esti
    from $rp_app_rank_category_insatll_monthly
    where $lastpar and zone='cn' and cate_id='0' and rank_install_esti<=6000 order by rank_install_esti
) as a
left join (
    select apppkg, pkg, url from (
        select coalesce(mapping.apppkg, d.pkg) as apppkg, d.pkg, url from (
            select pkg, url from (
                select pkg, url, row_number() over(partition by pkg order by day desc, url desc) as rn from (
                    select packagename as pkg, downloadlink as url, day from $app_detail_wdj
                    union all
                    select packagename as pkg, downloadlink as url, day from $app_detail_yyb
                ) as b
            ) as c
            where rn = 1
        )  as d
        left join
        (select * from $dim_app_pkg_mapping_par where $lastPkgPar ) as mapping
        on mapping.pkg = d.pkg
    ) as f
) as g
on a.apppkg = g.apppkg
left join dm_sdk_mapping.apppkg_info as h
on a.apppkg = h.apppkg
"

/opt/cloudera/parcels/CDH/bin/spark-submit --master yarn --deploy-mode cluster \
        --class com.mob.job.DataExport \
        --name "app_top3000_pickup_$lastpar" \
        --driver-memory 4G \
        --num-executors 10 \
        --executor-memory 4G \
        --executor-cores 4 \
      --driver-java-options "-XX:MaxPermSize=1g" \
        $libpath/mob-data-export-2.0.3.jar \
"{
    \"operationName\": \"hiveToMysql\",
    \"sourceInfo\": {
        \"object\": \"hive\",
        \"database\": \"dm_mobdi_tmp\",
        \"table\": \"sorted_pkg\",
        \"fields\": [
            \"pkg as pkg\",
            \"apppkg as apppkg\",
            \"rank_install as rank_install\",
            \"install_cnt as install_cnt\",
            \"appname as appname\",
            \"url as url\"
        ]
    },
    \"targetInfo\": {
        \"object\": \"mysql\",
        \"batchSize\": \"500\",
        \"table\": \"sorting_system.app_top3000_pickup\",
        \"url\": \"jdbc:mysql://10.89.120.12:3310/sorting_system?useUnicode=true&characterEncoding=UTF-8\",
        \"user\": \"root\",
        \"password\": \"mobtech2019java\"
    }
}"
