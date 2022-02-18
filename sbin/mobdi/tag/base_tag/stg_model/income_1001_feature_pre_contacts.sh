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

source /home/dba/mobdi_center/conf/hive-env.sh

tmp_db=$dm_mobdi_tmp

#input
calculate_model_device=${tmp_db}.calculate_model_device

#mapping
#dim_id_mapping_android_sec_df_view=dim_mobdi_mapping.dim_id_mapping_android_sec_df_view
pid_contacts_index_sec=${tmp_db}.pid_contacts_index_sec
pid_contacts_word2vec_index_sec=${tmp_db}.pid_contacts_word2vec_index_sec

#output
income_1001_pid_contacts_index_sec=${tmp_db}.income_1001_pid_contacts_index_sec
income_1001_phone_contacts_word2vec_index=${tmp_db}.income_1001_phone_contacts_word2vec_index

pidContactsIndexLastPar=(`hive -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('${tmp_db}', 'pid_contacts_index_sec ', 'day');
"`)
pidContactsWord2vecIndexLastPar=(`hive -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('${tmp_db}', 'pid_contacts_word2vec_index_sec', 'day');
"`)

pidPartition=`hive -e "show partitions $dim_device_pid_merge_df" | awk -v day=${day} -F '=' '$2<=day {print $0}'| sort| tail -n 1`


#通讯录特征计算
#先通过android_id_mapping_full表找出设备的最新手机号
#然后和通讯录特征结果dm_mobdi_tmp.pid_contacts_index_sec表join，得到设备的微商水军标志位、通讯录号码得分分段、是否有公司名、公司手机数量分段、职级排行分段、分词index
#最后处理50个词向量四分位数特征dm_mobdi_tmp.income_1001_pid_contacts_index_sec
hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function explode_tags as 'com.youzu.mob.java.udtf.ExplodeTags';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $income_1001_pid_contacts_index_sec partition(day='$day')
select c.device,c.pid,
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
  select a.device,b.pid
  from $calculate_model_device a
  inner join
  (
    select device,pid
    from
    (
      select device,pid,
             row_number() over(partition by device order by pn_tm desc) num
      from
      (
        select device,concat(pid,'=',pid_ltm) pid_list
        from $dim_device_pid_merge_df
        where $pidPartition
        and length(pid)>0
        and length(pid)<87000   --涉密后pid长度变长，同比例增加长度限制
      ) pid_info
      lateral view explode_tags(pid_list) pid_tmp as pid,pn_tm
    ) un
    where num=1
  ) b on a.device=b.device
  where a.day='$day'
) c
inner join
$pid_contacts_index_sec d on d.day='$pidContactsIndexLastPar' and c.pid=d.pid;

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
  from $income_1001_pid_contacts_index_sec a
  inner join
  $pid_contacts_word2vec_index_sec b on b.day='$pidContactsWord2vecIndexLastPar' and a.pid=b.pid
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