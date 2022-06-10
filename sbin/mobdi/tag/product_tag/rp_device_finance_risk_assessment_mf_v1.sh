#!/bin/bash

set -x -e

day=$1

echo "$day"


sourceTable1="rp_finance_anticheat_muid.rp_device_finance_risk_assessment_mf_v1"
outputTable="dm_mobdi_report.timewindow_online_profile_day_v3"


HADOOP_USER_NAME=dba hive -v -e "
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=500;
set mapred.map.tasks.speculative.execution=false;
set mapred.reduce.tasks.speculative.execution=false;
set hive.mapred.reduce.tasks.speculative.execution=false;
set hive.optimize.index.filter=true;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts=-Xmx6g -XX:+UseG1GC;
SET mapreduce.reduce.memory.mb=10240;
SET mapreduce.reduce.java.opts='-Xmx8g';


add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION map_to_str AS 'com.youzu.mob.java.map.MapToString';
create temporary function map_concat as 'com.youzu.mob.java.map.MapConcat';
create temporary function map_agg as 'com.youzu.mob.java.map.MapAgg';


insert overwrite table $outputTable partition (day='${day}')
select
  device,map_agg(profile) as  profile
from
(
    select trim(lower(device)) as device, map(
    '7793_1000',cast(fin_1_1 as string),
    '7794_1000',cast(fin_1_2 as string),
    '7795_1000',cast(fin_1_3 as string),
    '7796_1000',cast(fin_1_4 as string),
    '7797_1000',cast(fin_1_5 as string),
    '7798_1000',cast(fin_1_6 as string),
    '7799_1000',cast(fin_10_1 as string),
    '7800_1000',cast(fin_10_2 as string),
    '7801_1000',cast(fin_10_3 as string),
    '7802_1000',cast(fin_10_4 as string),
    '7803_1000',cast(fin_10_5 as string),
    '7804_1000',cast(fin_10_6 as string),
    '7805_1000',cast(fin_11_1 as string),
    '7806_1000',cast(fin_11_2 as string),
    '7807_1000',cast(fin_11_3 as string),
    '7808_1000',cast(fin_11_4 as string),
    '7809_1000',cast(fin_11_5 as string),
    '7810_1000',cast(fin_11_6 as string),
    '7811_1000',cast(fin_12_1 as string),
    '7812_1000',cast(fin_12_2 as string),
    '7813_1000',cast(fin_12_3 as string),
    '7814_1000',cast(fin_12_4 as string),
    '7815_1000',cast(fin_12_5 as string),
    '7816_1000',cast(fin_12_6 as string),
    '7817_1000',cast(fin_13_1 as string),
    '7818_1000',cast(fin_13_2 as string),
    '7819_1000',cast(fin_13_3 as string),
    '7820_1000',cast(fin_13_4 as string),
    '7821_1000',cast(fin_13_5 as string),
    '7822_1000',cast(fin_13_6 as string),
    '7823_1000',cast(fin_14_1 as string),
    '7824_1000',cast(fin_14_2 as string),
    '7825_1000',cast(fin_14_3 as string),
    '7826_1000',cast(fin_14_4 as string),
    '7827_1000',cast(fin_14_5 as string),
    '7828_1000',cast(fin_14_6 as string),
    '7829_1000',cast(fin_15_1 as string),
    '7830_1000',cast(fin_15_2 as string),
    '7831_1000',cast(fin_15_3 as string),
    '7832_1000',cast(fin_15_4 as string),
    '7833_1000',cast(fin_15_5 as string),
    '7834_1000',cast(fin_15_6 as string),
    '7835_1000',cast(fin_16_1 as string),
    '7836_1000',cast(fin_16_2 as string),
    '7837_1000',cast(fin_16_3 as string),
    '7838_1000',cast(fin_16_4 as string),
    '7839_1000',cast(fin_16_5 as string),
    '7840_1000',cast(fin_16_6 as string),
    '7841_1000',cast(fin_17_1 as string),
    '7842_1000',cast(fin_17_2 as string),
    '7843_1000',cast(fin_17_3 as string),
    '7844_1000',cast(fin_17_4 as string),
    '7845_1000',cast(fin_17_5 as string),
    '7846_1000',cast(fin_17_6 as string),
    '7847_1000',cast(fin_18_1 as string),
    '7848_1000',cast(fin_18_2 as string),
    '7849_1000',cast(fin_18_3 as string),
    '7850_1000',cast(fin_18_4 as string),
    '7851_1000',cast(fin_18_5 as string),
    '7852_1000',cast(fin_18_6 as string),
    '7853_1000',cast(fin_19_1 as string),
    '7854_1000',cast(fin_19_2 as string),
    '7855_1000',cast(fin_19_3 as string),
    '7856_1000',cast(fin_19_4 as string),
    '7857_1000',cast(fin_19_5 as string),
    '7858_1000',cast(fin_19_6 as string),
    '7859_1000',cast(fin_2_1 as string),
    '7860_1000',cast(fin_2_2 as string),
    '7861_1000',cast(fin_2_3 as string),
    '7862_1000',cast(fin_2_4 as string),
    '7863_1000',cast(fin_2_5 as string),
    '7864_1000',cast(fin_2_6 as string),
    '7865_1000',cast(fin_20_1 as string),
    '7866_1000',cast(fin_20_2 as string),
    '7867_1000',cast(fin_20_3 as string),
    '7868_1000',cast(fin_20_4 as string),
    '7869_1000',cast(fin_20_5 as string),
    '7870_1000',cast(fin_20_6 as string),
    '7871_1000',cast(fin_21_1 as string),
    '7872_1000',cast(fin_21_2 as string),
    '7873_1000',cast(fin_21_3 as string),
    '7874_1000',cast(fin_21_4 as string),
    '7875_1000',cast(fin_21_5 as string),
    '7876_1000',cast(fin_21_6 as string),
    '7877_1000',cast(fin_22_1 as string),
    '7878_1000',cast(fin_22_2 as string),
    '7879_1000',cast(fin_22_3 as string),
    '7880_1000',cast(fin_22_4 as string),
    '7881_1000',cast(fin_22_5 as string),
    '7882_1000',cast(fin_22_6 as string),
    '7883_1000',cast(fin_23_1 as string),
    '7884_1000',cast(fin_23_2 as string),
    '7885_1000',cast(fin_23_3 as string),
    '7886_1000',cast(fin_23_4 as string),
    '7887_1000',cast(fin_23_5 as string),
    '7888_1000',cast(fin_23_6 as string),
    '7889_1000',cast(fin_24_1 as string),
    '7890_1000',cast(fin_24_2 as string),
    '7891_1000',cast(fin_24_3 as string),
    '7892_1000',cast(fin_24_4 as string),
    '7893_1000',cast(fin_24_5 as string),
    '7894_1000',cast(fin_24_6 as string),
    '7895_1000',cast(fin_25_1 as string),
    '7896_1000',cast(fin_25_2 as string),
    '7897_1000',cast(fin_25_3 as string),
    '7898_1000',cast(fin_25_4 as string),
    '7899_1000',cast(fin_25_5 as string),
    '7900_1000',cast(fin_25_6 as string),
    '7901_1000',cast(fin_26_1 as string),
    '7902_1000',cast(fin_26_2 as string),
    '7903_1000',cast(fin_26_3 as string),
    '7904_1000',cast(fin_26_4 as string),
    '7905_1000',cast(fin_26_5 as string),
    '7906_1000',cast(fin_26_6 as string),
    '7907_1000',cast(fin_27_1 as string),
    '7908_1000',cast(fin_27_2 as string),
    '7909_1000',cast(fin_27_3 as string),
    '7910_1000',cast(fin_27_4 as string),
    '7911_1000',cast(fin_27_5 as string),
    '7912_1000',cast(fin_27_6 as string),
    '7913_1000',cast(fin_28_1 as string),
    '7914_1000',cast(fin_28_2 as string),
    '7915_1000',cast(fin_28_3 as string),
    '7916_1000',cast(fin_28_4 as string),
    '7917_1000',cast(fin_28_5 as string),
    '7918_1000',cast(fin_28_6 as string),
    '7919_1000',cast(fin_29_1 as string),
    '7920_1000',cast(fin_29_2 as string),
    '7921_1000',cast(fin_29_3 as string),
    '7922_1000',cast(fin_29_4 as string),
    '7923_1000',cast(fin_29_5 as string),
    '7924_1000',cast(fin_29_6 as string),
    '7925_1000',cast(fin_3_1 as string),
    '7926_1000',cast(fin_3_2 as string),
    '7927_1000',cast(fin_3_3 as string),
    '7928_1000',cast(fin_3_4 as string),
    '7929_1000',cast(fin_3_5 as string),
    '7930_1000',cast(fin_3_6 as string),
    '7931_1000',cast(fin_30_1 as string),
    '7932_1000',cast(fin_30_2 as string),
    '7933_1000',cast(fin_30_3 as string),
    '7934_1000',cast(fin_30_4 as string),
    '7935_1000',cast(fin_30_5 as string),
    '7936_1000',cast(fin_30_6 as string),
    '7937_1000',cast(fin_31_1 as string),
    '7938_1000',cast(fin_31_2 as string),
    '7939_1000',cast(fin_31_3 as string),
    '7940_1000',cast(fin_31_4 as string),
    '7941_1000',cast(fin_31_5 as string),
    '7942_1000',cast(fin_31_6 as string),
    '7943_1000',cast(fin_32_1 as string),
    '7944_1000',cast(fin_32_2 as string),
    '7945_1000',cast(fin_32_3 as string),
    '7946_1000',cast(fin_32_4 as string),
    '7947_1000',cast(fin_32_5 as string),
    '7948_1000',cast(fin_32_6 as string),
    '7949_1000',cast(fin_33_1 as string),
    '7950_1000',cast(fin_33_2 as string),
    '7951_1000',cast(fin_33_3 as string),
    '7952_1000',cast(fin_33_4 as string),
    '7953_1000',cast(fin_33_5 as string),
    '7954_1000',cast(fin_33_6 as string),
    '7955_1000',cast(fin_34_1 as string),
    '7956_1000',cast(fin_34_2 as string),
    '7957_1000',cast(fin_34_3 as string),
    '7958_1000',cast(fin_34_4 as string),
    '7959_1000',cast(fin_34_5 as string),
    '7960_1000',cast(fin_34_6 as string),
    '7961_1000',cast(fin_35_1 as string),
    '7962_1000',cast(fin_35_2 as string),
    '7963_1000',cast(fin_35_3 as string),
    '7964_1000',cast(fin_35_4 as string),
    '7965_1000',cast(fin_35_5 as string),
    '7966_1000',cast(fin_35_6 as string),
    '7967_1000',cast(fin_36_1 as string),
    '7968_1000',cast(fin_36_2 as string),
    '7969_1000',cast(fin_36_3 as string),
    '7970_1000',cast(fin_36_4 as string),
    '7971_1000',cast(fin_36_5 as string),
    '7972_1000',cast(fin_36_6 as string),
    '7973_1000',cast(fin_37_1 as string),
    '7974_1000',cast(fin_37_2 as string),
    '7975_1000',cast(fin_37_3 as string),
    '7976_1000',cast(fin_37_4 as string),
    '7977_1000',cast(fin_37_5 as string),
    '7978_1000',cast(fin_37_6 as string),
    '7979_1000',cast(fin_38_1 as string),
    '7980_1000',cast(fin_38_2 as string),
    '7981_1000',cast(fin_38_3 as string),
    '7982_1000',cast(fin_38_4 as string),
    '7983_1000',cast(fin_38_5 as string),
    '7984_1000',cast(fin_38_6 as string),
    '7985_1000',cast(fin_39_1 as string),
    '7986_1000',cast(fin_39_2 as string),
    '7987_1000',cast(fin_39_3 as string),
    '7988_1000',cast(fin_39_4 as string),
    '7989_1000',cast(fin_39_5 as string),
    '7990_1000',cast(fin_39_6 as string),
    '7991_1000',cast(fin_4_1 as string),
    '7992_1000',cast(fin_4_2 as string),
    '7993_1000',cast(fin_4_3 as string),
    '7994_1000',cast(fin_4_4 as string),
    '7995_1000',cast(fin_4_5 as string),
    '7996_1000',cast(fin_4_6 as string),
    '7997_1000',cast(fin_40_1 as string),
    '7998_1000',cast(fin_40_2 as string),
    '7999_1000',cast(fin_40_3 as string),
    '8000_1000',cast(fin_40_4 as string),
    '8001_1000',cast(fin_40_5 as string),
    '8002_1000',cast(fin_40_6 as string),
    '8003_1000',cast(fin_41_1 as string),
    '8004_1000',cast(fin_41_2 as string),
    '8005_1000',cast(fin_41_3 as string),
    '8006_1000',cast(fin_41_4 as string),
    '8007_1000',cast(fin_41_5 as string),
    '8008_1000',cast(fin_41_6 as string),
    '8009_1000',cast(fin_42_1 as string),
    '8010_1000',cast(fin_42_2 as string),
    '8011_1000',cast(fin_42_3 as string),
    '8012_1000',cast(fin_42_4 as string),
    '8013_1000',cast(fin_42_5 as string),
    '8014_1000',cast(fin_42_6 as string),
    '8015_1000',cast(fin_43_1 as string),
    '8016_1000',cast(fin_43_2 as string),
    '8017_1000',cast(fin_43_3 as string),
    '8018_1000',cast(fin_43_4 as string),
    '8019_1000',cast(fin_43_5 as string),
    '8020_1000',cast(fin_43_6 as string),
    '8021_1000',cast(fin_44_1 as string),
    '8022_1000',cast(fin_44_2 as string),
    '8023_1000',cast(fin_44_3 as string),
    '8024_1000',cast(fin_44_4 as string),
    '8025_1000',cast(fin_44_5 as string),
    '8026_1000',cast(fin_44_6 as string),
    '8027_1000',cast(fin_45_1 as string),
    '8028_1000',cast(fin_45_2 as string),
    '8029_1000',cast(fin_45_3 as string),
    '8030_1000',cast(fin_45_4 as string),
    '8031_1000',cast(fin_45_5 as string),
    '8032_1000',cast(fin_45_6 as string),
    '8033_1000',cast(fin_46_1 as string),
    '8034_1000',cast(fin_46_2 as string),
    '8035_1000',cast(fin_46_3 as string),
    '8036_1000',cast(fin_46_4 as string),
    '8037_1000',cast(fin_46_5 as string),
    '8038_1000',cast(fin_46_6 as string),
    '8039_1000',cast(fin_47_1 as string),
    '8040_1000',cast(fin_47_2 as string),
    '8041_1000',cast(fin_47_3 as string),
    '8042_1000',cast(fin_47_4 as string),
    '8043_1000',cast(fin_47_5 as string),
    '8044_1000',cast(fin_47_6 as string),
    '8045_1000',cast(fin_48_1 as string),
    '8046_1000',cast(fin_48_2 as string),
    '8047_1000',cast(fin_48_3 as string),
    '8048_1000',cast(fin_48_4 as string),
    '8049_1000',cast(fin_48_5 as string),
    '8050_1000',cast(fin_48_6 as string),
    '8051_1000',cast(fin_49_1 as string),
    '8052_1000',cast(fin_49_2 as string),
    '8053_1000',cast(fin_49_3 as string),
    '8054_1000',cast(fin_49_4 as string),
    '8055_1000',cast(fin_49_5 as string),
    '8056_1000',cast(fin_49_6 as string),
    '8057_1000',cast(fin_5_1 as string),
    '8058_1000',cast(fin_5_2 as string),
    '8059_1000',cast(fin_5_3 as string),
    '8060_1000',cast(fin_5_4 as string),
    '8061_1000',cast(fin_5_5 as string),
    '8062_1000',cast(fin_5_6 as string),
    '8063_1000',cast(fin_50_1 as string),
    '8064_1000',cast(fin_50_2 as string),
    '8065_1000',cast(fin_50_3 as string),
    '8066_1000',cast(fin_50_4 as string),
    '8067_1000',cast(fin_50_5 as string),
    '8068_1000',cast(fin_50_6 as string),
    '8069_1000',cast(fin_51_1 as string),
    '8070_1000',cast(fin_51_2 as string),
    '8071_1000',cast(fin_51_3 as string),
    '8072_1000',cast(fin_51_4 as string),
    '8073_1000',cast(fin_51_5 as string),
    '8074_1000',cast(fin_51_6 as string),
    '8075_1000',cast(fin_52_1 as string),
    '8076_1000',cast(fin_52_2 as string),
    '8077_1000',cast(fin_52_3 as string),
    '8078_1000',cast(fin_52_4 as string),
    '8079_1000',cast(fin_52_5 as string),
    '8080_1000',cast(fin_52_6 as string),
    '8081_1000',cast(fin_6_1 as string),
    '8082_1000',cast(fin_6_2 as string),
    '8083_1000',cast(fin_6_3 as string),
    '8084_1000',cast(fin_6_4 as string),
    '8085_1000',cast(fin_6_5 as string),
    '8086_1000',cast(fin_6_6 as string),
    '8087_1000',cast(fin_7_1 as string),
    '8088_1000',cast(fin_7_2 as string),
    '8089_1000',cast(fin_7_3 as string),
    '8090_1000',cast(fin_7_4 as string),
    '8091_1000',cast(fin_7_5 as string),
    '8092_1000',cast(fin_7_6 as string),
    '8093_1000',cast(fin_8_1 as string),
    '8094_1000',cast(fin_8_2 as string),
    '8095_1000',cast(fin_8_3 as string),
    '8096_1000',cast(fin_8_4 as string),
    '8097_1000',cast(fin_8_5 as string),
    '8098_1000',cast(fin_8_6 as string),
    '8099_1000',cast(fin_9_1 as string),
    '8100_1000',cast(fin_9_2 as string),
    '8101_1000',cast(fin_9_3 as string),
    '8102_1000',cast(fin_9_4 as string),
    '8103_1000',cast(fin_9_5 as string),
    '8104_1000',cast(fin_9_6 as string)) as profile
  from $sourceTable1
  where day='$day'
  and trim(lower(device)) rlike '^[a-f0-9]{40}$' and trim(device)!='0000000000000000000000000000000000000000'
)union_source
group by device
cluster by device
;
"
