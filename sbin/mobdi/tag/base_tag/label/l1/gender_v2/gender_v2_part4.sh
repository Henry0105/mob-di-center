#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi
source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
p7=$(date -d "$day -7 days" "+%Y%m%d")
insertday=${day}_muid
tmpdb=$dm_mobdi_tmp

#device_applist_new="dm_mobdi_mapping.device_applist_new"

gender_feature_v2_part2="${tmpdb}.gender_feature_v2_part2"

gender_feature_v2_part4="${tmpdb}.gender_feature_v2_part4"

hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
insert overwrite table $gender_feature_v2_part4 partition(day='$insertday')
select t1.device,
(index1)/(cast (t2.tot_install_apps as float)) index1,
(index2)/(cast (t2.tot_install_apps as float)) index2,
(index3)/(cast (t2.tot_install_apps as float)) index3,
(index4)/(cast (t2.tot_install_apps as float)) index4,
(index5)/(cast (t2.tot_install_apps as float)) index5,
(index6)/(cast (t2.tot_install_apps as float)) index6,
(index7)/(cast (t2.tot_install_apps as float)) index7,
(index8)/(cast (t2.tot_install_apps as float)) index8,
(index9)/(cast (t2.tot_install_apps as float)) index9,
(index10)/(cast (t2.tot_install_apps as float)) index10,
(index11)/(cast (t2.tot_install_apps as float)) index11,
(index12)/(cast (t2.tot_install_apps as float)) index12,
(index13)/(cast (t2.tot_install_apps as float)) index13,
(index14)/(cast (t2.tot_install_apps as float)) index14,
(index15)/(cast (t2.tot_install_apps as float)) index15,
(index16)/(cast (t2.tot_install_apps as float)) index16,
(index17)/(cast (t2.tot_install_apps as float)) index17,
(index18)/(cast (t2.tot_install_apps as float)) index18,
(index19)/(cast (t2.tot_install_apps as float)) index19,
(index20)/(cast (t2.tot_install_apps as float)) index20,
(index21)/(cast (t2.tot_install_apps as float)) index21,
(index22)/(cast (t2.tot_install_apps as float)) index22,
(index23)/(cast (t2.tot_install_apps as float)) index23,
(index24)/(cast (t2.tot_install_apps as float)) index24,
(index25)/(cast (t2.tot_install_apps as float)) index25,
(index26)/(cast (t2.tot_install_apps as float)) index26,
(index27)/(cast (t2.tot_install_apps as float)) index27,
(index28)/(cast (t2.tot_install_apps as float)) index28,
(index29)/(cast (t2.tot_install_apps as float)) index29,
(index30)/(cast (t2.tot_install_apps as float)) index30,
(index31)/(cast (t2.tot_install_apps as float)) index31,
(index32)/(cast (t2.tot_install_apps as float)) index32,
(index33)/(cast (t2.tot_install_apps as float)) index33,
(index34)/(cast (t2.tot_install_apps as float)) index34,
(index35)/(cast (t2.tot_install_apps as float)) index35,
(index36)/(cast (t2.tot_install_apps as float)) index36,
(index37)/(cast (t2.tot_install_apps as float)) index37,
(index38)/(cast (t2.tot_install_apps as float)) index38,
(index39)/(cast (t2.tot_install_apps as float)) index39,
(index40)/(cast (t2.tot_install_apps as float)) index40,
(index41)/(cast (t2.tot_install_apps as float)) index41,
(index42)/(cast (t2.tot_install_apps as float)) index42,
(index43)/(cast (t2.tot_install_apps as float)) index43,
(index44)/(cast (t2.tot_install_apps as float)) index44,
(index45)/(cast (t2.tot_install_apps as float)) index45,
(index46)/(cast (t2.tot_install_apps as float)) index46,
(index47)/(cast (t2.tot_install_apps as float)) index47,
(index48)/(cast (t2.tot_install_apps as float)) index48,
(index49)/(cast (t2.tot_install_apps as float)) index49,
(index50)/(cast (t2.tot_install_apps as float)) index50,
(index51)/(cast (t2.tot_install_apps as float)) index51,
(index52)/(cast (t2.tot_install_apps as float)) index52,
(index53)/(cast (t2.tot_install_apps as float)) index53,
(index54)/(cast (t2.tot_install_apps as float)) index54,
(index55)/(cast (t2.tot_install_apps as float)) index55,
(index56)/(cast (t2.tot_install_apps as float)) index56,
(index57)/(cast (t2.tot_install_apps as float)) index57,
(index58)/(cast (t2.tot_install_apps as float)) index58,
(index59)/(cast (t2.tot_install_apps as float)) index59,
(index60)/(cast (t2.tot_install_apps as float)) index60,
(index61)/(cast (t2.tot_install_apps as float)) index61,
(index62)/(cast (t2.tot_install_apps as float)) index62,
(index63)/(cast (t2.tot_install_apps as float)) index63,
(index64)/(cast (t2.tot_install_apps as float)) index64,
(index65)/(cast (t2.tot_install_apps as float)) index65,
(index66)/(cast (t2.tot_install_apps as float)) index66,
(index67)/(cast (t2.tot_install_apps as float)) index67,
(index68)/(cast (t2.tot_install_apps as float)) index68,
(index69)/(cast (t2.tot_install_apps as float)) index69,
(index70)/(cast (t2.tot_install_apps as float)) index70,
(index71)/(cast (t2.tot_install_apps as float)) index71,
(index72)/(cast (t2.tot_install_apps as float)) index72,
(index73)/(cast (t2.tot_install_apps as float)) index73,
(index74)/(cast (t2.tot_install_apps as float)) index74,
(index75)/(cast (t2.tot_install_apps as float)) index75,
(index76)/(cast (t2.tot_install_apps as float)) index76,
(index77)/(cast (t2.tot_install_apps as float)) index77,
(index78)/(cast (t2.tot_install_apps as float)) index78,
(index79)/(cast (t2.tot_install_apps as float)) index79,
(index80)/(cast (t2.tot_install_apps as float)) index80,
(index81)/(cast (t2.tot_install_apps as float)) index81,
(index82)/(cast (t2.tot_install_apps as float)) index82,
(index83)/(cast (t2.tot_install_apps as float)) index83,
(index84)/(cast (t2.tot_install_apps as float)) index84,
(index85)/(cast (t2.tot_install_apps as float)) index85,
(index86)/(cast (t2.tot_install_apps as float)) index86,
(index87)/(cast (t2.tot_install_apps as float)) index87,
(index88)/(cast (t2.tot_install_apps as float)) index88,
(index89)/(cast (t2.tot_install_apps as float)) index89,
(index90)/(cast (t2.tot_install_apps as float)) index90,
(index91)/(cast (t2.tot_install_apps as float)) index91,
(index92)/(cast (t2.tot_install_apps as float)) index92,
(index93)/(cast (t2.tot_install_apps as float)) index93,
(index94)/(cast (t2.tot_install_apps as float)) index94,
(index95)/(cast (t2.tot_install_apps as float)) index95,
(index96)/(cast (t2.tot_install_apps as float)) index96,
(index97)/(cast (t2.tot_install_apps as float)) index97,
(index98)/(cast (t2.tot_install_apps as float)) index98,
(index99)/(cast (t2.tot_install_apps as float)) index99,
(index100)/(cast (t2.tot_install_apps as float)) index100,
(index101)/(cast (t2.tot_install_apps as float)) index101,
(index102)/(cast (t2.tot_install_apps as float)) index102,
(index103)/(cast (t2.tot_install_apps as float)) index103,
(index104)/(cast (t2.tot_install_apps as float)) index104,
(index105)/(cast (t2.tot_install_apps as float)) index105,
(index106)/(cast (t2.tot_install_apps as float)) index106,
(index107)/(cast (t2.tot_install_apps as float)) index107,
(index108)/(cast (t2.tot_install_apps as float)) index108,
(index109)/(cast (t2.tot_install_apps as float)) index109,
(index110)/(cast (t2.tot_install_apps as float)) index110,
(index111)/(cast (t2.tot_install_apps as float)) index111,
(index112)/(cast (t2.tot_install_apps as float)) index112,
(index113)/(cast (t2.tot_install_apps as float)) index113,
(index114)/(cast (t2.tot_install_apps as float)) index114,
(index115)/(cast (t2.tot_install_apps as float)) index115,
(index116)/(cast (t2.tot_install_apps as float)) index116,
(index117)/(cast (t2.tot_install_apps as float)) index117,
(index118)/(cast (t2.tot_install_apps as float)) index118,
(index119)/(cast (t2.tot_install_apps as float)) index119,
(index120)/(cast (t2.tot_install_apps as float)) index120,
(index121)/(cast (t2.tot_install_apps as float)) index121,
(index122)/(cast (t2.tot_install_apps as float)) index122,
(index123)/(cast (t2.tot_install_apps as float)) index123,
(index124)/(cast (t2.tot_install_apps as float)) index124,
(index125)/(cast (t2.tot_install_apps as float)) index125,
(index126)/(cast (t2.tot_install_apps as float)) index126,
(index127)/(cast (t2.tot_install_apps as float)) index127,
(index128)/(cast (t2.tot_install_apps as float)) index128,
(index129)/(cast (t2.tot_install_apps as float)) index129,
(index130)/(cast (t2.tot_install_apps as float)) index130,
(index131)/(cast (t2.tot_install_apps as float)) index131,
(index132)/(cast (t2.tot_install_apps as float)) index132,
(index133)/(cast (t2.tot_install_apps as float)) index133,
(index134)/(cast (t2.tot_install_apps as float)) index134,
(index135)/(cast (t2.tot_install_apps as float)) index135,
(index136)/(cast (t2.tot_install_apps as float)) index136,
(index137)/(cast (t2.tot_install_apps as float)) index137,
(index138)/(cast (t2.tot_install_apps as float)) index138,
(index139)/(cast (t2.tot_install_apps as float)) index139,
(index140)/(cast (t2.tot_install_apps as float)) index140,
(index141)/(cast (t2.tot_install_apps as float)) index141,
(index142)/(cast (t2.tot_install_apps as float)) index142,
(index143)/(cast (t2.tot_install_apps as float)) index143,
(index144)/(cast (t2.tot_install_apps as float)) index144,
(index145)/(cast (t2.tot_install_apps as float)) index145,
(index146)/(cast (t2.tot_install_apps as float)) index146,
(index147)/(cast (t2.tot_install_apps as float)) index147,
(index148)/(cast (t2.tot_install_apps as float)) index148,
(index149)/(cast (t2.tot_install_apps as float)) index149,
(index150)/(cast (t2.tot_install_apps as float)) index150,
(index151)/(cast (t2.tot_install_apps as float)) index151,
(index152)/(cast (t2.tot_install_apps as float)) index152,
(index153)/(cast (t2.tot_install_apps as float)) index153,
(index154)/(cast (t2.tot_install_apps as float)) index154,
(index155)/(cast (t2.tot_install_apps as float)) index155,
(index156)/(cast (t2.tot_install_apps as float)) index156,
(index157)/(cast (t2.tot_install_apps as float)) index157,
(index158)/(cast (t2.tot_install_apps as float)) index158,
(index159)/(cast (t2.tot_install_apps as float)) index159,
(index160)/(cast (t2.tot_install_apps as float)) index160,
(index161)/(cast (t2.tot_install_apps as float)) index161,
(index162)/(cast (t2.tot_install_apps as float)) index162,
(index163)/(cast (t2.tot_install_apps as float)) index163,
(index164)/(cast (t2.tot_install_apps as float)) index164,
(index165)/(cast (t2.tot_install_apps as float)) index165,
(index166)/(cast (t2.tot_install_apps as float)) index166,
(index167)/(cast (t2.tot_install_apps as float)) index167,
(index168)/(cast (t2.tot_install_apps as float)) index168,
(index169)/(cast (t2.tot_install_apps as float)) index169,
(index170)/(cast (t2.tot_install_apps as float)) index170,
(index171)/(cast (t2.tot_install_apps as float)) index171,
(index172)/(cast (t2.tot_install_apps as float)) index172,
(index173)/(cast (t2.tot_install_apps as float)) index173,
(index174)/(cast (t2.tot_install_apps as float)) index174,
(index175)/(cast (t2.tot_install_apps as float)) index175,
(index176)/(cast (t2.tot_install_apps as float)) index176,
(index177)/(cast (t2.tot_install_apps as float)) index177,
(index178)/(cast (t2.tot_install_apps as float)) index178,
(index179)/(cast (t2.tot_install_apps as float)) index179,
(index180)/(cast (t2.tot_install_apps as float)) index180,
(index181)/(cast (t2.tot_install_apps as float)) index181,
(index182)/(cast (t2.tot_install_apps as float)) index182,
(index183)/(cast (t2.tot_install_apps as float)) index183,
(index184)/(cast (t2.tot_install_apps as float)) index184,
(index185)/(cast (t2.tot_install_apps as float)) index185,
(index186)/(cast (t2.tot_install_apps as float)) index186,
(index187)/(cast (t2.tot_install_apps as float)) index187,
(index188)/(cast (t2.tot_install_apps as float)) index188,
(index189)/(cast (t2.tot_install_apps as float)) index189,
(index190)/(cast (t2.tot_install_apps as float)) index190,
(index191)/(cast (t2.tot_install_apps as float)) index191,
(index192)/(cast (t2.tot_install_apps as float)) index192,
(index193)/(cast (t2.tot_install_apps as float)) index193,
(index194)/(cast (t2.tot_install_apps as float)) index194,
(index195)/(cast (t2.tot_install_apps as float)) index195,
(index196)/(cast (t2.tot_install_apps as float)) index196,
(index197)/(cast (t2.tot_install_apps as float)) index197,
(index198)/(cast (t2.tot_install_apps as float)) index198,
(index199)/(cast (t2.tot_install_apps as float)) index199,
(index200)/(cast (t2.tot_install_apps as float)) index200,
(index201)/(cast (t2.tot_install_apps as float)) index201,
(index202)/(cast (t2.tot_install_apps as float)) index202,
(index203)/(cast (t2.tot_install_apps as float)) index203,
(index204)/(cast (t2.tot_install_apps as float)) index204,
(index205)/(cast (t2.tot_install_apps as float)) index205,
(index206)/(cast (t2.tot_install_apps as float)) index206,
(index207)/(cast (t2.tot_install_apps as float)) index207,
(index208)/(cast (t2.tot_install_apps as float)) index208,
(index209)/(cast (t2.tot_install_apps as float)) index209,
(index210)/(cast (t2.tot_install_apps as float)) index210,
(index211)/(cast (t2.tot_install_apps as float)) index211,
(index212)/(cast (t2.tot_install_apps as float)) index212,
(index213)/(cast (t2.tot_install_apps as float)) index213,
(index214)/(cast (t2.tot_install_apps as float)) index214
from $gender_feature_v2_part2 t1 
join (select device,count(pkg) tot_install_apps from $dim_device_applist_new_di where day = '$day' group by device) t2
on t1.device=t2.device
where t1.day='$insertday';
"

#hive -e "alter table $gender_feature_v2_part4 drop partition(day<$p7);"
