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
#device_applist_new="dm_mobdi_mapping.device_applist_new"

#app_category_mapping_par="dm_sdk_mapping.app_category_mapping_par"

#gender_app_cate_index2="dm_sdk_mapping.gender_app_cate_index2"

gender_feature_v2_part2="${dm_mobdi_tmp}.gender_feature_v2_part2"

hive -e "
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
insert overwrite table $gender_feature_v2_part2 partition(day='$insertday')
select device, 
max(index1) index1,
max(index2) index2,
max(index3) index3,
max(index4) index4,
max(index5) index5,
max(index6) index6,
max(index7) index7,
max(index8) index8,
max(index9) index9,
max(index10) index10,
max(index11) index11,
max(index12) index12,
max(index13) index13,
max(index14) index14,
max(index15) index15,
max(index16) index16,
max(index17) index17,
max(index18) index18,
max(index19) index19,
max(index20) index20,
max(index21) index21,
max(index22) index22,
max(index23) index23,
max(index24) index24,
max(index25) index25,
max(index26) index26,
max(index27) index27,
max(index28) index28,
max(index29) index29,
max(index30) index30,
max(index31) index31,
max(index32) index32,
max(index33) index33,
max(index34) index34,
max(index35) index35,
max(index36) index36,
max(index37) index37,
max(index38) index38,
max(index39) index39,
max(index40) index40,
max(index41) index41,
max(index42) index42,
max(index43) index43,
max(index44) index44,
max(index45) index45,
max(index46) index46,
max(index47) index47,
max(index48) index48,
max(index49) index49,
max(index50) index50,
max(index51) index51,
max(index52) index52,
max(index53) index53,
max(index54) index54,
max(index55) index55,
max(index56) index56,
max(index57) index57,
max(index58) index58,
max(index59) index59,
max(index60) index60,
max(index61) index61,
max(index62) index62,
max(index63) index63,
max(index64) index64,
max(index65) index65,
max(index66) index66,
max(index67) index67,
max(index68) index68,
max(index69) index69,
max(index70) index70,
max(index71) index71,
max(index72) index72,
max(index73) index73,
max(index74) index74,
max(index75) index75,
max(index76) index76,
max(index77) index77,
max(index78) index78,
max(index79) index79,
max(index80) index80,
max(index81) index81,
max(index82) index82,
max(index83) index83,
max(index84) index84,
max(index85) index85,
max(index86) index86,
max(index87) index87,
max(index88) index88,
max(index89) index89,
max(index90) index90,
max(index91) index91,
max(index92) index92,
max(index93) index93,
max(index94) index94,
max(index95) index95,
max(index96) index96,
max(index97) index97,
max(index98) index98,
max(index99) index99,
max(index100) index100,
max(index101) index101,
max(index102) index102,
max(index103) index103,
max(index104) index104,
max(index105) index105,
max(index106) index106,
max(index107) index107,
max(index108) index108,
max(index109) index109,
max(index110) index110,
max(index111) index111,
max(index112) index112,
max(index113) index113,
max(index114) index114,
max(index115) index115,
max(index116) index116,
max(index117) index117,
max(index118) index118,
max(index119) index119,
max(index120) index120,
max(index121) index121,
max(index122) index122,
max(index123) index123,
max(index124) index124,
max(index125) index125,
max(index126) index126,
max(index127) index127,
max(index128) index128,
max(index129) index129,
max(index130) index130,
max(index131) index131,
max(index132) index132,
max(index133) index133,
max(index134) index134,
max(index135) index135,
max(index136) index136,
max(index137) index137,
max(index138) index138,
max(index139) index139,
max(index140) index140,
max(index141) index141,
max(index142) index142,
max(index143) index143,
max(index144) index144,
max(index145) index145,
max(index146) index146,
max(index147) index147,
max(index148) index148,
max(index149) index149,
max(index150) index150,
max(index151) index151,
max(index152) index152,
max(index153) index153,
max(index154) index154,
max(index155) index155,
max(index156) index156,
max(index157) index157,
max(index158) index158,
max(index159) index159,
max(index160) index160,
max(index161) index161,
max(index162) index162,
max(index163) index163,
max(index164) index164,
max(index165) index165,
max(index166) index166,
max(index167) index167,
max(index168) index168,
max(index169) index169,
max(index170) index170,
max(index171) index171,
max(index172) index172,
max(index173) index173,
max(index174) index174,
max(index175) index175,
max(index176) index176,
max(index177) index177,
max(index178) index178,
max(index179) index179,
max(index180) index180,
max(index181) index181,
max(index182) index182,
max(index183) index183,
max(index184) index184,
max(index185) index185,
max(index186) index186,
max(index187) index187,
max(index188) index188,
max(index189) index189,
max(index190) index190,
max(index191) index191,
max(index192) index192,
max(index193) index193,
max(index194) index194,
max(index195) index195,
max(index196) index196,
max(index197) index197,
max(index198) index198,
max(index199) index199,
max(index200) index200,
max(index201) index201,
max(index202) index202,
max(index203) index203,
max(index204) index204,
max(index205) index205,
max(index206) index206,
max(index207) index207,
max(index208) index208,
max(index209) index209,
max(index210) index210,
max(index211) index211,
max(index212) index212,
max(index213) index213,
max(index214) index214
from (
select device, 
case when index= 74 then cnt else 0 end index1,
case when index= 75 then cnt else 0 end index2,
case when index= 76 then cnt else 0 end index3,
case when index= 77 then cnt else 0 end index4,
case when index= 78 then cnt else 0 end index5,
case when index= 79 then cnt else 0 end index6,
case when index= 80 then cnt else 0 end index7,
case when index= 81 then cnt else 0 end index8,
case when index= 82 then cnt else 0 end index9,
case when index= 83 then cnt else 0 end index10,
case when index= 84 then cnt else 0 end index11,
case when index= 85 then cnt else 0 end index12,
case when index= 86 then cnt else 0 end index13,
case when index= 87 then cnt else 0 end index14,
case when index= 88 then cnt else 0 end index15,
case when index= 89 then cnt else 0 end index16,
case when index= 90 then cnt else 0 end index17,
case when index= 91 then cnt else 0 end index18,
case when index= 92 then cnt else 0 end index19,
case when index= 93 then cnt else 0 end index20,
case when index= 94 then cnt else 0 end index21,
case when index= 95 then cnt else 0 end index22,
case when index= 96 then cnt else 0 end index23,
case when index= 97 then cnt else 0 end index24,
case when index= 98 then cnt else 0 end index25,
case when index= 99 then cnt else 0 end index26,
case when index= 100 then cnt else 0 end index27,
case when index= 101 then cnt else 0 end index28,
case when index= 102 then cnt else 0 end index29,
case when index= 103 then cnt else 0 end index30,
case when index= 104 then cnt else 0 end index31,
case when index= 105 then cnt else 0 end index32,
case when index= 106 then cnt else 0 end index33,
case when index= 107 then cnt else 0 end index34,
case when index= 108 then cnt else 0 end index35,
case when index= 109 then cnt else 0 end index36,
case when index= 110 then cnt else 0 end index37,
case when index= 111 then cnt else 0 end index38,
case when index= 112 then cnt else 0 end index39,
case when index= 113 then cnt else 0 end index40,
case when index= 114 then cnt else 0 end index41,
case when index= 115 then cnt else 0 end index42,
case when index= 116 then cnt else 0 end index43,
case when index= 117 then cnt else 0 end index44,
case when index= 118 then cnt else 0 end index45,
case when index= 119 then cnt else 0 end index46,
case when index= 120 then cnt else 0 end index47,
case when index= 121 then cnt else 0 end index48,
case when index= 122 then cnt else 0 end index49,
case when index= 123 then cnt else 0 end index50,
case when index= 124 then cnt else 0 end index51,
case when index= 125 then cnt else 0 end index52,
case when index= 126 then cnt else 0 end index53,
case when index= 127 then cnt else 0 end index54,
case when index= 128 then cnt else 0 end index55,
case when index= 129 then cnt else 0 end index56,
case when index= 130 then cnt else 0 end index57,
case when index= 131 then cnt else 0 end index58,
case when index= 132 then cnt else 0 end index59,
case when index= 133 then cnt else 0 end index60,
case when index= 134 then cnt else 0 end index61,
case when index= 135 then cnt else 0 end index62,
case when index= 136 then cnt else 0 end index63,
case when index= 137 then cnt else 0 end index64,
case when index= 138 then cnt else 0 end index65,
case when index= 139 then cnt else 0 end index66,
case when index= 140 then cnt else 0 end index67,
case when index= 141 then cnt else 0 end index68,
case when index= 142 then cnt else 0 end index69,
case when index= 143 then cnt else 0 end index70,
case when index= 144 then cnt else 0 end index71,
case when index= 145 then cnt else 0 end index72,
case when index= 146 then cnt else 0 end index73,
case when index= 147 then cnt else 0 end index74,
case when index= 148 then cnt else 0 end index75,
case when index= 149 then cnt else 0 end index76,
case when index= 150 then cnt else 0 end index77,
case when index= 151 then cnt else 0 end index78,
case when index= 152 then cnt else 0 end index79,
case when index= 153 then cnt else 0 end index80,
case when index= 154 then cnt else 0 end index81,
case when index= 155 then cnt else 0 end index82,
case when index= 156 then cnt else 0 end index83,
case when index= 157 then cnt else 0 end index84,
case when index= 158 then cnt else 0 end index85,
case when index= 159 then cnt else 0 end index86,
case when index= 160 then cnt else 0 end index87,
case when index= 161 then cnt else 0 end index88,
case when index= 162 then cnt else 0 end index89,
case when index= 163 then cnt else 0 end index90,
case when index= 164 then cnt else 0 end index91,
case when index= 165 then cnt else 0 end index92,
case when index= 166 then cnt else 0 end index93,
case when index= 167 then cnt else 0 end index94,
case when index= 168 then cnt else 0 end index95,
case when index= 169 then cnt else 0 end index96,
case when index= 170 then cnt else 0 end index97,
case when index= 171 then cnt else 0 end index98,
case when index= 172 then cnt else 0 end index99,
case when index= 173 then cnt else 0 end index100,
case when index= 174 then cnt else 0 end index101,
case when index= 175 then cnt else 0 end index102,
case when index= 176 then cnt else 0 end index103,
case when index= 177 then cnt else 0 end index104,
case when index= 178 then cnt else 0 end index105,
case when index= 179 then cnt else 0 end index106,
case when index= 180 then cnt else 0 end index107,
case when index= 181 then cnt else 0 end index108,
case when index= 182 then cnt else 0 end index109,
case when index= 183 then cnt else 0 end index110,
case when index= 184 then cnt else 0 end index111,
case when index= 185 then cnt else 0 end index112,
case when index= 186 then cnt else 0 end index113,
case when index= 187 then cnt else 0 end index114,
case when index= 188 then cnt else 0 end index115,
case when index= 189 then cnt else 0 end index116,
case when index= 190 then cnt else 0 end index117,
case when index= 191 then cnt else 0 end index118,
case when index= 192 then cnt else 0 end index119,
case when index= 193 then cnt else 0 end index120,
case when index= 194 then cnt else 0 end index121,
case when index= 195 then cnt else 0 end index122,
case when index= 196 then cnt else 0 end index123,
case when index= 197 then cnt else 0 end index124,
case when index= 198 then cnt else 0 end index125,
case when index= 199 then cnt else 0 end index126,
case when index= 200 then cnt else 0 end index127,
case when index= 201 then cnt else 0 end index128,
case when index= 202 then cnt else 0 end index129,
case when index= 203 then cnt else 0 end index130,
case when index= 204 then cnt else 0 end index131,
case when index= 205 then cnt else 0 end index132,
case when index= 206 then cnt else 0 end index133,
case when index= 207 then cnt else 0 end index134,
case when index= 208 then cnt else 0 end index135,
case when index= 209 then cnt else 0 end index136,
case when index= 210 then cnt else 0 end index137,
case when index= 211 then cnt else 0 end index138,
case when index= 212 then cnt else 0 end index139,
case when index= 213 then cnt else 0 end index140,
case when index= 214 then cnt else 0 end index141,
case when index= 215 then cnt else 0 end index142,
case when index= 216 then cnt else 0 end index143,
case when index= 217 then cnt else 0 end index144,
case when index= 218 then cnt else 0 end index145,
case when index= 219 then cnt else 0 end index146,
case when index= 220 then cnt else 0 end index147,
case when index= 221 then cnt else 0 end index148,
case when index= 222 then cnt else 0 end index149,
case when index= 223 then cnt else 0 end index150,
case when index= 224 then cnt else 0 end index151,
case when index= 225 then cnt else 0 end index152,
case when index= 226 then cnt else 0 end index153,
case when index= 227 then cnt else 0 end index154,
case when index= 228 then cnt else 0 end index155,
case when index= 229 then cnt else 0 end index156,
case when index= 230 then cnt else 0 end index157,
case when index= 231 then cnt else 0 end index158,
case when index= 232 then cnt else 0 end index159,
case when index= 233 then cnt else 0 end index160,
case when index= 234 then cnt else 0 end index161,
case when index= 235 then cnt else 0 end index162,
case when index= 236 then cnt else 0 end index163,
case when index= 237 then cnt else 0 end index164,
case when index= 238 then cnt else 0 end index165,
case when index= 239 then cnt else 0 end index166,
case when index= 240 then cnt else 0 end index167,
case when index= 241 then cnt else 0 end index168,
case when index= 242 then cnt else 0 end index169,
case when index= 243 then cnt else 0 end index170,
case when index= 244 then cnt else 0 end index171,
case when index= 245 then cnt else 0 end index172,
case when index= 246 then cnt else 0 end index173,
case when index= 247 then cnt else 0 end index174,
case when index= 248 then cnt else 0 end index175,
case when index= 249 then cnt else 0 end index176,
case when index= 250 then cnt else 0 end index177,
case when index= 251 then cnt else 0 end index178,
case when index= 252 then cnt else 0 end index179,
case when index= 253 then cnt else 0 end index180,
case when index= 254 then cnt else 0 end index181,
case when index= 255 then cnt else 0 end index182,
case when index= 256 then cnt else 0 end index183,
case when index= 257 then cnt else 0 end index184,
case when index= 258 then cnt else 0 end index185,
case when index= 259 then cnt else 0 end index186,
case when index= 260 then cnt else 0 end index187,
case when index= 261 then cnt else 0 end index188,
case when index= 262 then cnt else 0 end index189,
case when index= 263 then cnt else 0 end index190,
case when index= 264 then cnt else 0 end index191,
case when index= 265 then cnt else 0 end index192,
case when index= 266 then cnt else 0 end index193,
case when index= 267 then cnt else 0 end index194,
case when index= 268 then cnt else 0 end index195,
case when index= 269 then cnt else 0 end index196,
case when index= 270 then cnt else 0 end index197,
case when index= 271 then cnt else 0 end index198,
case when index= 272 then cnt else 0 end index199,
case when index= 273 then cnt else 0 end index200,
case when index= 274 then cnt else 0 end index201,
case when index= 275 then cnt else 0 end index202,
case when index= 276 then cnt else 0 end index203,
case when index= 277 then cnt else 0 end index204,
case when index= 278 then cnt else 0 end index205,
case when index= 279 then cnt else 0 end index206,
case when index= 280 then cnt else 0 end index207,
case when index= 281 then cnt else 0 end index208,
case when index= 282 then cnt else 0 end index209,
case when index= 283 then cnt else 0 end index210,
case when index= 284 then cnt else 0 end index211,
case when index= 285 then cnt else 0 end index212,
case when index= 286 then cnt else 0 end index213,
case when index= 287 then cnt else 0 end index214
from (
select a.device,c.index,count(1) cnt from 
(select device,pkg app_split from $dim_device_applist_new_di where day = '$day') a
join
(select apppkg,cate_l2_id from $dim_app_category_mapping_par where version='1000' group by apppkg,cate_l2_id) b
on a.app_split=b.apppkg
join 
(select cate_l2_id, index from $dim_gender_app_cate_index2) c
on b.cate_l2_id=c.cate_l2_id
group by a.device, c.index
)d
)e
group by device;
"