#!/bin/bash

set -e -x

: '
@owner:hushk
@describe: 抽取模型代码
@projectName:MOBDI
@BusinessName:profile
'

# agebin_1004
export agebin_1004_sql_union=""
export agebin_1004_sql_join=""
export agebin_1004_sql_select="coalesce(full.agebin_1004,-1) as agebin_1004,coalesce(full.agebin_1004_cl,-1) as agebin_1004_cl"
function agebin_1004_sql() {
  day=$1
  dd=`date -d "$day" +%d`
  age_day=$(date -d "$day" +%Y%m01)
  age_day=$(date -d "$age_day -1 day" +%Y%m%d)
  if [ $dd -eq 10 ];then
    agebin_1004_sql_union="union all
        select device
        from $age_scoring_v4_result_di
        where day='$age_day'
        and device rlike '[a-f0-9]{40}'
        and device!='0000000000000000000000000000000000000000'
    "
    agebin_1004_sql_join="left join
    (
      select device,
      case when label=0 then 9 when label=1 then 8 when label=2 then 7 when label=3 then 6 when label>3 then 5 end as agebin_1004,
      maxpro agebin_1004_cl,day
      from $age_scoring_v4_result_di
      where day = '$age_day'
    ) agebin_1004_model on un.device=agebin_1004_model.device
    "
    agebin_1004_sql_select="
    case when full.agebin_1004_cl=1 then full.agebin_1004 else
     coalesce(agebin_1004_model.agebin_1004,full.agebin_1004,-1) end as agebin_1004,
    case when full.agebin_1004_cl=1 then full.agebin_1004_cl else
     coalesce(agebin_1004_model.agebin_1004_cl,full.agebin_1004_cl,-1) end as agebin_1004_cl
    "
  fi
}




# income_1002
export income_1002_sql_union=""
export income_1002_sql_join=""
export income_1002_sql_select=",coalesce(full.income_1002,-1) as income_1002,coalesce(full.income_1002_cl,-1) as income_1002_cl"
function income_1002_sql() {
  day=$1
  dd=`date -d "$day" +%d`
  income_day=$(date -d "$day" +%Y%m01)
  income_day=$(date -d "$income_day -1 day" +%Y%m%d)
  if [ $dd -eq 10 ];then
    income_1002_sql_union="union all
        select device
        from $income_scoring_v2_result_di
        where day='$income_day'
        and device rlike '[a-f0-9]{40}'
        and device!='0000000000000000000000000000000000000000'
    "
    income_1002_sql_join="left join
    (
      select
        device,label,max_prob,day
      from $income_scoring_v2_result_di
      where day = '$income_day'
    ) income_1002_model on un.device=income_1002_model.device
    "
    income_1002_sql_select=",
    (case
        when full.agebin_1004=9 then 3
        when full.city_level_1001=1 and full.edu=8 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.3 then 4
        when full.city_level_1001=1 and full.edu=8 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.8 and conv(substr(full.device, 0, 4), 16 ,10)/65535 >=0.3 then 5
        when full.city_level_1001=1 and full.edu=9 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.2 then 4
        when full.city_level_1001=1 and full.edu=9 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.8 and conv(substr(full.device, 0, 4), 16 ,10)/65535>=0.2 then 5
        when full.city_level_1001=2 and full.edu=8 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.2 then 4
        when full.city_level_1001=2 and full.edu=8 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.5 and conv(substr(full.device, 0, 4), 16 ,10)/65535 >=0.2 then 5
        when full.city_level_1001=2 and full.edu=9 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.1 then 4
        when full.city_level_1001=2 and full.edu=9 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.6 and conv(substr(full.device, 0, 4), 16 ,10)/65535>=0.1 then 5
        else -1
    end) as income_1002,
    (case
        when full.agebin_1004=9 then full.agebin_1004_cl
        when full.city_level_1001=1 and full.edu=8 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.3 then if(full.edu_cl/income_1002_model.max_prob>1,income_1002_model.max_prob,full.edu_cl)
        when full.city_level_1001=1 and full.edu=8 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.8 and conv(substr(full.device, 0, 4), 16 ,10)/65535 >= 0.3 then if(full.edu_cl/income_1002_model.max_prob>1,income_1002_model.max_prob,full.edu_cl)
        when full.city_level_1001=1 and full.edu=9 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.2 then if(full.edu_cl/income_1002_model.max_prob>1,income_1002_model.max_prob,full.edu_cl)
        when full.city_level_1001=1 and full.edu=9 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.8 and conv(substr(full.device, 0, 4), 16 ,10)/65535 >= 0.2 then if(full.edu_cl/income_1002_model.max_prob>1,income_1002_model.max_prob,full.edu_cl)
        when full.city_level_1001=2 and full.edu=8 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.2 then if(full.edu_cl/income_1002_model.max_prob>1,income_1002_model.max_prob,full.edu_cl)
        when full.city_level_1001=2 and full.edu=8 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.5 and conv(substr(full.device, 0, 4), 16 ,10)/65535 >= 0.2 then if(full.edu_cl/income_1002_model.max_prob>1,income_1002_model.max_prob,full.edu_cl)
        when full.city_level_1001=2 and full.edu=9 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.1 then if(full.edu_cl/income_1002_model.max_prob>1,income_1002_model.max_prob,full.edu_cl)
        when full.city_level_1001=2 and full.edu=9 and income_1002_model.label=0 and conv(substr(full.device, 0, 4), 16 ,10)/65535 < 0.6 and conv(substr(full.device, 0, 4), 16 ,10)/65535 >= 0.1 then if(full.edu_cl/income_1002_model.max_prob>1,income_1002_model.max_prob,full.edu_cl)
        else -1
    end) as income_1002_cl
    "
  fi
}
