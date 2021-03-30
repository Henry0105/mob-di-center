#!/bin/bash
set -x -e

: '
@owner:liuyanqiang
@describe:因为有的业务线还在使用，每天同步这些表的数据
@projectName:MOBDI
@BusinessName:profile
'

if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>'"
     exit 1
fi

day=$1

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

tmpdb="dw_mobdi_tmp"
appdb="rp_mobdi_report"

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
insert overwrite table $appdb.device_mapping_label_incr partition(day='${day}')
select device,carrier,cell_factory,sysver,model,model_level,screensize,breaked,public_date,network,country,province,city,
       country_cn,province_cn,city_cn,city_level,city_level_1001,identity,price
from $appdb.label_mapping_type_all_di
where day='${day}';

insert overwrite table $appdb.device_model_label_incr partition(day='${day}')
select device,gender,agebin,car,married,edu,income,house,kids,occupation,industry,life_stage,special_time,consum_level,
       agebin_1001,tag_list,repayment,segment,income_1001,occupation_1001,consume_level
from $appdb.label_model_type_all_di
where day='${day}';

insert overwrite table $appdb.device_statics_label_incr partition(day='${day}')
select device,applist,tot_install_apps,nationality,nationality_cn,group_list,catelist,processtime
from $appdb.label_statics_type_all_di
where day='${day}';
"
