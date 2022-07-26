#!/bin/bash

set -e -x

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day> "
    exit 1
fi

day=$1

function log_info(){

DATE_N=`date "+%Y-%m-%d %H:%M:%S"`
echo "$DATE_N $@"

}

log_info "run mobdi monitor"

log_info "monitor models_logic of $day  start"
log_info "monitor full of $day  start"
 ./mobdi_monitor_run_model_logic.sh $day 5
 ./mobdi_monitor_run_full.sh $day 5


log_info "monitor models_logic of $day over"

log_info "monitor full of $day over"
log_info "start delete history data"
last30day=`date -d"$day 30 days ago" +%Y%m%d`
mysql -h10.21.33.28 -u root -p'mobtech2019java' -P3306 -e "delete from mobdi_monitor.full_fields_monitor where stats_date<$last30day"
log_info "delete history data over"
