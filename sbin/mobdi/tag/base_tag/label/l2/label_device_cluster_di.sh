#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 计算设备的device_cluster,使用kmeans算法对设备聚类
@attention: 这个表还是老的表直接拷贝一份过来，后面需要重写
@projectName:MOBDI
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

##input
cluster_incr=$device_cluster_incr
#fake_input="rp_mobdi_app.label_l1_taglist_di" ##代表其实有这个来源，目前还没有切换
##output
label_cluster_di=$label_l2_device_cluster_di

##目前只是简单的把数据拷贝过来，后面需要重写
hive -v -e "
insert overwrite table $label_cluster_di partition (day=$day)
select device,cluster
from $cluster_incr
where day=$day
"
