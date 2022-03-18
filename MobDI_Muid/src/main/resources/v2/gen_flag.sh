#!/bin/bash
set -x -e

tmp_db=dm_mid_master

id_source="$tmp_db.dwd_all_id_detail"

hive -e "
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.support.quoted.identifiers=None;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
SET hive.exec.parallel=true;
set hive.mapjoin.smalltable.filesize=500000000;

create temporary table mobdi_test.black_ieid as
select ieid,count(distinct mid) cnt,1 ieid_flag
from $id_source where day='all' and coalesce(ieid,'')<>''
group by ieid having count(distinct mid) > 1;

create temporary table mobdi_test.black_oiid as
select oiid,factory,count(distinct mid) cnt,2 oiid_flag
from $id_source where day='all' and coalesce(oiid,'')<>''
group by oiid,factory having count(distinct mid) > 1;

create temporary table mobdi_test.black_mid_ieid as
select mid,count(distinct ieid) cnt,4 mid_ieid_flag
from $id_source where day='all' and coalesce(ieid,'')<>''
group by mid having count(distinct ieid) > 3;

create temporary table mobdi_test.black_mid_oiid as
select mid,count(distinct oiid) cnt,8 mid_oiid_flag
from $id_source where day='all' and coalesce(oiid,'')<>''
group by mid having count(distinct oiid) > 2;

insert overwrite table $id_source partition(day='all')
select a.duid,a.oiid,a.ieid,a.factory,model,unid,a.mid,
coalesce(ieid_flag,0) + coalesce(oiid_flag,0) + coalesce(mid_ieid_flag,0) + coalesce(mid_oiid_flag,0) as  flag
from $id_source a
left join mobdi_test.black_ieid b on a.ieid=b.ieid
left join mobdi_test.black_oiid c on a.oiid=c.oiid and a.factory=c.factory
left join mobdi_test.black_mid_ieid e on a.mid=e.mid
left join mobdi_test.black_mid_oiid f on a.mid=f.mid
where day='all'
"