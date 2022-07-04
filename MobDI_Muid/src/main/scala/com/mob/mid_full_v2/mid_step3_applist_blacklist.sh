#!/usr/bin/env bash

tmpSourceTbl=dm_mid_master.pkg_it_duid_category_tmp_rid
rnid_ieid_blacklist=dm_mid_master.rnid_ieid_blacklist
# rnid 黑名单
# 单个rnid，同一个app一个版本安装10次以上

hive -e "
$sqlset

insert overwrite table $rnid_ieid_blacklist partition(type='month_install10')

select rnid from
( select rnid,pkg,version,count(distinct firstinstalltime) cnt
  from  $tmpSourceTbl
  where firstinstalltime not like '%000'
  group by rnid,pkg,version
  ) a where cnt > 10
;

insert overwrite table $rnid_ieid_blacklist partition(type='month_rnid600')
select rnid
from
( select rnid,pkg,version,firstinstalltime
 from $tmpSourceTbl
        where firstinstalltime not like '%000'
        group by rnid,pkg,version,firstinstalltime
        )tmp
    group by rnid
having count(1) > 600;

insert overwrite table $rnid_ieid_blacklist partition(type='month_pkg400')
select rnid from (
select rnid, count(distinct pkg ) pkg_cnt
   from  $tmpSourceTbl
    where  firstinstalltime not like '%000'
  group by rnid
) c where  pkg_cnt>400

;
"