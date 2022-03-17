#!/bin/bash
set -x -e
hive -e "
select count(*) from (
select ieid,count(distinct mid) cnt from dm_mid_master.dwd_all_id_detail where day='all' and coalesce(ieid,'')<>'' group by ieid
) t where cnt >1
"
echo "========================="
hive -e "
select count(*) from (
select oiid,factory,count(distinct mid) cnt from dm_mid_master.dwd_all_id_detail where day='all' and coalesce(oiid,'')<>'' group by oiid,factory
) t where cnt >1
"
echo "========================="


hive -e "select count(*) from (select old_id from dm_mid_master.all_graph_result group by old_id) t"

hive -e "select count(*) from (select new_id from dm_mid_master.all_graph_result group by new_id) t"

hive -e "select new_id,count(*) cnt from dm_mid_master.all_graph_result group by new_id order by cnt desc limit 20"
