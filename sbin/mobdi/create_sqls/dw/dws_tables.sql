use dm_mobdi_topic;
create table  dm_mobdi_topic.dws_device_info_full like dm_mobdi_master.dwd_device_info_df;
create table  dm_mobdi_topic.dws_device_info_di like dm_mobdi_master.device_info_master_incr;