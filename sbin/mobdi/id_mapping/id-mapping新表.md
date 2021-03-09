1. id_mapping_monthly.sh ===========> 建6张表   id_mapping需要使用到 dm_mobdi_topic.dws_device_snsuid_list_android
       新表                                                   原表
       dw_mobdi_tmp.tmp_device_snsuid_android
       dw_mobdi_tmp.tmp_device_snsuid_ios
       dm_mobdi_topic.dws_device_snsuid_list_android
       dm_mobdi_topic.dws_device_snsuid_list_ios
       dm_mobdi_topic.dws_device_snsuid_mi
       dm_mobdi_topic.dws_device_snsuid_mf
       
2. new_device_duid_mapping.sh  =========> 建一张表
        新表                                                  原表
        dm_mobdi_topic.dws_device_duid_mapping_new       
        
       


dw_sdk_log.log_device_info_jh
dw_sdk_log.log_device_info
dw_sdk_log.pv
dw_sdk_log.dcookie
dm_mobdi_master.dwd_log_share_new_di
dm_mobdi_master.dwd_log_oauth_new_di
dm_smssdk_master.log_device_phone_dedup
dw_sdk_log.mobauth_operator_login
dw_sdk_log.mobauth_pvlog

 
