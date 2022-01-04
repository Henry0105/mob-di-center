package com.youzu.mob.utils

object Constants {

  private def prop(key: String): String = PropUtils.getProperty(key)
  lazy val DM_MOBDI_TMP = prop("dw_mobdi_md")
  lazy val TP_SDK_TMP = prop("tp_sdk_tmp")

  lazy val DWS_DEVICE_INSTALL_APP_RE_STATUS_DI = prop("dws_device_install_app_re_status_di")
  lazy val DWS_DEVICE_DUID_MAPPING_NEW = prop("dws_device_duid_mapping_new")

  lazy val PHONE_CONTACTS_DEDUP_FULL = prop("phone_contacts_dedup_full")

  lazy val IOS_ID_MAPPING_FULL_VIEW = prop("dim_id_mapping_ios_df_view")

  // 原表是rp_sdk_dmp.rp_device_profile_full,该表已不存在
  lazy val RP_DEVICE_PROFILE_FULL = prop("rp_device_profile_full")

  lazy val OFFLINE_ESTI_CONFIG = prop("dim_offline_esti_config")
  lazy val DWS_DEVICE_IP_INFO_DI = prop("dws_device_ip_info_di")
  lazy val MAP_CITY_SDK = prop("dim_map_city_sdk")
  lazy val MOBPUSH_UNSTALL_ANALYSIS_TMP = prop("mobpush_unstall_analysis_tmp")
  lazy val IOS_PERMANENT_PLACE_SEC = prop("ios_permanent_place_sec")
  lazy val IOS_ID_MAPPING_FULL_SEC_VIEW = prop("dim_id_mapping_ios_sec_df_view")

  lazy val SDK_LBS_DAILY_POI_IOS = prop("dws_ifid_lbs_poi_ios_sec_di")

  lazy val DWS_IFID_LBS_POI_IOS_SEC_DI = prop("dws_ifid_lbs_poi_ios_sec_di")
  lazy val PHONE_ONEDEGREE_REL = prop("phone_onedegree_rel")
  lazy val TIMEWINDOW_OFFLINE_PROFILE_IOS_SEC = prop("timewindow_offline_profile_ios_sec")
  lazy val TIMEWINDOW_OFFLINE_PROFILE_IOS = prop("timewindow_offline_profile_ios")
  lazy val APP_PKG_MAPPING_PAR = prop("dim_app_pkg_mapping_par")
  lazy val RP_DEVICE_PROFILE_FULL_VIEW = prop("rp_device_profile_full_view")
  lazy val APP_ACTIVE_DAILY = prop("app_active_daily")
  lazy val DWS_DEVICE_LBS_POI_ANDROID_SEC_DI = prop("dws_device_lbs_poi_android_sec_di")
  lazy val IOS_PERMANENT_PLACE = prop("ios_permanent_place")
  lazy val DWS_DEVICE_TRAVEL_LOCATION_DI= prop("dws_device_travel_location_di")
  lazy val MAP_COUNTRY_SDK = prop("dim_map_country_sdk")
  lazy val MAP_PROVINCE_LOC = prop("dim_map_province_loc")
  lazy val VACATION_FLAG = prop("dim_vacation_flag_par")
  lazy val INDEX_PROFILE_HISTORY_ALL = prop("index_profile_history_all")
  lazy val DWS_DEVICE_INSTALL_STATUS = prop("dws_device_install_status")
  lazy val DWD_LOG_DEVICE_UNSTALL_APP_INFO_SEC_DI = prop("dwd_log_device_unstall_app_info_sec_di")
  lazy val DWD_LOG_DEVICE_INSTALL_APP_INCR_INFO_SEC_DI = prop("dwd_log_device_install_app_incr_info_sec_di")
  lazy val DWD_LOG_DEVICE_INSTALL_APP_ALL_INFO_SEC_DI = prop("dwd_log_device_install_app_all_info_sec_di")
  lazy val DWS_DEVICE_APP_INSTALL_DI = prop("dws_device_app_install_di")
  lazy val ONLINE_CATEGORY_MAPPING = prop("dim_online_category_mapping")
  lazy val DWD_LOG_SHARE_NEW_DI = prop("dwd_log_share_new_di")
  lazy val DIM_SHARE_LEVEL_MAPPING = prop("dim_share_level_mapping")
  lazy val ADS_SHARE_LABEL_MONTHLY = prop("ads_share_label_monthly")
  lazy val TIMEWINDOW_ONLINE_PROFILE = prop("timewindow_online_profile")
  lazy val TIMEWINDOW_OFFLINE_PROFILE = prop("timewindow_offline_profile")
  lazy val GEOHASH6_AREA_MAPPING = prop("dim_geohash6_china_area_mapping_par")
  lazy val GEOHASH8_LBS_INFO_MAPPING = prop("dim_geohash8_china_area_mapping_par")
  lazy val DEVICE_PROFILE_LABEL_FULL_PAR = prop("device_profile_label_full_par")
  lazy val ADS_PPX_SCORE_WEEKLY = prop("ads_ppx_score_weekly")
  lazy val DIM_PPX_APP_MAPPING = prop("dim_ppx_app_mapping")
  lazy val RP_GAPOI_MYPHONE_PHONE = prop("rp_gapoi_myphone_phone")
  lazy val DEVICE_LOCATION_CURRENT = prop("dws_device_location_current_di")
  lazy val RP_DEVICE_LOCATION_PERMANENT = prop("rp_device_location_permanent")

  lazy val PHONE_LABEL = prop("phone_label")

  lazy val DWS_DEVICE_APP_INFO_DF = prop("dws_device_app_info_df")
  lazy val DIM_ID_MAPPING_ANDROID_DF_VIEW = prop("id_mapping_android_df_view")

  lazy val DWS_DEVICE_CATERING_DINEIN_DI = prop("dws_device_catering_dinein_di")
  lazy val DWS_DEVICE_LBS_POI_10TYPE_DI = prop("dws_device_lbs_poi_10type_di")
  lazy val DM_DEVICE_APPLIST_FULL = prop("dm_device_applist_full")
  lazy val DM_DEVICE_APPLIST_INCR = prop("dm_device_applist_incr")

}
