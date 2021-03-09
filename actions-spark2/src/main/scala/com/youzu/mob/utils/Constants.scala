package com.youzu.mob.utils

object Constants {

  private def prop(key: String): String = PropUtils.getProperty(key)
  val DM_MOBDI_TMP = prop("dw_mobdi_md")
  val DWS_DEVICE_INSTALL_APP_RE_STATUS_DI = prop("dws_device_install_app_re_status_di")
  val DWS_DEVICE_DUID_MAPPING_NEW = prop("dws_device_duid_mapping_new")
  val PHONE_CONTACTS_DEDUP_FULL = prop("phone_contacts_dedup_full")
  val IOS_ID_MAPPING_FULL_VIEW = prop("ios_id_mapping_full_view")
  val RP_DEVICE_PROFILE_FULL = prop("rp_device_profile_full")
  val OFFLINE_ESTI_CONFIG = prop("offline_esti_config")
  val DWS_DEVICE_IP_INFO_DI = prop("dws_device_ip_info_di")
  val MAP_CITY_SDK = prop("map_city_sdk")
  val MOBPUSH_UNSTALL_ANALYSIS_TMP = prop("mobpush_unstall_analysis_tmp")
  val IOS_PERMANENT_PLACE_SEC = prop("ios_permanent_place_sec")
  val IOS_ID_MAPPING_FULL_SEC_VIEW = prop("ios_id_mapping_full_sec_view")
  val SDK_LBS_DAILY_POI_IOS = prop("sdk_lbs_daily_poi_ios")
  val DWS_IFID_LBS_POI_IOS_SEC_DI = prop("dws_ifid_lbs_poi_ios_sec_di")
  val PHONE_ONEDEGREE_REL = prop("phone_onedegree_rel")
  val TIMEWINDOW_OFFLINE_PROFILE_IOS_SEC = prop("timewindow_offline_profile_ios_sec")
  val TIMEWINDOW_OFFLINE_PROFILE_IOS = prop("timewindow_offline_profile_ios")
  val APP_PKG_MAPPING_PAR = prop("app_pkg_mapping_par")
  val RP_DEVICE_PROFILE_FULL_VIEW = prop("rp_device_profile_full_view")
  val APP_ACTIVE_DAILY = prop("app_active_daily")
  val DWS_DEVICE_LBS_POI_ANDROID_SEC_DI = prop("dws_device_lbs_poi_android_sec_di")
  val IOS_PERMANENT_PLACE = prop("ios_permanent_place")
}
