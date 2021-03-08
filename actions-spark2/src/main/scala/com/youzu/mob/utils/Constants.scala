package com.youzu.mob.utils

object Constants {

  private def prop(key: String): String = PropUtils.getProperty(key)

  // MasterReservedTime
  val  DEVICE_RESERVED_TIME_INCR = prop("dw_mobdi_md.device_reserved_time_incr")
  val  DWS_DEVICE_INSTALL_APP_RE_STATUS_DI = prop("dm_mobdi_topic.dws_device_install_app_re_status_di") // dm_mobdi_master.master_reserved_new"

  // PhoneContactsDedupFull
  val DWS_DEVICE_DUID_MAPPING_NEW = prop("dm_mobdi_topic.dws_device_duid_mapping_new") // dm_sdk_mapping.device_duid_mapping_new
  val PHONE_CONTACTS = prop("dw_mobdi_md.phone_contacts")
  val PHONE_CONTACTS_DEDUP_FULL = prop("dm_mobdi_master.phone_contacts_dedup_full")
  val IOS_ID_MAPPING_FULL_VIEW = prop("dm_mobdi_mapping.ios_id_mapping_full_view")

  // PhoneContactsWordSplit
  val PHONE_CONTACTS_INDEX_WORD_SPLIT_PREPARE = prop("dw_mobdi_md.phone_contacts_index_word_split_prepare")

}
