package com.mob.deviceid.config

object HiveProps extends Serializable {
  val propUtils = new PropUtils("hive_database_table.properties")

  lazy val BASE_TABLE: String = getProperty("base_table")
  lazy val DEVICEID_NEW_IDS_MAPPING_FULL: String = getProperty("deviceid_new_ids_mapping_full")
  lazy val DEVICEID_NEW_IDS_MAPPING_INCR: String = getProperty("deviceid_new_ids_mapping_incr")
  lazy val DEVICEID_OLD_NEW_MAPPING: String = getProperty("deviceid_old_new_mapping")
  lazy val GEN_ID_TABLE: String = getProperty("gen_id_table")
  lazy val HEAD_FACTORY: String = getProperty("head_factory")
  lazy val SNID_BLACKLIST: String = getProperty("snid_blacklist")
  lazy val IEID_BLACKLIST: String = getProperty("ieid_blacklist")
  lazy val MCID_BLACKLIST: String = getProperty("mcid_blacklist")
  lazy val BLACKLIST_VIEW: String = getProperty("blacklist_view")
  lazy val SYSVER_MAPPING_PAR: String = getProperty("dm_sdk_mapping.sysver_mapping_par")

  lazy val MUID_IEID_MCID_MAPPING_INCR: String = getProperty("muid_ieid_mcid_mapping_incr")
  lazy val MUID_IEID_SNID_MAPPING_INCR: String = getProperty("muid_ieid_snid_mapping_incr")
  lazy val MUID_MCID_SNID_MAPPING_INCR: String = getProperty("muid_mcid_snid_mapping_incr")
  lazy val DW_MOBDI_MD: String = getProperty("dw_mobdi_md")
  lazy val MAPPING_FULL_PARTITION: String = getProperty("mapping_full_partition")
  lazy val MUID_MAPPING_INCR_PARTITION: String = getProperty("muid_mapping_incr_partition")

  private def getProperty: String => String = propUtils.getProperty
}
