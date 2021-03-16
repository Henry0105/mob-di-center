package com.youzu.mob.utils

import com.typesafe.config.ConfigFactory

object PropUtils {

  private val propList = Array("application.properties", "hive_db_tb_dashboard.properties",
    "hive_db_tb_master.properties", "hive_db_tb_mobdi_mapping.properties", "hive_db_tb_report.properties",
    "hive_db_tb_sdk_mapping.properties", "hive_db_tb_topic.properties"
  )

  lazy private val prop = propList.map(ConfigFactory.load).reduce((a, b) => a.withFallback(b))

  def getProperty(key: String): String = prop.getString(key)

}
