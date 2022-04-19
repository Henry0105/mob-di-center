package com.mob.mid_incr_v2

import org.apache.spark.sql.{DataFrame, SparkSession}

object DuidSource {

  def main(args: Array[String]): Unit = {

    val day: String = args(0)
    val pday = args(1)

    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"Duid2Unid_$day")
      .enableHiveSupport()
      .getOrCreate()

    compute(spark, day, pday)

    spark.stop()

  }

  def compute(spark: SparkSession, day: String, pday: String): DataFrame = {

    //1.源头表载入
    spark.sql(s"DROP TABLE IF EXISTS dm_mid_master.tmp_mid_source_$day")
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dm_mid_master.tmp_mid_source_$day STORED AS ORC AS
         |SELECT duid
         |     , factory
         |     , model
         |     , pkg_it
         |     , ieid
         |     , oiid
         |FROM
         |(
         |  SELECT duid
         |       , factory
         |       , model
         |       , pkg_it
         |       , ieid
         |       , oiid
         |  FROM
         |  (
         |    SELECT duid
         |         , factory
         |         , model
         |         , IF(version IS NOT NULL
         |              AND TRIM(version) NOT IN ('','null','NULL','unknown','none','other','未知','na')
         |              AND firstinstalltime IS NOT NULL
         |              AND LENGTH(firstinstalltime) = 13
         |              AND SUBSTR(firstinstalltime,-3,3) <> '000'
         |              AND pkg = REGEXP_EXTRACT(pkg, '^[a-zA-Z]+[0-9a-zA-Z_]*(\\.[a-zA-Z]+[0-9a-zA-Z_]*)*', 0),
         |              CONCAT(pkg,'_',version,'_',firstinstalltime),
         |              '') AS pkg_it
         |         , ieid
         |         , oiid
         |    FROM dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di
         |    WHERE day = '$day'
         |    AND duid IS NOT NULL
         |    AND TRIM(duid) <> ''
         |    AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |  ) install_all
         |  GROUP BY duid,factory,model,pkg_it,ieid,oiid
         |
         |  UNION ALL
         |
         |  SELECT duid
         |       , factory
         |       , model
         |       , '' AS pkg_it
         |       , ieid
         |       , oiid
         |  FROM dm_mobdi_master.dwd_pv_sec_di
         |  WHERE day = '$day'
         |  AND duid IS NOT NULL
         |  AND TRIM(duid) <> ''
         |  AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |  GROUP BY duid,factory,model,ieid,oiid
         |
         |  UNION ALL
         |
         |  SELECT duid
         |       , factory
         |       , model
         |       , '' AS pkg_it
         |       , ieid
         |       , oiid
         |  FROM dm_mobdi_master.dwd_mdata_nginx_pv_di
         |  WHERE day = '$day'
         |  AND duid IS NOT NULL
         |  AND TRIM(duid) <> ''
         |  AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |  GROUP BY duid,factory,model,ieid,oiid
         |
         |  UNION ALL
         |
         |  SELECT duid
         |       , factory
         |       , model
         |       , pkg_it
         |       , ieid
         |       , oiid
         |  FROM
         |  (
         |    SELECT COALESCE(IF(LOWER(TRIM(curduid)) < 0, '', LOWER(TRIM(curduid))), LOWER(TRIM(id))) AS duid
         |         , factory
         |         , model
         |         , '' AS pkg_it
         |         , ieid
         |         , oiid
         |    FROM dm_mobdi_master.dwd_log_device_info_jh_sec_di
         |    WHERE day = '$day'
         |  )jh
         |  WHERE duid IS NOT NULL
         |  AND TRIM(duid) <> ''
         |  AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |  GROUP BY duid,factory,model,pkg_it,ieid,oiid
         |
         |  UNION ALL
         |
         |  SELECT duid
         |       , factory
         |       , model
         |       , '' AS pkg_it
         |       , ieid
         |       , oiid
         |  FROM dm_mobdi_master.dwd_location_info_sec_di
         |  WHERE day = '$day'
         |  AND duid IS NOT NULL
         |  AND TRIM(duid) <> ''
         |  AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |  GROUP BY duid,factory,model,ieid,oiid
         |
         |  UNION ALL
         |
         |  SELECT duid
         |       , factory
         |       , model
         |       , '' AS pkg_it
         |       , ieid
         |       , oiid
         |  FROM dm_mobdi_master.dwd_auto_location_info_sec_di
         |  WHERE day = '$day'
         |  AND duid IS NOT NULL
         |  AND TRIM(duid) <> ''
         |  AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |  GROUP BY duid,factory,model,ieid,oiid
         |
         |  UNION ALL
         |
         |  SELECT duid
         |       , factory
         |       , model
         |       , '' AS pkg_it
         |       , ieid
         |       , oiid
         |  FROM dm_mobdi_master.dwd_log_wifi_info_sec_di
         |  WHERE day = '$day'
         |  AND duid IS NOT NULL
         |  AND TRIM(duid) <> ''
         |  AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |  GROUP BY duid,factory,model,ieid,oiid
         |
         |  UNION ALL
         |
         |  SELECT duid
         |       , factory
         |       , model
         |       , '' AS pkg_it
         |       , ieid
         |       , oiid
         |  FROM dm_mobdi_master.dwd_base_station_info_sec_di
         |  WHERE day = '$day'
         |  AND duid IS NOT NULL
         |  AND TRIM(duid) <> ''
         |  AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |  GROUP BY duid,factory,model,ieid,oiid
         |)a
         |GROUP BY duid,factory,model,pkg_it,ieid,oiid
         |""".stripMargin)

    //2.生成当日duid、oiid、ieid黑名单数据
    //2.1更新duid-oiid-ieid关系数据
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.duid_oiid_ieid_mapping_full PARTITION(day='$day')
         |SELECT duid
         |     , ieid
         |     , oiid
         |FROM
         |(
         |  SELECT duid
         |       , ieid
         |       , oiid
         |  FROM dm_mid_master.tmp_mid_source_$day
         |  WHERE (ieid <> '' OR oiid <> '')
         |  GROUP BY duid,ieid,oiid
         |
         |  UNION ALL
         |
         |  SELECT duid
         |       , ieid
         |       , oiid
         |  FROM dm_mid_master.duid_oiid_ieid_mapping_full
         |  WHERE day = '$pday'
         |)a
         |GROUP BY duid,ieid,oiid
         |""".stripMargin)

  }

}
