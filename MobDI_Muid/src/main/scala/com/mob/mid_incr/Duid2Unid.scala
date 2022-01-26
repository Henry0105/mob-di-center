package com.mob.mid_incr

import org.apache.spark.sql.SparkSession

object Duid2Unid {

  def main(args: Array[String]): Unit = {

    val day: String = args(0)

    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"Duid2Unid_$day")
      .enableHiveSupport()
      .getOrCreate()

    compute(spark, day)

    spark.stop()

  }

  def compute(spark: SparkSession, day: String): Unit = {


    //1.源头表载入
    spark.sql(
      s"""
         |SELECT duid
         |     , factory
         |     , model
         |     , IF(version IS NOT NULL
         |          AND TRIM(version) NOT IN ('','null','NULL','unknown','none','other','未知','na')
         |          AND firstinstalltime IS NOT NULL
         |          AND LENGTH(firstinstalltime) = 13
         |          AND SUBSTR(firstinstalltime,-3,3) <> '000'
         |          AND pkg = REGEXP_EXTRACT(pkg, '^[a-zA-Z]+[0-9a-zA-Z_]*(\\.[a-zA-Z]+[0-9a-zA-Z_]*)*', 0),
         |          CONCAT(pkg,'_',version,'_',firstinstalltime),
         |          '') AS pkg_it
         |     , IF(TRIM(LOWER(ieid)) IN ('','null','unknown','none','other','未知','na'),'',ieid) AS ieid
         |     , IF(TRIM(LOWER(oiid)) IN ('','null','unknown','none','other','未知','na'),'',oiid) AS oiid
         |     , '' AS asid
         |     , 'install' AS source
         |FROM dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di
         |WHERE day = '$day'
         |AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |
         |UNION ALL
         |
         |SELECT duid
         |     , factory
         |     , model
         |     , '' AS pkg_it
         |     , IF(TRIM(LOWER(ieid)) IN ('','null','unknown','none','other','未知','na'),'',ieid) AS ieid
         |     , IF(TRIM(LOWER(oiid)) IN ('','null','unknown','none','other','未知','na'),'',oiid) AS oiid
         |     , '' AS asid
         |     , 'pv' AS source
         |FROM dm_mobdi_master.dwd_pv_sec_di
         |WHERE day = '$day'
         |AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |
         |UNION ALL
         |
         |SELECT duid
         |     , factory
         |     , model
         |     , '' AS pkg_it
         |     , IF(TRIM(LOWER(ieid)) IN ('','null','unknown','none','other','未知','na'),'',ieid) AS ieid
         |     , IF(TRIM(LOWER(oiid)) IN ('','null','unknown','none','other','未知','na'),'',oiid) AS oiid
         |     , '' AS asid
         |     , 'mdata' AS source
         |FROM dm_mobdi_master.dwd_mdata_nginx_pv_di
         |WHERE day = '$day'
         |AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |
         |UNION ALL
         |
         |SELECT duid
         |     , factory
         |     , model
         |     , pkg_it
         |     , ieid
         |     , oiid
         |     , asid
         |     , source
         |FROM
         |(
         |  SELECT COALESCE(IF(LOWER(TRIM(curduid)) < 0, '', LOWER(TRIM(curduid))), LOWER(TRIM(id))) AS duid
         |       , factory
         |       , model
         |       , '' AS pkg_it
         |       , IF(TRIM(LOWER(ieid)) IN ('','null','unknown','none','other','未知','na'),'',ieid) AS ieid
         |       , IF(TRIM(LOWER(oiid)) IN ('','null','unknown','none','other','未知','na'),'',oiid) AS oiid
         |       , IF(TRIM(LOWER(asid)) IN ('','null','unknown','none','other','未知','na'),'',asid) AS asid
         |       , 'jh' AS source
         |  FROM dm_mobdi_master.dwd_log_device_info_jh_sec_di
         |  WHERE day = '$day'
         |)jh
         |WHERE duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |
         |
         |UNION ALL
         |
         |SELECT duid
         |     , factory
         |     , model
         |     , '' AS pkg_it
         |     , IF(TRIM(LOWER(ieid)) IN ('','null','unknown','none','other','未知','na'),'',ieid) AS ieid
         |     , IF(TRIM(LOWER(oiid)) IN ('','null','unknown','none','other','未知','na'),'',oiid) AS oiid
         |     , '' AS asid
         |     , 'location' AS source
         |FROM dm_mobdi_master.dwd_location_info_sec_di
         |WHERE day = '$day'
         |AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |
         |UNION ALL
         |
         |SELECT duid
         |     , factory
         |     , model
         |     , '' AS pkg_it
         |     , IF(TRIM(LOWER(ieid)) IN ('','null','unknown','none','other','未知','na'),'',ieid) AS ieid
         |     , IF(TRIM(LOWER(oiid)) IN ('','null','unknown','none','other','未知','na'),'',oiid) AS oiid
         |     , '' AS asid
         |     , 'auto_location' AS source
         |FROM dm_mobdi_master.dwd_auto_location_info_sec_di
         |WHERE day = '$day'
         |AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |
         |UNION ALL
         |
         |SELECT duid
         |     , factory
         |     , model
         |     , '' AS pkg_it
         |     , IF(TRIM(LOWER(ieid)) IN ('','null','unknown','none','other','未知','na'),'',ieid) AS ieid
         |     , IF(TRIM(LOWER(oiid)) IN ('','null','unknown','none','other','未知','na'),'',oiid) AS oiid
         |     , '' AS asid
         |     , 'wifi' AS source
         |FROM dm_mobdi_master.dwd_log_wifi_info_sec_di
         |WHERE day = '$day'
         |AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |
         |UNION ALL
         |
         |SELECT duid
         |     , factory
         |     , model
         |     , '' AS pkg_it
         |     , IF(TRIM(LOWER(ieid)) IN ('','null','unknown','none','other','未知','na'),'',ieid) AS ieid
         |     , IF(TRIM(LOWER(oiid)) IN ('','null','unknown','none','other','未知','na'),'',oiid) AS oiid
         |     , '' AS asid
         |     , 'base_station' AS source
         |FROM dm_mobdi_master.dwd_base_station_info_sec_di
         |WHERE day = '$day'
         |AND duid = REGEXP_EXTRACT(duid, '(s_)?[0-9a-f]{40}|[0-9a-zA-Z\\-]{36}|[0-9]{14,17}', 0)
         |""".stripMargin).createOrReplaceTempView("sourceTable")

    //2.生成当日duid黑名单数据
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.duid_oiid_ieid_mapping_full PARTITION(day='$day')
         |SELECT duid
         |     , ieid
         |     , oiid
         |FROM sourceTable
         |WHERE (ieid <> '' OR oiid <> '')
         |GROUP BY duid,ieid,oiid
         |""".stripMargin)

    spark.sql(
      s"""
         |SELECT /*+ BROADCAST(b) */
         |       a.duid
         |     , a.ieid
         |     , a.oiid
         |FROM dm_mid_master.duid_oiid_ieid_mapping_full a
         |LEFT ANTI JOIN
         |(
         |  SELECT duid
         |  FROM dm_mid_master.duid_blacklist
         |  WHERE day < '$day'
         |)b
         |ON a.duid = b.duid
         |""".stripMargin).createOrReplaceTempView("remove_black")

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.duid_blacklist PARTITION(day='$day')
         |SELECT duid
         |FROM
         |(
         |  SELECT duid
         |       , COLLECT_SET(ieid) AS ieids
         |       , COLLECT_SET(oiid) AS oiids
         |  FROM remove_black
         |  GROUP BY duid
         |)a
         |WHERE (SIZE(ieids) > 3 OR SIZE(oiids) > 3)
         |""".stripMargin)


    //3.去掉黑名单duid并且清洗厂商机型
    //3.1去除duid 1对多厂商机型
    spark.sql(
      """
        |SELECT duid
        |FROM
        |(
        |  SELECT duid,CONCAT(UPPER(TRIM(factory)),'_',UPPER(TRIM(model))) AS factory_model
        |  FROM sourceTable
        |  GROUP BY duid,CONCAT(UPPER(TRIM(factory)),'_',UPPER(TRIM(model)))
        |)a
        |GROUP BY duid
        |HAVING COUNT(1) > 1
        |""".stripMargin).createOrReplaceTempView("factory_model_nimiety")

    spark.sql(
      """
        |SELECT /*+ BROADCAST(d) */
        |       c.duid
        |     , CASE
        |         WHEN LOWER(TRIM(c.factory)) IN ('null','none','na','other','未知','','unknown') OR c.factory IS NULL
        |         THEN 'unknown'
        |       ELSE coalesce(UPPER(TRIM(d.clean_brand_origin)), 'other')
        |       END AS factory
        |     , c.pkg_it
        |     , c.ieid
        |     , c.oiid
        |     , c.asid
        |     , c.source
        |FROM
        |(
        |  SELECT a.*
        |  FROM
        |  (
        |    SELECT *
        |    FROM sourceTable
        |    WHERE (pkg_it <> '' OR ieid <> '' OR oiid <> '' OR asid <> '')
        |  )a
        |  LEFT ANTI JOIN dm_mid_master.duid_blacklist b ON a.duid = b.duid
        |  LEFT ANTI JOIN factory_model_nimiety x ON a.duid = x.duid
        |)c
        |LEFT JOIN dm_sdk_mapping.brand_model_mapping_par d
        |ON d.version = '1000'
        |AND UPPER(TRIM(d.brand)) = UPPER(TRIM(c.factory))
        |AND UPPER(TRIM(d.model)) = UPPER(TRIM(c.model))
        |""".stripMargin).createOrReplaceTempView("duid_clear")

    //2.当日duid匹配已有unid_final
    spark.sql(
      s"""
        |SELECT a.duid
        |     , a.factory
        |     , a.pkg_it
        |     , a.ieid
        |     , a.oiid
        |     , a.asid
        |     , b.unid_final
        |     , COALESCE(b.duid_final,a.duid) AS duid_final
        |     , IF(b.unid_final IS NULL,'1','0') AS flag
        |     , source
        |FROM duid_clear a
        |LEFT JOIN
        |(
        |  SELECT duid
        |       , unid_final
        |       , duid_final
        |  FROM dm_mid_master.duid_unidfinal_duidfinal_mapping
        |  WHERE day < '$day'
        |) b
        |ON a.duid = b.duid
        |""".stripMargin).createOrReplaceTempView("duid_unidfinal_tmp")

    //中间落表
    spark.sql(s"DROP TABLE IF EXISTS dm_mid_master.duid_unidfinal_tmp_$day")
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dm_mid_master.duid_unidfinal_tmp_$day STORED AS ORC AS
         |SELECT *
         |FROM duid_unidfinal_tmp
         |""".stripMargin)

    //3.纯新增的duid生成雪花id
    spark.sql("create temporary function fsid as 'com.mob.udf.HistorySnowflakeUDF'")
    spark.sql(
      s"""
         |SELECT duid
         |     , fsid('$day') AS unid
         |FROM
         |(
         |  SELECT duid
         |  FROM dm_mid_master.duid_unidfinal_tmp_$day
         |  WHERE flag = '1'
         |  GROUP BY duid
         |)a
         |""".stripMargin).createOrReplaceTempView("duid_fsid_incr")

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dm_mid_master.duid_incr_tmp PARTITION(day = '$day')
         |SELECT a.duid
         |     , a.factory
         |     , a.pkg_it
         |     , a.ieid
         |     , a.oiid
         |     , a.asid
         |     , COALESCE(a.unid_final,b.unid) AS unid
         |     , a.flag
         |     , a.source
         |     , a.duid_final
         |FROM dm_mid_master.duid_unidfinal_tmp_$day a
         |LEFT JOIN duid_fsid_incr b
         |ON a.duid = b.duid
         |GROUP BY a.duid,a.factory,a.pkg_it,a.ieid,a.oiid,a.asid,COALESCE(a.unid_final,b.unid),a.flag,a.source,a.duid_final
         |""".stripMargin)

  }

}
