package com.mob.mid_incr_v2

import org.apache.spark.sql.{DataFrame, SparkSession}

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

    //1.去掉黑名单duid并且清洗厂商机型
    spark.sql(
      s"""
         |SELECT /*+ BROADCAST(d) */
         |       c.duid
         |     , CASE
         |         WHEN LOWER(TRIM(c.factory)) IN ('null','none','na','other','未知','','unknown') OR c.factory IS NULL
         |         THEN 'unknown'
         |       ELSE COALESCE(UPPER(TRIM(d.clean_brand_origin)), 'other')
         |       END AS factory
         |     , c.pkg_it
         |     , c.ieid
         |     , c.oiid
         |FROM
         |(
         |  SELECT a.duid
         |       , a.factory
         |       , a.model
         |       , a.pkg_it
         |       , IF(x.ieid IS NOT NULL,'',a.ieid) AS ieid
         |       , IF(y.oiid IS NOT NULL,'',a.oiid) AS oiid
         |  FROM
         |  (
         |    SELECT *
         |         , IF(ieid = '',CAST(RAND() AS STRING),ieid) AS ieid_v2
         |         , IF(oiid = '',CAST(RAND() AS STRING),oiid) AS oiid_v2
         |    FROM dm_mid_master.tmp_mid_source_$day
         |    WHERE (pkg_it <> '' OR ieid <> '' OR oiid <> '')
         |  )a
         |  LEFT ANTI JOIN dm_mid_master.duid_blacklist_full b ON a.duid = b.duid
         |  LEFT JOIN dm_mid_master.ieid_blacklist_full x ON a.ieid_v2 = x.ieid
         |  LEFT JOIN dm_mid_master.oiid_blacklist_full y ON a.oiid_v2 = y.oiid
         |  WHERE (x.ieid IS NULL OR y.oiid IS NULL)
         |)c
         |LEFT JOIN dm_sdk_mapping.brand_model_mapping_par d
         |ON d.version = '1000'
         |AND UPPER(TRIM(d.brand)) = UPPER(TRIM(c.factory))
         |AND UPPER(TRIM(d.model)) = UPPER(TRIM(c.model))
         |""".stripMargin).createOrReplaceTempView("duid_clear")


    //2.当日duid匹配已有unid_final
    spark.sql(s"DROP TABLE IF EXISTS dm_mid_master.duid_unidfinal_tmp_$day")
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dm_mid_master.duid_unidfinal_tmp_$day STORED AS ORC AS
         |SELECT a.duid
         |     , a.factory
         |     , a.pkg_it
         |     , a.ieid
         |     , a.oiid
         |     , b.unid_final
         |     , COALESCE(b.duid_final,a.duid) AS duid_final
         |     , IF(b.unid_final IS NULL,'1','0') AS flag
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
         |     , COALESCE(a.unid_final,b.unid) AS unid
         |     , a.flag
         |     , a.duid_final
         |FROM dm_mid_master.duid_unidfinal_tmp_$day a
         |LEFT JOIN duid_fsid_incr b
         |ON a.duid = b.duid
         |GROUP BY a.duid,a.factory,a.pkg_it,a.ieid,a.oiid,COALESCE(a.unid_final,b.unid),a.flag,a.duid_final
         |""".stripMargin)

  }

}
